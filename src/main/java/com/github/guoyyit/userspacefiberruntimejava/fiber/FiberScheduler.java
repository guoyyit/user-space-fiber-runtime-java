package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The user-space cooperative runtime. Owns worker threads, a priority/aging ready queue, a
 * deadline wheel and all runtime quotas. Fibers are multiplexed onto the workers and never block
 * a worker thread while suspended: a suspension point throws {@link SuspendSignal}, unwinding the
 * worker back to this loop while the fiber's heap frame waits in {@link #parkedFibers}.
 */
public final class FiberScheduler implements AutoCloseable {

    final FiberSchedulerConfig config;
    final ReadyQueue readyQueue;
    final TimerWheel timerWheel;
    final Clock clock;

    final AtomicLong idGen = new AtomicLong();
    final AtomicLong seqGen = new AtomicLong();

    final Object lock = new Object();
    private final List<FiberImpl<?>> parkedFibers = new ArrayList<>();
    private final List<Thread> workers = new ArrayList<>();
    int liveCount;
    int parkedCount;
    long completedCount;
    long failedCount;
    long cancelledCount;
    private volatile boolean running;
    private volatile boolean shutdown;

    public FiberScheduler() {
        this(FiberSchedulerConfig.builder().build());
    }

    public FiberScheduler(FiberSchedulerConfig config) {
        this.config = config;
        this.clock = config.clock();
        this.readyQueue = new ReadyQueue(clock, config.agingStepMillis(), config.agingBonusCap());
        this.timerWheel = new TimerWheel();
    }

    public static FiberScheduler create() {
        FiberScheduler s = new FiberScheduler();
        s.start();
        return s;
    }

    public static FiberScheduler create(FiberSchedulerConfig config) {
        FiberScheduler s = new FiberScheduler(config);
        if (config.autoStart()) {
            s.start();
        }
        return s;
    }

    // ============================================================ lifecycle

    public void start() {
        synchronized (lock) {
            if (running) {
                return;
            }
            running = true;
            shutdown = false;
            clock.setChangeListener(this::onClockChanged);
            workers.clear();
            for (int i = 0; i < config.workerThreads(); i++) {
                final int idx = i;
                Thread w = new Thread(this::runWorkerLoop, "fiber-worker-" + idx);
                w.setDaemon(true);
                workers.add(w);
                w.start();
            }
        }
    }

    public FiberScope openScope(ChildFailurePolicy policy) {
        return new FiberScope(this, policy != null ? policy : config.defaultChildFailurePolicy());
    }

    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Prevent new fibers, cancel every live fiber (descendants cascade) and wait for the workers
     * to drain. Safe to call repeatedly.
     */
    public void shutdown() {
        List<FiberImpl<?>> toCancel;
        synchronized (lock) {
            if (shutdown) {
                return;
            }
            shutdown = true;
            running = false;
            toCancel = new ArrayList<>(parkedFibers);
            for (FiberImpl<?> ready : drainReady()) {
                toCancel.add(ready);
            }
            lock.notifyAll();
        }
        for (FiberImpl<?> f : toCancel) {
            f.requestCancel(CancelReason.SHUTDOWN, true);
        }
        for (Thread w : workers) {
            try {
                w.join(2000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    boolean isRunning() {
        return running;
    }

    // ============================================================== spawning

    public <T> Fiber<T> spawn(FiberTask<T> task) {
        return spawn(FiberOptions.builder().build(), task);
    }

    public <T> Fiber<T> spawn(FiberOptions options, FiberTask<T> task) {
        FiberImpl<T> f = createFiber(null, options, task);
        schedule(f);
        return f;
    }

    <T> FiberImpl<T> createFiber(ChildOwner owner, FiberOptions options, FiberTask<T> task) {
        if (options == null) {
            options = FiberOptions.builder().build();
        }
        synchronized (lock) {
            if (shutdown) {
                throw new FiberException("scheduler is shut down");
            }
            if (liveCount >= config.maxLiveFibers()) {
                throw new FiberLimitException("live fiber limit reached: "
                        + config.maxLiveFibers());
            }
            int quota = options.maxStackDepth() != null
                    ? options.maxStackDepth()
                    : config.defaultMaxStackDepth();
            long id = idGen.incrementAndGet();
            long seq = seqGen.incrementAndGet();
            String nm = options.name() != null ? options.name() : ("fiber-" + id);
            FiberOptions eff = FiberOptions.builder()
                    .name(nm)
                    .priority(options.priority())
                    .timeout(options.timeout())
                    .childFailurePolicy(options.childFailurePolicy())
                    .maxStackDepth(quota)
                    .build();
            FiberImpl<T> f = new FiberImpl<>(this, owner, eff, task, id, seq, quota);
            liveCount++;
            if (owner != null) {
                owner.registerChild(f);
            }
            armFiberTimeoutLocked(f);
            return f;
        }
    }

    // ============================================================ scheduling

    void schedule(FiberImpl<?> f) {
        synchronized (lock) {
            enqueueLocked(f);
        }
        signalWork();
    }

    private void enqueueLocked(FiberImpl<?> f) {
        if (f.state == FiberState.NEW) {
            f.state = FiberState.READY;
        }
        f.enqueueTime = clock.nowMillis();
        readyQueue.add(f);
    }

    void signalWork() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    private void onClockChanged() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    /**
     * Record a suspension requested by the running fiber body. This does NOT move the fiber yet:
     * the worker still has the fiber on its JVM stack. The suspension is committed atomically in
     * {@link #finishSlice} once {@link SuspendSignal} has fully unwound the worker, which prevents
     * another worker from observing/resuming a fiber whose stack is still running.
     */
    void suspend(FiberImpl<?> f, FiberState suspendedState, long deadlineMillis) {
        if (suspendedState != FiberState.READY
                && parkedCount >= config.maxParkedFibers()) {
            throw new ParkLimitException("parked fiber limit reached: "
                    + config.maxParkedFibers());
        }
        f.pendingSuspend = true;
        f.pendingState = suspendedState;
        f.pendingDeadline = deadlineMillis;
        f.waitDeadline = deadlineMillis;
    }

    /** Body finished but children remain: classify as parked without scheduling a timer. */
    void suspendParkedOnly(FiberImpl<?> f) {
        synchronized (lock) {
            if (!f.inParkedList) {
                f.inParkedList = true;
                parkedFibers.add(f);
                parkedCount++;
            }
        }
    }

    /**
     * Called by a worker after {@code runSlice} returns, to atomically commit the fiber's next
     * disposition now that its stack has fully unwound.
     */
    void finishSlice(FiberImpl<?> f) {
        synchronized (lock) {
            if (f.state.isTerminal()) {
                return; // settled via completion during the slice
            }
            if (!f.pendingSuspend) {
                // The slice itself settled the fiber (e.g. body finished early and is now WAITING on
                // children) or it terminated: leave its state untouched. Only re-enqueue when it
                // returned while still RUNNING without recording any disposition.
                if (f.state == FiberState.RUNNING) {
                    f.state = FiberState.READY;
                    f.enqueueTime = clock.nowMillis();
                    readyQueue.add(f);
                    lock.notifyAll();
                }
                return;
            }
            f.pendingSuspend = false;

            // A cancel/timeout that arrived while the stack was unwinding interrupts immediately.
            if (f.cancelReason != null) {
                f.state = FiberState.READY;
                f.resumeState = FiberImpl.ResumeState.CANCELLED;
                f.pendingState = FiberState.READY;
                f.enqueueTime = clock.nowMillis();
                readyQueue.add(f);
                lock.notifyAll();
                return;
            }

            f.state = f.pendingState;
            if (f.pendingState == FiberState.READY) {
                // Voluntary yield: goes straight back to the ready queue, never "parked".
                f.enqueueTime = clock.nowMillis();
                readyQueue.add(f);
            } else if (f.pendingState == FiberState.WAITING && f.joinSatisfied()) {
                // The awaited child(ren) already terminated while this fiber's stack was unwinding
                // (check-then-suspend window). Do not park: re-enter immediately as a normal resume.
                f.state = FiberState.READY;
                f.resumeState = FiberImpl.ResumeState.CONTINUE;
                f.enqueueTime = clock.nowMillis();
                readyQueue.add(f);
            } else {
                if (!f.inParkedList) {
                    f.inParkedList = true;
                    parkedFibers.add(f);
                    parkedCount++;
                }
                if (f.pendingDeadline != Long.MAX_VALUE) {
                    f.timerEntry = timerWheel.schedule(f.pendingDeadline, f::onTimerFire);
                }
            }
            lock.notifyAll();
        }
    }

    /**
     * Wake a suspended fiber: cancel its timer and move it to the ready queue. Idempotent.
     * The fiber's {@code waitKind}/{@code resumeState} are consumed by its next run slice to
     * distinguish a normal resume from a timeout or cancellation.
     */
    boolean wakeUp(FiberImpl<?> f) {
        synchronized (lock) {
            if (f.state.isTerminal()) {
                return false;
            }
            boolean wasParked = f.inParkedList;
            if (wasParked) {
                f.inParkedList = false;
                parkedFibers.remove(f);
                parkedCount--;
            }
            if (f.timerEntry != null) {
                timerWheel.cancel(f.timerEntry);
                f.timerEntry = null;
            }
            // A normal (manual/join) wake is a CONTINUE unless a deadline already flagged TIMED_OUT.
            if (f.resumeState != FiberImpl.ResumeState.TIMED_OUT) {
                f.resumeState = FiberImpl.ResumeState.CONTINUE;
            }
            // A fiber still RUNNING on some worker stack is handled by finishSlice (do not enqueue
            // it here, or two workers could run it).
            if (f.state == FiberState.RUNNING) {
                return false;
            }
            if (f.state.isSuspended() || f.state == FiberState.READY || wasParked) {
                f.state = FiberState.READY;
                f.enqueueTime = clock.nowMillis();
                readyQueue.add(f);
                lock.notifyAll();
                return true;
            }
            return false;
        }
    }

    // ============================================================ completion

    void complete(FiberImpl<?> f) {
        ChildOwner owner;
        boolean completed;
        synchronized (lock) {
            if (f.state.isTerminal()) {
                return;
            }
            if (f.inParkedList) {
                f.inParkedList = false;
                parkedFibers.remove(f);
                parkedCount--;
            }
            f.pendingSuspend = false;
            if (f.timerEntry != null) {
                timerWheel.cancel(f.timerEntry);
                f.timerEntry = null;
            }
            if (f.overallTimerEntry != null) {
                timerWheel.cancel(f.overallTimerEntry);
                f.overallTimerEntry = null;
            }
            f.decideTerminal();
            liveCount--;
            completed = f.state == FiberState.COMPLETED;
            if (completed) {
                completedCount++;
            } else if (f.state == FiberState.CANCELLED) {
                cancelledCount++;
            } else {
                failedCount++;
            }
            owner = f.parent;
            lock.notifyAll();
        }
        Thread waiter = f.externalWaiter.getAndSet(null);
        if (waiter != null) {
            LockSupportUtil.unpark(waiter);
        }
        if (owner != null) {
            owner.childEnded(f);
        }
    }

    // ============================================================ external join

    /**
     * Block the calling non-fiber thread until the fiber terminates or the wall-clock deadline
     * passes. Immediate wake-ups come via {@link LockSupportUtil#unpark} from completion or a
     * clock change; the bounded park is only a safety net for a missed notification.
     */
    void externalAwait(FiberImpl<?> f, long wallDeadlineMillis) {
        if (f.state.isTerminal()) {
            return;
        }
        Thread current = Thread.currentThread();
        if (!f.externalWaiter.compareAndSet(null, current)) {
            throw new FiberException("fiber already awaited by another thread");
        }
        try {
            while (!f.state.isTerminal()) {
                if (shutdown) {
                    break;
                }
                long remaining = wallDeadlineMillis == Long.MAX_VALUE
                        ? Long.MAX_VALUE
                        : wallDeadlineMillis - System.currentTimeMillis();
                if (remaining <= 0L) {
                    break;
                }
                long parkMs = remaining == Long.MAX_VALUE ? 50L : Math.min(remaining, 50L);
                LockSupportUtil.park(parkMs);
                if (Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } finally {
            f.externalWaiter.compareAndSet(current, null);
        }
        if (!f.state.isTerminal()) {
            throw new FiberTimeoutException("await timed out");
        }
    }

    // ============================================================ worker loop

    private void runWorkerLoop() {
        while (true) {
            FiberImpl<?> f;
            synchronized (lock) {
                while (true) {
                    timerWheel.pollExpired(clock.nowMillis());
                    f = readyQueue.poll();
                    if (f != null) {
                        break;
                    }
                    if (shutdown && liveCount == 0) {
                        return;
                    }
                    long next = timerWheel.nextDeadline();
                    long waitMillis;
                    if (next == Long.MAX_VALUE) {
                        waitMillis = (clock instanceof ManualClock) ? 10_000L : 1_000L;
                    } else {
                        waitMillis = Math.max(1L, next - clock.nowMillis());
                        if (!(clock instanceof ManualClock)) {
                            waitMillis = Math.min(waitMillis, 1_000L);
                        }
                    }
                    try {
                        lock.wait(waitMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                f.state = FiberState.RUNNING;
            }

            try {
                f.runSlice();
                if (!f.state.isTerminal()) {
                    // Commit the pending suspension (yield/park/sleep/join) now that the worker's
                    // JVM stack for this fiber has fully unwound.
                    finishSlice(f);
                }
            } catch (Throwable t) {
                // Absolute fault isolation: a runnable bug must never kill a worker.
                if (!f.state.isTerminal()) {
                    f.bodyDone = true;
                    f.bodySucceeded = false;
                    f.bodyFailure = t;
                    forceFail(f, t);
                }
            }
        }
    }

    private void forceFail(FiberImpl<?> f, Throwable t) {
        synchronized (lock) {
            if (f.state.isTerminal()) {
                return;
            }
            if (f.inParkedList) {
                f.inParkedList = false;
                parkedFibers.remove(f);
                parkedCount--;
            }
            f.pendingSuspend = false;
            f.failure = t;
            f.bodyDone = true;
            f.bodySucceeded = false;
            f.decideTerminal();
            liveCount--;
            failedCount++;
            ChildOwner owner = f.parent;
            if (owner != null) {
                owner.childEnded(f);
            }
        }
    }

    /** Arm the fiber's overall deadline. Caller must hold {@link #lock}. */
    private void armFiberTimeoutLocked(FiberImpl<?> f) {
        Duration timeout = f.options.timeout();
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return;
        }
        long deadline = clock.nowMillis() + timeout.toMillis();
        f.overallTimerEntry = timerWheel.schedule(deadline, () -> {
            if (!f.state.isTerminal() && f.cancelReason == null) {
                f.requestCancel(CancelReason.TIMEOUT, true);
            }
        });
    }

    private List<FiberImpl<?>> drainReady() {
        List<FiberImpl<?>> all = new ArrayList<>();
        while (true) {
            FiberImpl<?> f = readyQueue.poll();
            if (f == null) {
                break;
            }
            all.add(f);
        }
        return all;
    }

    public SchedulerStats stats() {
        synchronized (lock) {
            return new SchedulerStats(liveCount, readyQueue.size(), parkedCount,
                    completedCount, failedCount, cancelledCount);
        }
    }
}
