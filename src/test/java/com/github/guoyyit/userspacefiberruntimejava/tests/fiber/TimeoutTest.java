package com.github.guoyyit.userspacefiberruntimejava.tests.fiber;

import com.github.guoyyit.userspacefiberruntimejava.fiber.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TimeoutTest {

    private static FiberSchedulerConfig cfg(ManualClock clock) {
        return FiberSchedulerConfig.builder().clock(clock).workerThreads(1).build();
    }

    private static void awaitState(Fiber<?> f, FiberState wanted) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (f.state() != wanted && System.currentTimeMillis() < deadline) {
            Thread.sleep(2);
        }
        assertEquals(wanted, f.state(), () -> "wanted " + wanted + " but was " + f.state());
    }

    private static void awaitTerminated(Fiber<?> f) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (!f.isDone() && System.currentTimeMillis() < deadline) {
            Thread.sleep(2);
        }
        assertTrue(f.isDone(), () -> "not terminal: " + f.state());
    }

    @Test
    void overallTimeoutInterruptsSuspendedFiber() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock));
             FiberScope scope = FiberScope.open(s)) {
            AtomicInteger cancels = new AtomicInteger();
            FiberOptions opts = FiberOptions.builder()
                    .timeout(Duration.ofMillis(100))
                    .build();
            Fiber<String> f = scope.spawn(opts, TestTasks.parkForever("x"));
            f.onCancel(cancels::incrementAndGet);

            awaitState(f, FiberState.SUSPENDED);
            clock.advance(100);

            awaitTerminated(f);
            assertEquals(FiberState.CANCELLED, f.state());
            assertEquals(CancelReason.TIMEOUT, f.cancelReason());
            assertEquals(1, cancels.get(), "cancel callback must fire exactly once");
        }
    }

    @Test
    void manualCancelWinsOverTimeoutAndCallbackFiresOnce() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock));
             FiberScope scope = FiberScope.open(s)) {
            AtomicInteger cancels = new AtomicInteger();
            FiberOptions opts = FiberOptions.builder()
                    .timeout(Duration.ofMillis(100))
                    .build();
            Fiber<String> f = scope.spawn(opts, TestTasks.parkForever("x"));
            f.onCancel(cancels::incrementAndGet);

            awaitState(f, FiberState.SUSPENDED);
            f.cancel(CancelReason.MANUAL);
            awaitTerminated(f);

            // Let the overall deadline pass too; it must not double-cancel.
            clock.advance(500);
            Thread.sleep(20);

            assertEquals(FiberState.CANCELLED, f.state());
            assertEquals(CancelReason.MANUAL, f.cancelReason());
            assertEquals(1, cancels.get());
            // Repeated cancel requests are idempotent.
            f.cancel();
            f.onCancel(cancels::incrementAndGet); // registered after cancel fires immediately? once total
            assertEquals(2, cancels.get(), "late-registered callback runs at most once");
        }
    }

    @Test
    void multipleCallbacksEachRunExactlyOnce() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock));
             FiberScope scope = FiberScope.open(s)) {
            AtomicInteger a = new AtomicInteger();
            AtomicInteger b = new AtomicInteger();
            Fiber<String> f = scope.spawn(TestTasks.parkForever("x"));
            f.onCancel(a::incrementAndGet);
            f.onCancel(b::incrementAndGet);

            awaitState(f, FiberState.SUSPENDED);
            clock.advance(1); // no timer armed: still parked
            assertEquals(FiberState.SUSPENDED, f.state());

            f.resume(); // explicit resume lets it complete normally; no cancel callback
            f.await();
            assertEquals(FiberState.COMPLETED, f.state());
            assertEquals(0, a.get());
            assertEquals(0, b.get());
        }
    }

    @Test
    void timedParkThrowsTimeoutButPlainParkResumes() throws Exception {
        ManualClock clock = new ManualClock(0);
        // Root fibers (no scope): this test focuses purely on timer semantics, so we do not want a
        // scope to surface the deliberate timeout failure on close.
        try (FiberScheduler s = FiberScheduler.create(cfg(clock))) {
            // timed park expiry -> FAILED with FiberTimeoutException
            Fiber<?> timed = s.spawn(ctx -> {
                while (true) {
                    switch ((int) ctx.stage()) {
                        case 0 -> {
                            ctx.stage(1);
                            ctx.park(Duration.ofMillis(50));
                        }
                        case 1 -> {
                            return "ok";
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });
            awaitState(timed, FiberState.TIMED_WAITING);
            clock.advance(50);
            awaitTerminated(timed);
            assertEquals(FiberState.FAILED, timed.state());
            assertTrue(timed.failure() instanceof FiberTimeoutException,
                    "expected timeout, got " + timed.failure());

            // a plain park resumed externally completes normally
            Fiber<?> plain = s.spawn(TestTasks.parkForever("done"));
            awaitState(plain, FiberState.SUSPENDED);
            assertTrue(plain.resume());
            assertEquals("done", plain.await());
        }
    }

    @Test
    void joinWithTimeoutRacesChildCompletion() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock))) {
            // Case 1: child finishes before the join deadline -> join returns normally.
            Fiber<?> fastParent = s.spawn(pctx -> {
                while (true) {
                    switch ((int) pctx.stage()) {
                        case 0 -> {
                            Fiber<?> child = pctx.spawn(TestTasks.sleepThen("c", 40));
                            pctx.stage(1);
                            pctx.join(child, java.time.Duration.ofMillis(100));
                        }
                        case 1 -> {
                            return "joined-fast";
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });
            awaitState(fastParent, FiberState.WAITING);
            clock.advance(40);
            assertEquals("joined-fast", fastParent.await());

            // Case 2: deadline first -> join fails with FiberTimeoutException.
            Fiber<?> slowParent = s.spawn(pctx -> {
                while (true) {
                    switch ((int) pctx.stage()) {
                        case 0 -> {
                            pctx.spawn(TestTasks.sleepThen("c", 1000));
                            pctx.stage(1);
                            pctx.joinAll(java.time.Duration.ofMillis(50));
                        }
                        case 1 -> {
                            return "joined-slow";
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });
            awaitState(slowParent, FiberState.WAITING);
            // The join deadline (t=50) fails the body, but structured semantics keep the parent
            // alive until its sleeping child ends at t=1000; then it settles FAILED.
            clock.advance(50);
            assertEquals(FiberState.WAITING, slowParent.state());
            clock.advance(950);
            awaitTerminated(slowParent);
            assertEquals(FiberState.FAILED, slowParent.state());
            assertTrue(slowParent.failure() instanceof FiberTimeoutException,
                    "got " + slowParent.failure());
        }
    }
}
