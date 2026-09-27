package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * An independent, cooperatively scheduled unit of execution ("fiber").
 *
 * <p>A fiber is created, scheduled, suspended, resumed and joined inside the host service
 * process. It occupies no worker thread while suspended.
 */
public abstract class Fiber<T> {

    public abstract long id();

    public abstract String name();

    public abstract FiberState state();

    public abstract Priority priority();

    public abstract void priority(Priority priority);

    public abstract boolean isDone();

    public abstract T result();

    public abstract Optional<T> resultOpt();

    /** Failure cause when {@link FiberState#FAILED}; cancellation signal when cancelled. */
    public abstract Throwable failure();

    public abstract CancelReason cancelReason();

    /**
     * Request cancellation. The fiber, if suspended, is interrupted on its next resume; if it is
     * already terminal this is a no-op. Cascades to descendants when invoked on a parent.
     *
     * @return true if this call transitioned the fiber towards cancellation, false if already done
     */
    public abstract boolean cancel(CancelReason reason);

    public boolean cancel() {
        return cancel(CancelReason.MANUAL);
    }

    /** Resume an externally {@link FiberContext#park() parked} fiber. */
    public abstract boolean resume();

    /** Suspend the calling fiber (or external thread) until this one terminates. */
    public abstract T await() throws Exception;

    public abstract T await(Duration timeout) throws Exception;

    public abstract Fiber<?> parent();

    public abstract List<Fiber<?>> children();

    /** Register a callback fired exactly once when/if the fiber is cancelled. */
    public abstract void onCancel(Runnable callback);

    public abstract FiberLocals locals();
}
