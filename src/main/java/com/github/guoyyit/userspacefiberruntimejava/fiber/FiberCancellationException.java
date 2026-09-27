package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * Thrown inside a fiber body when it resumes after being cancelled.
 * A body that catches this must rethrow it to honor cancellation; the runtime
 * treats the uncaught instance as the cancellation signal rather than a failure.
 */
public final class FiberCancellationException extends FiberException {
    private final transient Fiber<?> fiber;
    private final CancelReason reason;

    public FiberCancellationException(Fiber<?> fiber, CancelReason reason) {
        super("Fiber cancelled: " + reason);
        this.fiber = fiber;
        this.reason = reason;
    }

    public Fiber<?> fiber() {
        return fiber;
    }

    public CancelReason reason() {
        return reason;
    }
}
