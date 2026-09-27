package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * Internal control-flow signal: thrown by a suspension point to unwind the JVM stack of the
 * currently running fiber, releasing the worker thread. It is an {@link Error} (never a user
 * exception) so it cannot be accidentally swallowed by {@code catch (Exception)} in fiber code,
 * and it is always consumed by the scheduler. No stack trace is captured (pure control flow).
 */
final class SuspendSignal extends Error {
    static final SuspendSignal INSTANCE = new SuspendSignal();

    private SuspendSignal() {
        super("fiber-suspend");
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
