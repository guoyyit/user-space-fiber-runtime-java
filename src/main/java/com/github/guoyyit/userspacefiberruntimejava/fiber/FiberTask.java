package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * The body of a fiber.
 *
 * <p>The portable runtime saves continuation state on the heap rather than moving the JVM call
 * stack, so implementations must be written as resumable state machines: keep values that must
 * survive a suspension in {@link FiberLocals} (or in fields of the task object) and use
 * {@link FiberContext#stage(long)} as the resume label. A suspend call never blocks a worker
 * thread; on resume the body is re-entered from the top and jumps to the current stage.
 *
 * @param <T> the result type
 */
@FunctionalInterface
public interface FiberTask<T> {
    T run(FiberContext ctx) throws Exception;
}
