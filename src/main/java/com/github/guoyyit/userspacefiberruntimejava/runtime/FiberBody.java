package com.github.guoyyit.userspacefiberruntimejava.runtime;

/**
 * The executable unit of a fiber.
 *
 * <p>Contract: the body must be deterministic. All side effects must go
 * through {@link FiberContext#call} so they are journaled and not re-executed
 * on replay. Suspension points are the {@code ctx.*} operations.
 */
@FunctionalInterface
public interface FiberBody<T> {
    T run(FiberContext ctx) throws Exception;
}
