package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * Internal common contract of something that owns child fibers: either another fiber
 * (structured concurrency hierarchy) or a root {@link FiberScope} on an external thread.
 */
interface ChildOwner {

    FiberScheduler scheduler();

    void registerChild(FiberImpl<?> child);

    /** Called once, after the child has reached a terminal state. */
    void childEnded(FiberImpl<?> child);

    /** Non-null cancellation reason when this owner has been cancelled. */
    CancelReason cancellation();
}
