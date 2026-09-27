package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * How a parent (or scope) reacts when one of its children fails with an exception.
 * In every policy the parent waits for all children to terminate before it completes.
 */
public enum ChildFailurePolicy {
    /** Cancel all surviving siblings as soon as one child fails (surfaces errors fast). */
    CANCEL_SIBLINGS,
    /** Never cancel siblings because of a failure; wait for every child to finish. */
    CONTINUE
}
