package com.github.guoyyit.userspacefiberruntimejava.fiber;

/**
 * Why a fiber was cancelled.
 */
public enum CancelReason {
    MANUAL,
    TIMEOUT,
    PARENT,
    SHUTDOWN
}
