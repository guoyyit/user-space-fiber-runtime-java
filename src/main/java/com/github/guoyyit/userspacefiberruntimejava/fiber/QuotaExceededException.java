package com.github.guoyyit.userspacefiberruntimejava.fiber;

/** Base class for resource-quota rejections. */
public class QuotaExceededException extends FiberException {
    public QuotaExceededException(String message) {
        super(message);
    }
}
