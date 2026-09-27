package com.github.guoyyit.userspacefiberruntimejava.fiber;

/** Base class for failures raised by the fiber runtime itself (not by user code). */
public class FiberException extends RuntimeException {
    public FiberException(String message) {
        super(message);
    }

    public FiberException(String message, Throwable cause) {
        super(message, cause);
    }
}
