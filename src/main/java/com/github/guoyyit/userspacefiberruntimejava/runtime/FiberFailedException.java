package com.github.guoyyit.userspacefiberruntimejava.runtime;

/** Wraps an uncaught fiber body failure when observed from outside. */
public class FiberFailedException extends RuntimeException {
    public FiberFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
