package com.github.guoyyit.userspacefiberruntimejava.runtime;

import java.util.concurrent.CancellationException;

/** Delivered inside a fiber (at a suspension point) when it is cancelled. */
public class FiberCancellationException extends CancellationException {
    public FiberCancellationException(String message) {
        super(message);
    }
}
