package com.github.guoyyit.userspacefiberruntimejava.runtime;

/**
 * Internal control-flow signal: unwinds the fiber body stack when the body
 * hits a blocking operation. The fiber then holds no execution thread; its
 * progress is restored later by journal replay. Never escapes the dispatcher.
 */
final class SuspendControl extends RuntimeException {

    static final SuspendControl INSTANCE = new SuspendControl();

    private SuspendControl() {
        super(null, null, false, false);
    }
}
