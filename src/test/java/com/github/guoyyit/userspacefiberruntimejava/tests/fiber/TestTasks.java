package com.github.guoyyit.userspacefiberruntimejava.tests.fiber;

import com.github.guoyyit.userspacefiberruntimejava.fiber.FiberContext;
import com.github.guoyyit.userspacefiberruntimejava.fiber.FiberTask;

import java.time.Duration;

/**
 * Correctly-shaped CPS state machines used by the tests.
 *
 * <p>The portable runtime re-enters the body from the top after every suspension; each suspend
 * call is therefore preceded by advancing {@link FiberContext#stage(long)} so that the resumed
 * invocation jumps past the suspend instead of performing it again.
 */
final class TestTasks {

    private TestTasks() {
    }

    /** Sleep for {@code ms}, then return {@code result}. */
    static FiberTask<String> sleepThen(String result, long ms) {
        return ctx -> {
            while (true) {
                switch ((int) ctx.stage()) {
                    case 0 -> {
                        ctx.stage(1);
                        ctx.sleep(Duration.ofMillis(ms));
                    }
                    case 1 -> {
                        return result;
                    }
                    default -> throw new IllegalStateException();
                }
            }
        };
    }

    /** Park forever; only an external resume/cancel ends it. Returns {@code result} on resume. */
    static FiberTask<String> parkForever(String result) {
        return ctx -> {
            while (true) {
                switch ((int) ctx.stage()) {
                    case 0 -> {
                        ctx.stage(1);
                        ctx.park();
                    }
                    case 1 -> {
                        return result;
                    }
                    default -> throw new IllegalStateException();
                }
            }
        };
    }

    /** Yield exactly once (cooperative switch), then return {@code result}. */
    static FiberTask<Integer> yieldOnceThen(int result) {
        return ctx -> {
            while (true) {
                switch ((int) ctx.stage()) {
                    case 0 -> {
                        ctx.stage(1);
                        ctx.yield();
                    }
                    case 1 -> {
                        return result;
                    }
                    default -> throw new IllegalStateException();
                }
            }
        };
    }
}
