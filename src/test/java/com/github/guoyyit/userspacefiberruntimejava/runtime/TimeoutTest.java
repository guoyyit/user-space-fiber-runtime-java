package com.github.guoyyit.userspacefiberruntimejava.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 3: timeouts and deadlines. */
class TimeoutTest {

    @Test
    void timeoutInterruptsSuspendedFiber() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            FiberHandle<Void> h = rt.spawn(FiberOptions.DEFAULT.withTimeout(Duration.ofSeconds(5)), ctx -> {
                ctx.sleep(Duration.ofHours(1)); // parked when the deadline hits
                return null;
            });
            Outcome<Void> out = h.await();
            assertInstanceOf(Outcome.Cancelled.class, out);
            assertInstanceOf(TimeoutCancellationException.class, ((Outcome.Cancelled<Void>) out).reason());
            assertEquals(FiberState.CANCELLED, h.state());
        }
    }

    @Test
    void timeoutInterruptsIndefiniteParkAndCallbackRunsExactlyOnce() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            AtomicInteger callbacks = new AtomicInteger();
            FiberHandle<Void> h = rt.spawn(FiberOptions.DEFAULT.withTimeout(Duration.ofMillis(100)), ctx -> {
                ctx.onCancel(callbacks::incrementAndGet);
                ctx.park(); // no timer of its own: only the deadline can wake it
                return null;
            });
            Outcome<Void> out = h.await();
            assertInstanceOf(Outcome.Cancelled.class, out);
            assertEquals(1, callbacks.get(), "cancel callback must run exactly once");
        }
    }

    @Test
    void fiberCompletingBeforeDeadlineWinsTheRace() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            FiberHandle<String> h = rt.spawn(FiberOptions.DEFAULT.withTimeout(Duration.ofSeconds(10)), ctx -> {
                ctx.sleep(Duration.ofSeconds(3));
                return "done";
            });
            Outcome<String> out = h.await();
            assertEquals("done", out.getOrThrow());
            assertEquals(FiberState.COMPLETED, h.state());
        }
    }

    @Test
    void manualCancelBeatsLaterDeadlineAndRunsCallbackOnce() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            AtomicInteger callbacks = new AtomicInteger();
            FiberHandle<Void> h = rt.spawn(FiberOptions.DEFAULT.withTimeout(Duration.ofMillis(50)), ctx -> {
                ctx.onCancel(callbacks::incrementAndGet);
                ctx.park();
                return null;
            });
            waitFor(() -> rt.activeFibers().size() == 1);
            h.cancel(); // before the 50ms deadline
            Outcome<Void> out = h.await();
            assertInstanceOf(Outcome.Cancelled.class, out);
            assertInstanceOf(FiberCancellationException.class, ((Outcome.Cancelled<Void>) out).reason());
            assertEquals(1, callbacks.get());
        }
    }

    private static void waitFor(Check check) throws InterruptedException {
        for (int i = 0; i < 500; i++) {
            if (check.ok()) {
                return;
            }
            Thread.sleep(5);
        }
        fail("condition not met in time");
    }

    private interface Check {
        boolean ok();
    }
}
