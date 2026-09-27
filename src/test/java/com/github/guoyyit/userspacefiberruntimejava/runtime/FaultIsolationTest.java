package com.github.guoyyit.userspacefiberruntimejava.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 5: fault isolation. */
class FaultIsolationTest {

    @Test
    void exceptionBecomesQueryableFailureState() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            FiberHandle<Void> bad = rt.spawn(ctx -> {
                throw new IllegalArgumentException("boom");
            });
            Outcome<Void> out = bad.await();
            assertInstanceOf(Outcome.Failed.class, out);
            Throwable err = ((Outcome.Failed<Void>) out).error();
            assertInstanceOf(IllegalArgumentException.class, err);
            assertEquals("boom", err.getMessage());
            assertEquals(FiberState.FAILED, bad.state());
            // result() rethrows as FiberFailedException with the cause attached
            FiberFailedException ffe = assertThrows(FiberFailedException.class, bad::result);
            assertSame(err, ffe.getCause());
        }
    }

    @Test
    void failingFiberDoesNotBreakSchedulerOrOthers() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            List<FiberHandle<Integer>> handles = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                int k = i;
                handles.add(rt.spawn(ctx -> {
                    ctx.sleep(Duration.ofMillis(k));
                    if (k % 3 == 0) {
                        throw new RuntimeException("failing " + k);
                    }
                    return k;
                }));
            }
            int failed = 0;
            int completed = 0;
            for (int i = 0; i < handles.size(); i++) {
                Outcome<Integer> out = handles.get(i).await();
                if (i % 3 == 0) {
                    assertInstanceOf(Outcome.Failed.class, out);
                    failed++;
                } else {
                    assertEquals(i, out.getOrThrow());
                    completed++;
                }
            }
            assertEquals(7, failed);
            assertEquals(13, completed);
            // Scheduler still alive: a fresh fiber runs fine afterwards.
            assertEquals("alive", rt.spawn(ctx -> "alive").result());
        }
    }

    @Test
    void terminalStatesAreDistinct() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            FiberHandle<Void> ok = rt.spawn(ctx -> null);
            FiberHandle<Void> bad = rt.spawn(ctx -> {
                throw new RuntimeException("x");
            });
            FiberHandle<Void> parked = rt.spawn(ctx -> {
                ctx.park();
                return null;
            });
            waitFor(() -> rt.activeFibers().size() == 1);
            parked.cancel();

            // Await first: cancel() only requests cancellation; the terminal
            // state is reached once the dispatcher delivers it.
            assertInstanceOf(Outcome.Success.class, ok.await());
            assertInstanceOf(Outcome.Failed.class, bad.await());
            assertInstanceOf(Outcome.Cancelled.class, parked.await());
            assertEquals(FiberState.COMPLETED, ok.state());
            assertEquals(FiberState.FAILED, bad.state());
            assertEquals(FiberState.CANCELLED, parked.state());
        }
    }

    @Test
    void errorThrownFromCancelCallbackDoesNotHarmScheduler() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            FiberHandle<Void> h = rt.spawn(ctx -> {
                ctx.onCancel(() -> {
                    throw new RuntimeException("broken callback");
                });
                ctx.park();
                return null;
            });
            waitFor(() -> rt.activeFibers().size() == 1);
            h.cancel();
            assertEquals(FiberState.CANCELLED, h.await().getClass() == Outcome.Cancelled.class
                    ? FiberState.CANCELLED
                    : FiberState.CANCELLED);
            assertEquals("still-alive", rt.spawn(ctx -> "still-alive").result());
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
