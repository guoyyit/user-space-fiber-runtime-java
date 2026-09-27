package com.github.guoyyit.userspacefiberruntimejava.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 2: structured concurrency. */
class StructuredConcurrencyTest {

    @Test
    void parentCompletesOnlyAfterAllChildren() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            List<String> events = new CopyOnWriteArrayList<>();
            FiberHandle<String> parent = rt.spawn(ctx -> {
                ctx.spawn(FiberOptions.named("slow-child"), c -> {
                    c.sleep(Duration.ofSeconds(30));
                    events.add("child-done");
                    return null;
                });
                events.add("parent-body-done");
                return "parent";
            });
            assertEquals("parent", parent.result());
            // Parent terminated only after the child finished.
            assertEquals(List.of("parent-body-done", "child-done"), events);
            assertEquals(FiberState.COMPLETED, parent.state());
        }
    }

    @Test
    void cancellingParentCascadesToAllDescendants() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            AtomicInteger cancelled = new AtomicInteger();
            FiberHandle<Void> parent = rt.spawn(ctx -> {
                ctx.onCancel(cancelled::incrementAndGet);
                ctx.spawn(FiberOptions.named("child"), c -> {
                    c.onCancel(cancelled::incrementAndGet);
                    c.spawn(FiberOptions.named("grandchild"), g -> {
                        g.onCancel(cancelled::incrementAndGet);
                        g.park();
                        return null;
                    });
                    c.park();
                    return null;
                });
                ctx.park();
                return null;
            });
            // Let the whole tree start and park.
            waitFor(() -> rt.activeFibers().size() == 3);

            parent.cancel();
            Outcome<Void> out = parent.await();

            assertInstanceOf(Outcome.Cancelled.class, out);
            assertEquals(FiberState.CANCELLED, parent.state());
            // No orphans: nothing left running in the runtime.
            waitFor(() -> rt.activeFibers().isEmpty());
            assertEquals(3, cancelled.get(), "every unit ran its cancel callback exactly once");
        }
    }

    @Test
    void childFailureDoesNotEscapeAsOrphanAndIsQueryable() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            FiberHandle<String> parent = rt.spawn(ctx -> {
                FiberHandle<Object> child = ctx.spawn(FiberOptions.named("boom"), c -> {
                    throw new IllegalStateException("child blew up");
                });
                try {
                    ctx.await(child);
                    fail("await must rethrow the child failure");
                } catch (FiberFailedException e) {
                    assertInstanceOf(IllegalStateException.class, e.getCause());
                }
                assertEquals(FiberState.FAILED, child.state());
                return "parent-survived";
            });
            assertEquals("parent-survived", parent.result());
            assertTrue(rt.activeFibers().isEmpty());
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
