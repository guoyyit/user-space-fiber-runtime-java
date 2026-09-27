package com.github.guoyyit.userspacefiberruntimejava.runtime;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 4: priority + fairness (aging), no starvation. */
class FairnessTest {

    @Test
    void lowPriorityFiberIsNotStarvedByHighPriorityFlood() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            List<String> events = new CopyOnWriteArrayList<>();
            int highYieldCount = 200;

            // Orchestrator: submissions happen on the dispatcher thread, so
            // all three fibers compete from the same starting line.
            FiberHandle<Void> root = rt.spawn(ctx -> {
                FiberHandle<Void> low = ctx.spawn(FiberOptions.DEFAULT.withPriority(FiberPriority.LOW), c -> {
                    c.call(() -> {
                        events.add("L");
                        return null;
                    });
                    return null;
                });
                FiberHandle<Void> high1 = ctx.spawn(FiberOptions.DEFAULT.withPriority(FiberPriority.HIGH),
                        c -> yieldLoop(c, highYieldCount, events, "H1"));
                FiberHandle<Void> high2 = ctx.spawn(FiberOptions.DEFAULT.withPriority(FiberPriority.HIGH),
                        c -> yieldLoop(c, highYieldCount, events, "H2"));
                ctx.await(low);
                ctx.await(high1);
                ctx.await(high2);
                return null;
            });
            root.await();

            int lowIndex = events.indexOf("L");
            assertTrue(lowIndex >= 0, "low priority fiber never ran");
            assertTrue(lowIndex < 2 * highYieldCount,
                    "low priority fiber starved until all high priority work finished (index=" + lowIndex + ")");
        }
    }

    @Test
    void highPriorityRunsBeforeLowWhenFresh() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            List<String> events = new CopyOnWriteArrayList<>();
            FiberHandle<Void> root = rt.spawn(ctx -> {
                FiberHandle<Void> low = ctx.spawn(FiberOptions.DEFAULT.withPriority(FiberPriority.LOW), c -> {
                    c.call(() -> {
                        events.add("low");
                        return null;
                    });
                    c.yieldNow();
                    return null;
                });
                FiberHandle<Void> high = ctx.spawn(FiberOptions.DEFAULT.withPriority(FiberPriority.HIGH), c -> {
                    c.call(() -> {
                        events.add("high");
                        return null;
                    });
                    c.yieldNow();
                    return null;
                });
                ctx.await(low);
                ctx.await(high);
                return null;
            });
            root.await();
            assertEquals(List.of("high", "low"), events,
                    "with no aging backlog, higher base priority must win the first turn");
        }
    }

    private static Void yieldLoop(FiberContext ctx, int n, List<String> events, String tag) {
        for (int i = 0; i < n; i++) {
            ctx.call(() -> {
                events.add(tag);
                return null;
            });
            ctx.yieldNow();
        }
        return null;
    }
}
