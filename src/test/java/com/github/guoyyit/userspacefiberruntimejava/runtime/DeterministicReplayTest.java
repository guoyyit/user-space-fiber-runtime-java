package com.github.guoyyit.userspacefiberruntimejava.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Requirements 1 & 4: local state survives replay; scheduling is reproducible. */
class DeterministicReplayTest {

    @Test
    void sameInputProducesIdenticalScheduleOrder() throws Exception {
        List<String> run1 = runWorkload();
        List<String> run2 = runWorkload();
        assertEquals(run1, run2, "identical input must produce identical schedule order");
        assertFalse(run1.isEmpty());
    }

    @Test
    void localStateIsCompleteAcrossManySuspensions() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            FiberHandle<Integer> h = rt.spawn(ctx -> {
                int acc = 0;
                for (int i = 1; i <= 50; i++) {
                    ctx.sleep(Duration.ofMillis(i));
                    ctx.yieldNow();
                    acc += i;
                }
                return acc;
            });
            assertEquals(50 * 51 / 2, h.result());
        }
    }

    @Test
    void journaledEffectsAreNotReexecutedOnReplay() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            List<String> effects = new ArrayList<>();
            FiberHandle<String> h = rt.spawn(ctx -> {
                String a = ctx.call(() -> {
                    effects.add("A");
                    return "a";
                });
                ctx.sleep(Duration.ofMillis(10));
                String b = ctx.call(() -> {
                    effects.add("B");
                    return "b";
                });
                ctx.sleep(Duration.ofMillis(10));
                ctx.yieldNow();
                return a + b;
            });
            assertEquals("ab", h.result());
            assertEquals(List.of("A", "B"), effects, "each effect must execute exactly once");
        }
    }

    /** A mixed workload: priorities, sleeps, yields, child spawn + await. */
    private static List<String> runWorkload() throws InterruptedException {
        List<String> events = new ArrayList<>();
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            FiberHandle<Void> root = rt.spawn(ctx -> {
                FiberHandle<Void> a = ctx.spawn(FiberOptions.DEFAULT.withPriority(FiberPriority.HIGH), c -> {
                    for (int i = 0; i < 3; i++) {
                        int k = i;
                        c.call(() -> log(events, "A" + k));
                        c.yieldNow();
                    }
                    return null;
                });
                FiberHandle<Void> b = ctx.spawn(FiberOptions.DEFAULT.withPriority(FiberPriority.LOW), c -> {
                    for (int i = 0; i < 3; i++) {
                        int k = i;
                        c.call(() -> log(events, "B" + k));
                        c.sleep(Duration.ofMillis(5));
                    }
                    return null;
                });
                FiberHandle<Void> d = ctx.spawn(FiberOptions.DEFAULT, c -> {
                    c.call(() -> log(events, "D0"));
                    c.sleep(Duration.ofMillis(12));
                    c.call(() -> log(events, "D1"));
                    return null;
                });
                ctx.await(a);
                ctx.await(b);
                ctx.await(d);
                return null;
            });
            root.await();
        }
        return events;
    }

    private static Object log(List<String> events, String tag) {
        synchronized (events) {
            events.add(tag);
        }
        return null;
    }
}
