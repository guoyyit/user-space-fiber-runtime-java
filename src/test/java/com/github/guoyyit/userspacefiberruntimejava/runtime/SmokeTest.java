package com.github.guoyyit.userspacefiberruntimejava.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SmokeTest {

    @Test
    void sleepSuspendsWithoutHoldingThreadAndResumesWithState() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            AtomicInteger effectCalls = new AtomicInteger();
            FiberHandle<String> h = rt.spawn(ctx -> {
                int a = ctx.call(() -> {
                    effectCalls.incrementAndGet();
                    return 40;
                });
                ctx.sleep(Duration.ofSeconds(10));
                int b = ctx.call(() -> {
                    effectCalls.incrementAndGet();
                    return 2;
                });
                return "sum=" + (a + b);
            });
            Outcome<String> out = h.await();
            assertInstanceOf(Outcome.Success.class, out);
            assertEquals("sum=42", out.getOrThrow());
            // Effects ran exactly once despite replay after the sleep.
            assertEquals(2, effectCalls.get());
            assertEquals(FiberState.COMPLETED, h.state());
        }
    }

    @Test
    void parentWaitsForChildAndAwaitsValue() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            FiberHandle<Integer> h = rt.spawn(ctx -> {
                FiberHandle<Integer> child = ctx.spawn(FiberOptions.named("child"), c -> {
                    c.sleep(Duration.ofMillis(50));
                    return 7;
                });
                int v = ctx.await(child);
                return v * 6;
            });
            assertEquals(42, h.result());
            assertTrue(rt.activeFibers().isEmpty());
        }
    }

    @Test
    void yieldInterleavesFibers() throws Exception {
        try (FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start()) {
            List<String> order = new ArrayList<>();
            FiberBody<Void> body = ctx -> {
                for (int i = 0; i < 3; i++) {
                    int k = i;
                    ctx.call(() -> {
                        order.add(Thread.currentThread().getName());
                        return null;
                    });
                    ctx.yieldNow();
                }
                return null;
            };
            var a = rt.spawn(FiberOptions.named("A"), body);
            var b = rt.spawn(FiberOptions.named("B"), body);
            a.await();
            b.await();
            assertEquals(FiberState.COMPLETED, a.state());
            assertEquals(FiberState.COMPLETED, b.state());
        }
    }
}
