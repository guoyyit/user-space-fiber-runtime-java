package com.github.guoyyit.userspacefiberruntimejava.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Requirement 6: resource quotas with fail-fast rejection and no leaks. */
class QuotaTest {

    @Test
    void liveFiberLimitRejectsAndDoesNotLeak() throws Exception {
        FiberQuotas quotas = new FiberQuotas(3, Integer.MAX_VALUE, Integer.MAX_VALUE);
        try (FiberRuntime rt = FiberRuntime.builder().quotas(quotas).virtualClock(new VirtualClock()).start()) {
            List<FiberHandle<?>> parked = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                parked.add(rt.spawn(ctx -> {
                    ctx.park();
                    return null;
                }));
            }
            // 4th creation must be rejected explicitly.
            QuotaExceededException qee = assertThrows(QuotaExceededException.class,
                    () -> rt.spawn(ctx -> null));
            assertEquals(QuotaExceededException.Kind.LIVE_FIBERS, qee.kind());
            assertEquals(3, rt.activeFibers().size());

            // No leak: after one finishes, a new one may be created.
            parked.get(0).cancel();
            waitFor(() -> rt.activeFibers().size() == 2);
            assertEquals("ok", rt.spawn(ctx -> "ok").result());
        }
    }

    @Test
    void suspendedLimitRejectsInsideFiberAndReleasesNothing() throws Exception {
        FiberQuotas quotas = new FiberQuotas(Integer.MAX_VALUE, 2, Integer.MAX_VALUE);
        try (FiberRuntime rt = FiberRuntime.builder().quotas(quotas).virtualClock(new VirtualClock()).start()) {
            List<FiberHandle<?>> handles = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                handles.add(rt.spawn(ctx -> {
                    ctx.park();
                    return null;
                }));
            }
            // Third fiber tries to park: the suspension itself is rejected.
            FiberHandle<Void> third = rt.spawn(ctx -> {
                ctx.park();
                return null;
            });
            Outcome<Void> out = third.await();
            assertInstanceOf(Outcome.Failed.class, out);
            assertInstanceOf(QuotaExceededException.class, ((Outcome.Failed<Void>) out).error());
            assertEquals(QuotaExceededException.Kind.SUSPENDED_FIBERS,
                    ((QuotaExceededException) ((Outcome.Failed<Void>) out).error()).kind());

            // The two parked fibers are untouched; cancel one and the quota frees up.
            handles.get(0).cancel();
            waitFor(() -> rt.activeFibers().size() == 1);
            assertEquals("ok", rt.spawn(ctx -> {
                ctx.sleep(Duration.ofMillis(1));
                return "ok";
            }).result());
        }
    }

    @Test
    void stackDepthLimitRejectsDeepBodies() throws Exception {
        FiberQuotas quotas = new FiberQuotas(Integer.MAX_VALUE, Integer.MAX_VALUE, 20);
        try (FiberRuntime rt = FiberRuntime.builder().quotas(quotas).virtualClock(new VirtualClock()).start()) {
            FiberHandle<Void> deep = rt.spawn(ctx -> recurse(ctx, 1000));
            Outcome<Void> out = deep.await();
            assertInstanceOf(Outcome.Failed.class, out);
            assertInstanceOf(QuotaExceededException.class, ((Outcome.Failed<Void>) out).error());
            assertEquals(QuotaExceededException.Kind.STACK_DEPTH,
                    ((QuotaExceededException) ((Outcome.Failed<Void>) out).error()).kind());
        }
    }

    private static Void recurse(FiberContext ctx, int n) {
        if (n == 0) {
            ctx.park();
            return null;
        }
        return recurse(ctx, n - 1);
    }

    @Test
    void childSpawnAlsoRespectsLiveLimit() throws Exception {
        FiberQuotas quotas = new FiberQuotas(2, Integer.MAX_VALUE, Integer.MAX_VALUE);
        try (FiberRuntime rt = FiberRuntime.builder().quotas(quotas).virtualClock(new VirtualClock()).start()) {
            FiberHandle<String> parent = rt.spawn(ctx -> {
                FiberHandle<Void> c1 = ctx.spawn(FiberOptions.named("c1"), c -> {
                    c.park();
                    return null;
                });
                String verdict;
                try {
                    ctx.spawn(FiberOptions.named("c2"), c -> null);
                    verdict = "not-rejected";
                } catch (QuotaExceededException e) {
                    verdict = "rejected:" + e.kind();
                }
                c1.cancel(); // let the parent terminate
                return verdict;
            });
            assertEquals("rejected:LIVE_FIBERS", parent.result());
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
