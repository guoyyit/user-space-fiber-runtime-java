package com.github.guoyyit.userspacefiberruntimejava.tests.fiber;

import com.github.guoyyit.userspacefiberruntimejava.fiber.*;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FaultIsolationTest {

    private static FiberSchedulerConfig cfg(ManualClock clock) {
        return FiberSchedulerConfig.builder().clock(clock).workerThreads(1).build();
    }

    private static void awaitTerminated(Fiber<?> f) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (!f.isDone() && System.currentTimeMillis() < deadline) {
            Thread.sleep(2);
        }
        assertTrue(f.isDone(), () -> "not terminal: " + f.state());
    }

    private static void awaitState(Fiber<?> f, FiberState w) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (f.state() != w && System.currentTimeMillis() < deadline) {
            Thread.sleep(2);
        }
        assertEquals(w, f.state());
    }

    @Test
    void normalExceptionAndCancelAreDistinct() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock));
             FiberScope scope = FiberScope.open(s, ChildFailurePolicy.CONTINUE)) {

            Fiber<Integer> ok = scope.spawn(ctx -> 7);
            Fiber<Integer> boom = scope.spawn(ctx -> {
                throw new IllegalStateException("boom");
            });
            Fiber<String> cancelled = scope.spawn(TestTasks.parkForever("c"));

            awaitTerminated(ok);
            awaitTerminated(boom);
            awaitState(cancelled, FiberState.SUSPENDED);
            cancelled.cancel();
            awaitTerminated(cancelled);

            assertEquals(FiberState.COMPLETED, ok.state());
            assertEquals(7, ok.result());
            assertNull(ok.failure());

            assertEquals(FiberState.FAILED, boom.state());
            assertTrue(boom.failure() instanceof IllegalStateException);
            assertEquals("boom", boom.failure().getMessage());

            assertEquals(FiberState.CANCELLED, cancelled.state());
            assertTrue(cancelled.failure() instanceof FiberCancellationException);
            assertEquals(CancelReason.MANUAL, cancelled.cancelReason());

            assertEquals(1, s.stats().completed);
            assertEquals(1, s.stats().failed);
            assertEquals(1, s.stats().cancelled);
        }
    }

    @Test
    void failingFiberDoesNotBreakSchedulerOrSiblings() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock))) {
            AtomicInteger ranAfter = new AtomicInteger();
            Fiber<?> crash = s.spawn(ctx -> {
                throw new RuntimeException("nasty");
            });
            Fiber<?> after = s.spawn(ctx -> {
                ranAfter.incrementAndGet();
                return "still-scheduled";
            });
            awaitTerminated(crash);
            awaitTerminated(after);
            assertEquals(FiberState.FAILED, crash.state());
            assertEquals(FiberState.COMPLETED, after.state());
            assertEquals(1, ranAfter.get());

            // The scheduler remains fully usable after a fiber blew up.
            Fiber<?> later = s.spawn(ctx -> "alive");
            assertEquals("alive", later.await());
        }
    }

    @Test
    void childFailureIsQueryableAndDoesNotEscape() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock))) {
            // The PARENT's policy decides sibling/parent cancellation; use CONTINUE on the parent.
            FiberOptions parentOpts = FiberOptions.builder()
                    .childFailurePolicy(ChildFailurePolicy.CONTINUE)
                    .build();
            Fiber<?> parent = s.spawn(parentOpts, pctx -> {
                while (true) {
                    switch ((int) pctx.stage()) {
                        case 0 -> {
                            pctx.spawn(c -> {
                                throw new IllegalArgumentException("child-boom");
                            });
                            pctx.spawn(c -> "fine");
                            pctx.stage(1);
                            pctx.joinAll();
                        }
                        case 1 -> {
                            Fiber<?> bad = pctx.fiber().children().get(0);
                            Fiber<?> good = pctx.fiber().children().get(1);
                            return "parent-finished:" + String.valueOf(good.result())
                                    + ":" + bad.state();
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });

            Object result = parent.await();
            assertEquals(FiberState.COMPLETED, parent.state());
            assertEquals("parent-finished:fine:FAILED", result);
            Fiber<?> bad = parent.children().get(0);
            assertEquals(FiberState.FAILED, bad.state());
            assertTrue(bad.failure() instanceof IllegalArgumentException);
            assertEquals("child-boom", bad.failure().getMessage());
        }
    }

    @Test
    void throwingCancelCallbackDoesNotPropagate() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock));
             FiberScope scope = FiberScope.open(s)) {
            AtomicInteger second = new AtomicInteger();
            Fiber<?> f = scope.spawn(TestTasks.parkForever("x"));
            f.onCancel(() -> {
                throw new RuntimeException("callback-explosion");
            });
            f.onCancel(second::incrementAndGet);

            awaitState(f, FiberState.SUSPENDED);
            assertDoesNotThrow(() -> f.cancel());
            awaitTerminated(f);
            assertEquals(FiberState.CANCELLED, f.state());
            assertEquals(1, second.get(), "other callbacks still run");
        }
    }

    @Test
    void awaitingFailedFiberRethrowsButDoesNotKillCaller() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock))) {
            Fiber<?> f = s.spawn(ctx -> {
                throw new IllegalStateException("await-me");
            });
            IllegalStateException caught = assertThrows(IllegalStateException.class, f::await);
            assertEquals("await-me", caught.getMessage());
            // service process / calling thread is still alive and the runtime usable
            assertEquals("ok", s.spawn(ctx -> "ok").await());
        }
    }
}
