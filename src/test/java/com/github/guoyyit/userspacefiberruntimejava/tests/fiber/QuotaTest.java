package com.github.guoyyit.userspacefiberruntimejava.tests.fiber;

import com.github.guoyyit.userspacefiberruntimejava.fiber.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QuotaTest {

    private static ManualClock clock() {
        return new ManualClock(0);
    }

    private static void awaitTerminated(Fiber<?> f) throws InterruptedException {
        long d = System.currentTimeMillis() + 3000;
        while (!f.isDone() && System.currentTimeMillis() < d) {
            Thread.sleep(2);
        }
        assertTrue(f.isDone(), () -> "not terminal: " + f.state());
    }

    private static void awaitState(Fiber<?> f, FiberState w) throws InterruptedException {
        long d = System.currentTimeMillis() + 3000;
        while (f.state() != w && System.currentTimeMillis() < d) {
            Thread.sleep(2);
        }
        assertEquals(w, f.state());
    }

    @Test
    void liveFiberLimitIsEnforcedAndRejectionLeaksNothing() throws Exception {
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(clock()).workerThreads(1).maxLiveFibers(2).build();
        try (FiberScheduler s = FiberScheduler.create(cfg);
             FiberScope scope = FiberScope.open(s)) {
            Fiber<?> a = scope.spawn(TestTasks.parkForever("a"));
            Fiber<?> b = scope.spawn(TestTasks.parkForever("b"));
            awaitState(a, FiberState.SUSPENDED);
            awaitState(b, FiberState.SUSPENDED);
            assertEquals(2, s.stats().liveFibers);

            // Third live fiber must be rejected at creation.
            assertThrows(FiberLimitException.class,
                    () -> scope.spawn(TestTasks.parkForever("c")));

            // Rejection leaked neither a live slot nor a scope child.
            assertEquals(2, s.stats().liveFibers);
            assertEquals(2, scope.children().size());

            // After one ends, a slot frees and creation succeeds again.
            a.resume();
            awaitTerminated(a);
            Fiber<?> c = scope.spawn(TestTasks.parkForever("c"));
            awaitState(c, FiberState.SUSPENDED);
            assertEquals(2, s.stats().liveFibers);

            // Release the remaining parked children before the scope closes (no orphans allowed).
            b.cancel();
            c.resume();
            awaitTerminated(b);
            awaitTerminated(c);
        }
    }

    @Test
    void perStackDepthQuotaIsEnforced() throws Exception {
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(clock()).workerThreads(1).build();
        try (FiberScheduler s = FiberScheduler.create(cfg);
             FiberScope scope = FiberScope.open(s, ChildFailurePolicy.CONTINUE)) {
            FiberOptions opts = FiberOptions.builder().maxStackDepth(3).build();
            Fiber<?> f = scope.spawn(opts, ctx -> {
                ctx.enterFrame(); // 1
                ctx.enterFrame(); // 2
                ctx.enterFrame(); // 3 -> still allowed
                ctx.enterFrame(); // 4 -> reject
                return "unreachable";
            });
            awaitTerminated(f);
            assertEquals(FiberState.FAILED, f.state());
            assertTrue(f.failure() instanceof StackQuotaException,
                    "got " + f.failure());
        }
    }

    @Test
    void parkedFiberLimitIsEnforcedAndDoesNotLeak() throws Exception {
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(clock()).workerThreads(1).maxParkedFibers(1).build();
        try (FiberScheduler s = FiberScheduler.create(cfg)) {
            Fiber<?> a = s.spawn(TestTasks.parkForever("a"));
            awaitState(a, FiberState.SUSPENDED);
            assertEquals(1, s.stats().parkedFibers);

            // The next fiber parks while the global parked quota is full: its suspension point
            // must reject it and the fiber becomes FAILED (not stuck, not half-bookkept).
            Fiber<?> b = s.spawn(ctx -> {
                while (true) {
                    switch ((int) ctx.stage()) {
                        case 0 -> {
                            ctx.stage(1);
                            // yield once first so a has definitely parked
                            ctx.yield();
                        }
                        case 1 -> {
                            ctx.stage(2);
                            ctx.park();
                        }
                        case 2 -> {
                            return "b";
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });
            awaitTerminated(b);
            assertEquals(FiberState.FAILED, b.state());
            assertTrue(b.failure() instanceof ParkLimitException, "got " + b.failure());

            // Parked accounting is unchanged and the first fiber is unaffected.
            assertEquals(1, s.stats().parkedFibers);
            assertEquals(FiberState.SUSPENDED, a.state());
            a.resume();
            assertEquals("a", a.await());
            // After a wakes, a park slot frees and the quota can be used again.
            assertEquals(0, s.stats().parkedFibers);
        }
    }
}
