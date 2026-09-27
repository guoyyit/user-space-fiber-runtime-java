package com.github.guoyyit.userspacefiberruntimejava.tests.fiber;

import com.github.guoyyit.userspacefiberruntimejava.fiber.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class LifecycleTest {

    private static void awaitState(Fiber<?> f, FiberState w) throws InterruptedException {
        long d = System.currentTimeMillis() + 3000;
        while (f.state() != w && System.currentTimeMillis() < d) {
            Thread.sleep(2);
        }
        assertEquals(w, f.state());
    }

    private static void awaitTerminated(Fiber<?> f) throws InterruptedException {
        long d = System.currentTimeMillis() + 3000;
        while (!f.isDone() && System.currentTimeMillis() < d) {
            Thread.sleep(2);
        }
        assertTrue(f.isDone(), () -> "not terminal: " + f.state());
    }

    @Test
    void shutdownCancelsSuspendedFibersAndDrainsCleanly() throws Exception {
        ManualClock clock = new ManualClock(0);
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(clock).workerThreads(1).build();
        FiberScheduler s = FiberScheduler.create(cfg);
        Fiber<?> parked = s.spawn(TestTasks.parkForever("x"));
        awaitState(parked, FiberState.SUSPENDED);

        s.shutdown(); // must cancel the parked fiber and let workers exit
        awaitTerminated(parked);
        assertEquals(FiberState.CANCELLED, parked.state());
        assertEquals(CancelReason.SHUTDOWN, parked.cancelReason());
        assertEquals(0, s.stats().liveFibers);
        assertTrue(s.isShutdown());

        // New work is rejected after shutdown.
        assertThrows(FiberException.class, () -> s.spawn(ctx -> 1));

        // shutdown is idempotent and safe via close().
        assertDoesNotThrow(s::close);
    }

    @Test
    void awaitWithTimeoutThrowsButFiberStaysAlive() throws Exception {
        ManualClock clock = new ManualClock(0);
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(clock).workerThreads(1).build();
        try (FiberScheduler s = FiberScheduler.create(cfg)) {
            Fiber<?> parked = s.spawn(TestTasks.parkForever("x"));
            awaitState(parked, FiberState.SUSPENDED);

            assertThrows(FiberTimeoutException.class,
                    () -> parked.await(Duration.ofMillis(50)));
            // The timeout on the *caller's* await does not cancel the parked fiber.
            assertEquals(FiberState.SUSPENDED, parked.state());

            parked.resume();
            assertEquals("x", parked.await());
        }
    }

    @Test
    void exceptionInFiberNeverReachesCallerThread() throws Exception {
        // Spawning a soon-to-fail fiber must not throw on the caller; failure is queryable.
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(
                FiberSchedulerConfig.builder().clock(clock).workerThreads(1).build())) {
            Fiber<?> f = assertDoesNotThrow(() -> s.spawn(ctx -> {
                throw new RuntimeException("contained");
            }));
            awaitTerminated(f);
            assertEquals(FiberState.FAILED, f.state());
            assertEquals("contained", f.failure().getMessage());
        }
    }
}
