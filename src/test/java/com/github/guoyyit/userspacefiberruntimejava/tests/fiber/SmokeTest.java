package com.github.guoyyit.userspacefiberruntimejava.tests.fiber;

import com.github.guoyyit.userspacefiberruntimejava.fiber.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class SmokeTest {

    @Test
    void createSuspendResumeAndKeepLocals() throws Exception {
        ManualClock clock = new ManualClock(0);
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(clock)
                .workerThreads(1)
                .build();
        try (FiberScheduler scheduler = FiberScheduler.create(cfg)) {
            Fiber<Integer> f = scheduler.spawn(ctx -> {
                while (true) {
                    switch ((int) ctx.stage()) {
                        case 0 -> {
                            ctx.locals().put("acc", 10);
                            ctx.stage(1);
                            ctx.park(); // releases worker; resume jumps to case 1
                        }
                        case 1 -> {
                            Integer acc = ctx.locals().get("acc");
                            ctx.locals().put("acc", acc + 32);
                            return ctx.locals().get("acc");
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });

            waitFor(f, FiberState.SUSPENDED);
            assertEquals(FiberState.SUSPENDED, f.state());
            assertEquals(Integer.valueOf(10), f.locals().get("acc"));

            assertTrue(f.resume());
            Integer result = f.await();
            assertEquals(42, result);
            assertEquals(FiberState.COMPLETED, f.state());
        }
    }

    @Test
    void sleepCompletesWhenClockAdvances() throws Exception {
        ManualClock clock = new ManualClock(0);
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(clock).workerThreads(1).build();
        try (FiberScheduler scheduler = FiberScheduler.create(cfg)) {
            Fiber<Boolean> f = scheduler.spawn(ctx -> {
                while (true) {
                    switch ((int) ctx.stage()) {
                        case 0 -> {
                            ctx.stage(1);
                            ctx.sleep(Duration.ofMillis(100));
                        }
                        case 1 -> {
                            return true;
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });
            waitFor(f, FiberState.TIMED_WAITING);
            clock.advance(100);
            assertTrue(f.await());
            assertEquals(FiberState.COMPLETED, f.state());
        }
    }

    static void waitFor(Fiber<?> f, FiberState wanted) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (f.state() != wanted && !f.isDone() && System.currentTimeMillis() < deadline) {
            Thread.sleep(2);
        }
        assertEquals(wanted, f.state(), "fiber did not reach " + wanted + " in time");
    }
}
