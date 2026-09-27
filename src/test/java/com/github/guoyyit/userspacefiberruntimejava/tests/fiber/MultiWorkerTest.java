package com.github.guoyyit.userspacefiberruntimejava.tests.fiber;

import com.github.guoyyit.userspacefiberruntimejava.fiber.*;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MultiWorkerTest {

    @Test
    void manyFibersAcrossMultipleWorkersAllTerminateWithoutOrphans() throws Exception {
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(Clock.system())
                .workerThreads(4)
                .maxLiveFibers(5_000)
                .build();
        int total = 2_000;
        AtomicInteger sum = new AtomicInteger();
        try (FiberScheduler s = FiberScheduler.create(cfg)) {
            Fiber<?>[] fibers = new Fiber<?>[total];
            for (int i = 0; i < total; i++) {
                final int v = i;
                fibers[i] = s.spawn(ctx -> {
                    int r = TestTasks.yieldOnceThen(v).run(ctx);
                    sum.incrementAndGet();
                    return r;
                });
            }
            for (Fiber<?> f : fibers) {
                f.await();
            }
            long deadline = System.currentTimeMillis() + 3000;
            while (s.stats().liveFibers != 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(total, sum.get());
            assertEquals(0, s.stats().liveFibers, "no live/orphan fibers remain");
            assertEquals(total, s.stats().completed);
        }
    }

    @Test
    void structuredTreeCompletesUnderMultipleWorkers() throws Exception {
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(Clock.system()).workerThreads(4).build();
        try (FiberScheduler s = FiberScheduler.create(cfg);
             FiberScope scope = FiberScope.open(s)) {
            Fiber<?> root = scope.spawn(pctx -> {
                while (true) {
                    switch ((int) pctx.stage()) {
                        case 0 -> {
                            for (int i = 0; i < 20; i++) {
                                final int idx = i;
                                pctx.spawn(c -> {
                                    while (true) {
                                        switch ((int) c.stage()) {
                                            case 0 -> {
                                                c.spawn(TestTasks.yieldOnceThen(idx * 10 + 1));
                                                c.spawn(TestTasks.yieldOnceThen(idx * 10 + 2));
                                                c.stage(1);
                                                c.joinAll();
                                            }
                                            case 1 -> {
                                                return "ok";
                                            }
                                            default -> throw new IllegalStateException();
                                        }
                                    }
                                });
                            }
                            pctx.stage(1);
                            pctx.joinAll();
                        }
                        case 1 -> {
                            return "root";
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });
            assertEquals("root", root.await());
            assertEquals(0, s.stats().liveFibers);
        }
    }
}
