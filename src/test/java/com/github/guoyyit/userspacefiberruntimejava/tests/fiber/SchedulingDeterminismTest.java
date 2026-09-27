package com.github.guoyyit.userspacefiberruntimejava.tests.fiber;

import com.github.guoyyit.userspacefiberruntimejava.fiber.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class SchedulingDeterminismTest {

    /** Build a non-started scheduler so every submitted fiber is READY before draining starts. */
    private static FiberScheduler pausedScheduler(ManualClock clock) {
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(clock)
                .workerThreads(1)
                .agingStepMillis(10)
                .build();
        return new FiberScheduler(cfg);
    }

    private List<String> oneRun() throws Exception {
        ManualClock clock = new ManualClock(0); // frozen => aging bonus is zero for everyone
        FiberScheduler s = pausedScheduler(clock);
        List<String> order = new CopyOnWriteArrayList<>();
        List<Fiber<Integer>> fibers = new ArrayList<>();

        // Interleave priorities and submission order.
        fibers.add(s.spawn(FiberOptions.builder().priority(Priority.LOW).build(),
                ctx -> { order.add("L0"); return 0; }));
        fibers.add(s.spawn(FiberOptions.builder().priority(Priority.HIGH).build(),
                ctx -> { order.add("H0"); return 0; }));
        fibers.add(s.spawn(FiberOptions.builder().priority(Priority.LOW).build(),
                ctx -> { order.add("L1"); return 0; }));
        fibers.add(s.spawn(FiberOptions.builder().priority(Priority.NORMAL).build(),
                ctx -> { order.add("N0"); return 0; }));
        fibers.add(s.spawn(FiberOptions.builder().priority(Priority.HIGH).build(),
                ctx -> { order.add("H1"); return 0; }));
        fibers.add(s.spawn(FiberOptions.builder().priority(Priority.NORMAL).build(),
                ctx -> { order.add("N1"); return 0; }));

        s.start();
        for (Fiber<Integer> f : fibers) {
            f.await();
        }
        s.shutdown();
        return order;
    }

    @Test
    void strictPriorityThenFifoWithinLevelAndReproducible() throws Exception {
        List<String> first = oneRun();
        List<String> second = oneRun();

        List<String> expected = List.of("H0", "H1", "N0", "N1", "L0", "L1");
        assertEquals(expected, first, "high->normal->low, FIFO within a level");
        assertEquals(expected, second, "same inputs must replay to the same order");
        assertEquals(first, second);
    }
}
