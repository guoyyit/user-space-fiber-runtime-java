package com.github.guoyyit.userspacefiberruntimejava.fiber;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** White-box test of priority aging (anti-starvation) and reproducible tie-breaking. */
class ReadyQueueAgingTest {

    private static final FiberTask<Void> NOOP = ctx -> null;

    private FiberImpl<?> newFiber(FiberScheduler s, Priority p) {
        FiberOptions o = FiberOptions.builder().priority(p).build();
        return s.createFiber(null, o, NOOP);
    }

    @Test
    void anOldLowPriorityFiberIsNotStarvedByNewHighSubmissions() {
        ManualClock clock = new ManualClock(0);
        FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
                .clock(clock).workerThreads(1)
                .agingStepMillis(10).agingBonusCap(Priority.HIGH.weight() - Priority.LOW.weight())
                .maxLiveFibers(100)
                .build();
        FiberScheduler s = new FiberScheduler(cfg);
        ReadyQueue q = new ReadyQueue(clock, cfg.agingStepMillis(), cfg.agingBonusCap());

        FiberImpl<?> low = newFiber(s, Priority.LOW);
        q.add(low);                         // enqueued at t=0
        clock.advance(1000);               // waited long enough to reach the top weight band

        List<FiberImpl<?>> highs = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            FiberImpl<?> h = newFiber(s, Priority.HIGH);
            highs.add(h);
            q.add(h);                      // fresh high-priority submissions at t=1000
        }

        // Effective weight of the old LOW fiber is now equal to HIGH; lower seq breaks the tie,
        // so it is dispatched before ANY of the fresh high-priority fibers => no starvation.
        FiberImpl<?> first = q.poll();
        assertSame(low, first, "aged low-priority fiber must overtake fresh high-priority ones");

        // And the result is reproducible in an identically-sequenced fresh run.
        ManualClock clock2 = new ManualClock(0);
        FiberScheduler s2 = new FiberScheduler(FiberSchedulerConfig.builder()
                .clock(clock2).workerThreads(1)
                .agingStepMillis(10).agingBonusCap(Priority.HIGH.weight() - Priority.LOW.weight())
                .maxLiveFibers(100).build());
        ReadyQueue q2 = new ReadyQueue(clock2, 10, Priority.HIGH.weight() - Priority.LOW.weight());
        FiberImpl<?> low2 = newFiber(s2, Priority.LOW);
        q2.add(low2);
        clock2.advance(1000);
        for (int i = 0; i < 8; i++) {
            q2.add(newFiber(s2, Priority.HIGH));
        }
        assertSame(low2, q2.poll());
    }

    @Test
    void withoutAgingHighAlwaysPrecedesLow() {
        ManualClock clock = new ManualClock(0);
        FiberScheduler s = new FiberScheduler(FiberSchedulerConfig.builder()
                .clock(clock).workerThreads(1).maxLiveFibers(100).build());
        ReadyQueue q = new ReadyQueue(clock, 10, 10);

        FiberImpl<?> low = newFiber(s, Priority.LOW);
        FiberImpl<?> high = newFiber(s, Priority.HIGH);
        q.add(low);
        q.add(high); // same instant: no age bonus

        assertSame(high, q.poll());
        assertSame(low, q.poll());
    }
}
