package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.util.PriorityQueue;

/**
 * Ready queue with priority + aging.
 *
 * <p>Effective weight = static priority weight + age bonus, where the age bonus is
 * {@code min(agingBonusCap, waitedMillis / agingStepMillis)}. Because waiting strictly increases
 * effective weight, a flood of high-priority submissions can never starve a low-priority fiber
 * forever: once it has waited long enough its effective weight reaches the top band. Ties are
 * broken by the global spawn sequence, so under a single worker with a fixed clock the exact
 * dispatch order is fully reproducible.
 *
 * <p>Single-thread-confined to the scheduler dispatch path (external code only hands fibers in
 * via the scheduler lock, which re-inserts them here).
 */
final class ReadyQueue {

    private static final class Node {
        final FiberImpl<?> fiber;
        final long enqueueTime;

        Node(FiberImpl<?> fiber, long enqueueTime) {
            this.fiber = fiber;
            this.enqueueTime = enqueueTime;
        }
    }

    private final Clock clock;
    private final long agingStepMillis;
    private final int agingBonusCap;
    private final PriorityQueue<Node> heap;

    ReadyQueue(Clock clock, long agingStepMillis, int agingBonusCap) {
        this.clock = clock;
        this.agingStepMillis = agingStepMillis;
        this.agingBonusCap = agingBonusCap;
        this.heap = new PriorityQueue<>(this::compare);
    }

    private int compare(Node a, Node b) {
        int wa = effectiveWeight(a);
        int wb = effectiveWeight(b);
        if (wa != wb) {
            return Integer.compare(wb, wa); // higher weight first
        }
        return Long.compare(a.fiber.seq, b.fiber.seq); // stable, reproducible tie-break
    }

    private int effectiveWeight(Node n) {
        long waited = Math.max(0L, clock.nowMillis() - n.enqueueTime);
        long bonus = agingStepMillis > 0 ? waited / agingStepMillis : 0L;
        if (bonus > agingBonusCap) {
            bonus = agingBonusCap;
        }
        return n.fiber.priority.weight() + (int) bonus;
    }

    void add(FiberImpl<?> fiber) {
        if (fiber.enqueueTime == 0L) {
            fiber.enqueueTime = clock.nowMillis();
        }
        heap.add(new Node(fiber, fiber.enqueueTime));
    }

    FiberImpl<?> poll() {
        Node n = heap.poll();
        return n == null ? null : n.fiber;
    }

    boolean remove(FiberImpl<?> fiber) {
        return heap.removeIf(node -> node.fiber == fiber);
    }

    int size() {
        return heap.size();
    }

    boolean isEmpty() {
        return heap.isEmpty();
    }
}
