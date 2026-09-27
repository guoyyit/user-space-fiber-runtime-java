package com.github.guoyyit.userspacefiberruntimejava.fiber;

import java.util.PriorityQueue;

/**
 * Min-heap of deadline entries keyed by the logical {@link Clock}. Expired entries are drained
 * deterministically (earliest deadline first; ties broken by insertion sequence).
 *
 * <p>This class is single-thread-confined: only the scheduler's dispatch path calls it, so no
 * internal synchronization is needed and firing order is exactly reproducible with a
 * {@link ManualClock}.
 */
final class TimerWheel {

    static final class Entry {
        final long deadline;
        final long seq;
        final Runnable onFire;
        volatile boolean fired;
        volatile boolean cancelled;

        Entry(long deadline, long seq, Runnable onFire) {
            this.deadline = deadline;
            this.seq = seq;
            this.onFire = onFire;
        }
    }

    private final PriorityQueue<Entry> heap = new PriorityQueue<>((a, b) -> {
        int cmp = Long.compare(a.deadline, b.deadline);
        return cmp != 0 ? cmp : Long.compare(a.seq, b.seq);
    });

    private long seq;

    TimerWheel() {
    }

    Entry schedule(long deadline, Runnable onFire) {
        Entry e = new Entry(deadline, ++seq, onFire);
        heap.add(e);
        return e;
    }

    /** Remove a still-pending entry (e.g. a wake-up that happened early). */
    boolean cancel(Entry entry) {
        if (entry.fired || entry.cancelled) {
            return false;
        }
        boolean removed = heap.remove(entry);
        entry.cancelled = true;
        return removed;
    }

    /** Fire every entry with deadline &lt;= now, in deadline/seq order; returns the count. */
    int pollExpired(long now) {
        int fired = 0;
        while (true) {
            Entry e = heap.peek();
            if (e == null || e.deadline > now) {
                break;
            }
            heap.poll();
            if (e.cancelled) {
                continue;
            }
            e.fired = true;
            e.onFire.run();
            fired++;
        }
        return fired;
    }

    /** Smallest pending deadline, or {@link Long#MAX_VALUE} when empty. */
    long nextDeadline() {
        Entry e = heap.peek();
        return e == null ? Long.MAX_VALUE : e.deadline;
    }

    int size() {
        return heap.size();
    }
}
