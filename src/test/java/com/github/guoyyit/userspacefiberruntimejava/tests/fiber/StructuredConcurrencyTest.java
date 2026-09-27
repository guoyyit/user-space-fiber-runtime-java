package com.github.guoyyit.userspacefiberruntimejava.tests.fiber;

import com.github.guoyyit.userspacefiberruntimejava.fiber.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StructuredConcurrencyTest {

    private static FiberSchedulerConfig cfg(ManualClock clock) {
        return FiberSchedulerConfig.builder().clock(clock).workerThreads(1).build();
    }

    private static void awaitState(Fiber<?> f, FiberState wanted) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (f.state() != wanted && System.currentTimeMillis() < deadline) {
            Thread.sleep(2);
        }
        assertEquals(wanted, f.state(), () -> "wanted " + wanted + " but was " + f.state());
    }

    private static void awaitTerminated(Fiber<?> f) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (!f.isDone() && System.currentTimeMillis() < deadline) {
            Thread.sleep(2);
        }
        assertTrue(f.isDone(), () -> "fiber not terminal, state=" + f.state());
    }

    @Test
    void parentCannotCompleteUntilAllChildrenEnd() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock));
             FiberScope scope = FiberScope.open(s)) {

            Fiber<String> parent = scope.spawn(pctx -> {
                while (true) {
                    switch ((int) pctx.stage()) {
                        case 0 -> {
                            pctx.spawn(TestTasks.sleepThen("a", 50));
                            pctx.spawn(TestTasks.sleepThen("b", 20));
                            // Return without joining: runtime must keep the parent alive.
                            return "parent-result";
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });

            awaitState(parent, FiberState.WAITING);
            List<Fiber<?>> kids = parent.children();
            assertEquals(2, kids.size());

            clock.advance(30); // child b fires at t=20
            awaitState(kids.get(1), FiberState.COMPLETED);
            assertFalse(parent.isDone());
            assertEquals(FiberState.WAITING, parent.state());

            clock.advance(30); // t=60: child a fires
            awaitTerminated(parent);
            assertEquals(FiberState.COMPLETED, parent.state());
            assertEquals("parent-result", parent.result());
            assertTrue(kids.stream().allMatch(Fiber::isDone));
            assertEquals(0, s.stats().liveFibers);
        }
    }

    @Test
    void cancellingParentCascadesToAllDescendants() throws Exception {
        ManualClock clock = new ManualClock(0);
        try (FiberScheduler s = FiberScheduler.create(cfg(clock));
             FiberScope scope = FiberScope.open(s)) {

            Fiber<?>[] grandHolder = new Fiber<?>[1];

            FiberTask<String> childTask = cctx -> {
                while (true) {
                    switch ((int) cctx.stage()) {
                        case 0 -> {
                            Fiber<?> grand = cctx.spawn(TestTasks.parkForever("g"));
                            grandHolder[0] = grand;
                            cctx.stage(1);
                            cctx.join(grand);
                        }
                        case 1 -> {
                            return "c";
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            };

            Fiber<String> parent = scope.spawn(pctx -> {
                while (true) {
                    switch ((int) pctx.stage()) {
                        case 0 -> {
                            Fiber<?> child = pctx.spawn(childTask);
                            pctx.stage(1);
                            pctx.join(child);
                        }
                        case 1 -> {
                            return "p";
                        }
                        default -> throw new IllegalStateException();
                    }
                }
            });

            Fiber<?> grand = awaitGrand(grandHolder);
            awaitState(grand, FiberState.SUSPENDED);
            awaitState(parent, FiberState.WAITING);

            assertTrue(parent.cancel());
            assertFalse(parent.cancel(), "second cancel must be a no-op");

            awaitTerminated(parent);
            Fiber<?> child = parent.children().get(0);
            awaitTerminated(child);
            awaitTerminated(grand);

            assertEquals(FiberState.CANCELLED, parent.state());
            assertEquals(FiberState.CANCELLED, child.state());
            assertEquals(FiberState.CANCELLED, grand.state());

            assertEquals(CancelReason.MANUAL, parent.cancelReason());
            assertEquals(CancelReason.PARENT, child.cancelReason());
            assertEquals(CancelReason.PARENT, grand.cancelReason());

            scope.close(); // cancellation is not a failure: must return normally
            assertEquals(0, s.stats().liveFibers);
        }
    }

    private static Fiber<?> awaitGrand(Fiber<?>[] holder) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (holder[0] == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(2);
        }
        assertNotNull(holder[0]);
        return holder[0];
    }
}
