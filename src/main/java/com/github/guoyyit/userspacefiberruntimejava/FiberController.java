package com.github.guoyyit.userspacefiberruntimejava;

import com.github.guoyyit.userspacefiberruntimejava.runtime.FiberHandle;
import com.github.guoyyit.userspacefiberruntimejava.runtime.FiberOptions;
import com.github.guoyyit.userspacefiberruntimejava.runtime.FiberRuntime;
import com.github.guoyyit.userspacefiberruntimejava.runtime.Outcome;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Demo / monitoring endpoints for the user-space fiber runtime running
 * inside this service process.
 */
@RestController
@RequestMapping("/api/fibers")
public class FiberController {

    private final FiberRuntime runtime;
    private final Map<Long, FiberHandle<?>> spawned = new ConcurrentHashMap<>();

    public FiberController(FiberRuntime runtime) {
        this.runtime = runtime;
    }

    public record FiberView(long id, String name, String state, String outcome) {
    }

    /** Spawn a demo fiber that suspends itself in steps without holding a thread. */
    @PostMapping
    public FiberView spawn(@RequestParam(defaultValue = "demo") String name,
                           @RequestParam(defaultValue = "5") int steps,
                           @RequestParam(defaultValue = "200") long sleepMs) {
        FiberHandle<String> handle = runtime.spawn(FiberOptions.named(name), ctx -> {
            for (int i = 0; i < steps; i++) {
                ctx.sleep(Duration.ofMillis(sleepMs));
            }
            return "finished " + steps + " steps";
        });
        spawned.put(handle.id(), handle);
        return view(handle);
    }

    @GetMapping
    public List<FiberView> list() {
        return runtime.activeFibers().stream().map(FiberController::view).toList();
    }

    @GetMapping("/{id}")
    public ResponseEntity<FiberView> get(@PathVariable long id) {
        FiberHandle<?> h = spawned.get(id);
        return h == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(view(h));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<FiberView> cancel(@PathVariable long id) {
        FiberHandle<?> h = spawned.get(id);
        if (h == null) {
            return ResponseEntity.notFound().build();
        }
        h.cancel();
        return ResponseEntity.ok(view(h));
    }

    private static FiberView view(FiberHandle<?> h) {
        Outcome<?> outcome = h.outcome();
        return new FiberView(h.id(), h.name(), h.state().name(),
                outcome == null ? null : outcome.toString());
    }
}
