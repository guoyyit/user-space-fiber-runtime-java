package com.github.guoyyit.userspacefiberruntimejava.runtime;

/**
 * Runtime resource quotas. Any exceeded limit fails fast with
 * {@link QuotaExceededException} and releases everything already accounted.
 *
 * @param maxLiveFibers      max non-terminal fibers (creation count limit)
 * @param maxSuspendedFibers max fibers parked at the same time
 * @param maxStackDepth      max Java stack depth of a fiber body at a
 *                           suspension point (single-stack occupancy limit)
 */
public record FiberQuotas(int maxLiveFibers, int maxSuspendedFibers, int maxStackDepth) {

    public static final FiberQuotas UNLIMITED = new FiberQuotas(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

    public FiberQuotas {
        if (maxLiveFibers <= 0 || maxSuspendedFibers <= 0 || maxStackDepth <= 0) {
            throw new IllegalArgumentException("quotas must be positive");
        }
    }
}
