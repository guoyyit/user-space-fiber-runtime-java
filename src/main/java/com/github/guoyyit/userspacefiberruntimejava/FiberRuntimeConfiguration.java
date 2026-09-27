package com.github.guoyyit.userspacefiberruntimejava;

import com.github.guoyyit.userspacefiberruntimejava.runtime.FiberQuotas;
import com.github.guoyyit.userspacefiberruntimejava.runtime.FiberRuntime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Exposes the in-process fiber runtime as a Spring bean. */
@Configuration
public class FiberRuntimeConfiguration {

    @Bean(destroyMethod = "close")
    public FiberRuntime fiberRuntime(
            @Value("${fiber.quotas.max-live:10000}") int maxLive,
            @Value("${fiber.quotas.max-suspended:10000}") int maxSuspended,
            @Value("${fiber.quotas.max-stack-depth:1024}") int maxStackDepth) {
        return FiberRuntime.builder()
                .quotas(new FiberQuotas(maxLive, maxSuspended, maxStackDepth))
                .start();
    }
}
