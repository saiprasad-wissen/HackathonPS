package com.hacksys.backend.config;

import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Map;
import java.util.concurrent.Executor;

@Configuration
public class AsyncConfig {

    /**
     * TaskDecorator that copies the caller thread's MDC context map into the
     * worker thread before task execution and clears it afterwards, preventing
     * MDC context leakage between pooled threads.
     *
     * Fix for INC-20260509133449-B866EB (ASYNC_CONFIRM_TIMEOUT):
     * Without this decorator async threads start with an empty MDC, causing
     * trace_id / service fields to be lost and confirmation notifications to
     * time out because the logging / tracing pipeline cannot correlate them.
     */
    static class MdcTaskDecorator implements TaskDecorator {
        @Override
        public Runnable decorate(Runnable runnable) {
            // Capture the MDC context of the SUBMITTING (caller) thread
            Map<String, String> callerMdcContext = MDC.getCopyOfContextMap();
            return () -> {
                try {
                    // Restore caller's MDC context into the WORKER thread
                    if (callerMdcContext != null) {
                        MDC.setContextMap(callerMdcContext);
                    } else {
                        MDC.clear();
                    }
                    runnable.run();
                } finally {
                    // Always clean up to avoid context leakage to the next task
                    MDC.clear();
                }
            };
        }
    }

    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(3);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("hacksys-async-");
        // Fix INC-20260509133449-B866EB: propagate MDC context to child threads
        executor.setTaskDecorator(new MdcTaskDecorator());
        executor.initialize();
        return executor;
    }
}
