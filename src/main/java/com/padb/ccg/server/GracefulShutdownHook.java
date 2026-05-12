package com.padb.ccg.server;

import com.padb.ccg.core.spi.RequestLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 优雅关闭钩子，监听 Spring 容器关闭事件，
 * 等待日志缓冲区中的剩余日志写入数据库后再退出。
 */
@Component
public class GracefulShutdownHook {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownHook.class);

    /** 关闭时等待日志刷新的最大时长 */
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    private final RequestLogger requestLogger;

    public GracefulShutdownHook(RequestLogger requestLogger) {
        this.requestLogger = requestLogger;
    }

    /**
     * 监听容器关闭事件，刷新日志缓冲区
     */
    @EventListener(ContextClosedEvent.class)
    public void onShutdown(ContextClosedEvent event) {
        log.info("Graceful shutdown initiated, flushing logs...");
        try {
            requestLogger.shutdown().block(SHUTDOWN_TIMEOUT);
            log.info("Logs flushed successfully");
        } catch (Exception e) {
            log.error("Error during log flush on shutdown", e);
        }
    }
}
