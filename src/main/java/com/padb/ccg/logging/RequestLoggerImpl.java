package com.padb.ccg.logging;

import com.padb.ccg.core.model.RequestLogEntry;
import com.padb.ccg.core.spi.RequestLogger;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 请求日志记录器实现，采用 fire-and-forget 异步模式。
 *
 * 架构设计：
 * - 使用 {@link LinkedBlockingQueue} 作为内存缓冲区（容量 1000）
 * - 定时调度线程每 5 秒批量刷新到数据库
 * - 缓冲区满时丢弃新日志（非阻塞），记录警告
 * - 关闭时排空剩余日志再退出
 */
@Component
public class RequestLoggerImpl implements RequestLogger {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggerImpl.class);

    /** 内存缓冲区最大容量 */
    private static final int BUFFER_CAPACITY = 1000;
    /** 定时刷新间隔（秒） */
    private static final int FLUSH_INTERVAL_SECONDS = 5;
    /** 单次刷新最大提取条数 */
    private static final int MAX_DRAIN = 500;
    /** 关闭时等待刷新的最大秒数 */
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 10;

    /** 日志缓冲区，有界阻塞队列 */
    private final LinkedBlockingQueue<RequestLogEntry> buffer = new LinkedBlockingQueue<>(BUFFER_CAPACITY);

    /** 单线程定时调度器，负责周期性刷新缓冲区 */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "log-flush");
        t.setDaemon(true);
        return t;
    });

    /** 运行状态标记 */
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final LogBatchWriter batchWriter;

    public RequestLoggerImpl(LogBatchWriter batchWriter) {
        this.batchWriter = batchWriter;
    }

    /** 启动定时刷新任务 */
    @PostConstruct
    void start() {
        scheduler.scheduleAtFixedRate(this::flush, FLUSH_INTERVAL_SECONDS, FLUSH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void log(RequestLogEntry entry) {
        // 非阻塞入队，缓冲区满时丢弃并记录警告
        if (!buffer.offer(entry)) {
            log.warn("Log buffer full ({}), dropping entry for user={}", BUFFER_CAPACITY, entry.username());
        }
    }

    /** 将缓冲区中的日志批量写入数据库 */
    void flush() {
        var entries = new ArrayList<RequestLogEntry>(MAX_DRAIN);
        buffer.drainTo(entries, MAX_DRAIN);
        if (!entries.isEmpty()) {
            log.debug("Flushing {} log entries to database", entries.size());
            batchWriter.writeBatch(entries);
        }
    }

    @Override
    public Mono<Void> shutdown() {
        running.set(false);
        scheduler.shutdown();
        try {
            // 等待定时任务终止
            if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        // 排空缓冲区中剩余的所有日志
        var remaining = new ArrayList<RequestLogEntry>(buffer.size());
        buffer.drainTo(remaining);
        if (!remaining.isEmpty()) {
            log.debug("Shutdown: flushing {} remaining log entries", remaining.size());
            batchWriter.writeBatch(remaining);
        }
        return Mono.empty();
    }
}
