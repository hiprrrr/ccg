package com.padb.ccg.core.spi;

import com.padb.ccg.core.model.RequestLogEntry;
import reactor.core.publisher.Mono;

/**
 * 请求日志记录器接口，采用 fire-and-forget 模式。
 * 实现类不得阻塞调用线程 —— {@code log()} 应将日志入队后立即返回。
 */
public interface RequestLogger {

    /**
     * 异步记录一条请求日志，调用后立即返回，不阻塞 Reactor 事件循环
     *
     * @param entry 请求日志条目
     */
    void log(RequestLogEntry entry);

    /**
     * 优雅关闭日志记录器，等待缓冲区中剩余日志写入完成
     *
     * @return 关闭完成的 Mono 信号
     */
    Mono<Void> shutdown();
}
