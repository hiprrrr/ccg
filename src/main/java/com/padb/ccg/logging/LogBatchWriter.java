package com.padb.ccg.logging;

import com.padb.ccg.core.model.RequestLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

/**
 * 日志批量写入器，使用 JDBC batchUpdate 将请求日志批量写入 MySQL。
 * 单个批次失败不影响后续批次，仅记录错误日志。
 */
@Component
public class LogBatchWriter {

    private static final Logger log = LoggerFactory.getLogger(LogBatchWriter.class);

    /** 批量插入 SQL */
    private static final String SQL = """
            INSERT INTO request_logs (username, model, provider_id, success, error_msg, input_tokens, output_tokens, duration_ms, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;

    public LogBatchWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 批量写入日志条目
     *
     * @param entries 待写入的日志条目列表
     */
    public void writeBatch(List<RequestLogEntry> entries) {
        try {
            jdbcTemplate.batchUpdate(SQL, entries, entries.size(), (ps, entry) -> {
                ps.setString(1, entry.username());
                ps.setString(2, entry.model());
                ps.setString(3, entry.providerId());
                ps.setBoolean(4, entry.success());
                ps.setString(5, entry.errorMsg());
                // token 字段可为 null，使用 setObject 处理
                ps.setObject(6, entry.inputTokens(), java.sql.Types.INTEGER);
                ps.setObject(7, entry.outputTokens(), java.sql.Types.INTEGER);
                ps.setInt(8, entry.durationMs());
                ps.setTimestamp(9, Timestamp.from(entry.createdAt()));
            });
        } catch (Exception e) {
            log.error("Failed to write batch of {} log entries", entries.size(), e);
        }
    }
}
