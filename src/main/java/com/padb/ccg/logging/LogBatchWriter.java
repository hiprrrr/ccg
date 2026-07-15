package com.padb.ccg.logging;

import com.padb.ccg.core.model.ProviderNames;
import com.padb.ccg.core.model.RequestLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 日志批量写入器，使用 JDBC batchUpdate 将请求日志批量写入 MySQL。
 * 单个批次失败不影响后续批次，仅记录错误日志。
 */
@Component
public class LogBatchWriter {

    private static final Logger log = LoggerFactory.getLogger(LogBatchWriter.class);

    /** 批量插入 SQL */
    private static final String SQL = """
            INSERT INTO request_logs (username, model, provider, provider_id, success, error_msg, input_tokens, output_tokens, duration_ms, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    /** 北京时间时区，用于按自然小时聚合上游 token 消耗 */
    private static final ZoneId BEIJING_ZONE = ZoneId.of("Asia/Shanghai");

    /** 小时 token 用量累加 SQL，按 person_id + provider + window_start 做幂等窗口聚合 */
    private static final String USAGE_SQL = """
            INSERT INTO person_token_usage_hourly (person_id, provider, window_start, input_tokens, output_tokens, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                input_tokens = input_tokens + VALUES(input_tokens),
                output_tokens = output_tokens + VALUES(output_tokens),
                updated_at = VALUES(updated_at)
            """;

    /** 按模型维度的小时 token 用量累加 SQL，按 person_id + model + provider + window_start 做幂等窗口聚合 */
    private static final String MODEL_USAGE_SQL = """
            INSERT INTO person_model_token_usage_hourly (person_id, model, provider, window_start, input_tokens, output_tokens, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                input_tokens = input_tokens + VALUES(input_tokens),
                output_tokens = output_tokens + VALUES(output_tokens),
                updated_at = VALUES(updated_at)
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
                ps.setString(1, entry.personId());
                ps.setString(2, entry.model());
                ps.setString(3, providerName(entry.provider()));
                ps.setString(4, entry.providerId());
                ps.setBoolean(5, entry.success());
                ps.setString(6, entry.errorMsg());
                // token 字段可为 null，使用 setObject 处理
                ps.setObject(7, entry.inputTokens(), java.sql.Types.INTEGER);
                ps.setObject(8, entry.outputTokens(), java.sql.Types.INTEGER);
                ps.setInt(9, entry.durationMs());
                ps.setTimestamp(10, Timestamp.from(entry.createdAt()));
            });
            writeUsageBatch(entries);
        } catch (Exception e) {
            log.error("Failed to write batch of {} log entries", entries.size(), e);
        }
    }

    /**
     * 按个人、模型和北京时间小时窗口累加上游实际消耗的输入/输出 token。
     * 只有已产生上游 token 的请求才会进入聚合，认证失败或未转发上游的请求不会写入。
     */
    private void writeUsageBatch(List<RequestLogEntry> entries) {
        Map<UsageKey, UsageBucket> personBuckets = new LinkedHashMap<>();
        Map<ModelUsageKey, ModelUsageBucket> modelBuckets = new LinkedHashMap<>();
        for (RequestLogEntry entry : entries) {
            int inputTokens = entry.inputTokens() == null ? 0 : entry.inputTokens();
            int outputTokens = entry.outputTokens() == null ? 0 : entry.outputTokens();
            if (inputTokens <= 0 && outputTokens <= 0) {
                continue;
            }

            LocalDateTime windowStart = LocalDateTime.ofInstant(entry.createdAt(), BEIJING_ZONE)
                    .truncatedTo(ChronoUnit.HOURS);
            String provider = providerName(entry.provider());
            UsageKey personKey = new UsageKey(entry.personId(), provider, windowStart);
            personBuckets.compute(personKey, (ignored, bucket) -> {
                if (bucket == null) {
                    return new UsageBucket(personKey.personId(), personKey.provider(), personKey.windowStart(),
                            inputTokens, outputTokens);
                }
                return bucket.add(inputTokens, outputTokens);
            });

            ModelUsageKey modelKey = new ModelUsageKey(entry.personId(), entry.model(), provider, windowStart);
            modelBuckets.compute(modelKey, (ignored, bucket) -> {
                if (bucket == null) {
                    return new ModelUsageBucket(modelKey.personId(), modelKey.model(), modelKey.provider(),
                            modelKey.windowStart(), inputTokens, outputTokens);
                }
                return bucket.add(inputTokens, outputTokens);
            });
        }

        if (personBuckets.isEmpty()) {
            return;
        }

        Timestamp now = Timestamp.valueOf(LocalDateTime.now(BEIJING_ZONE));
        jdbcTemplate.batchUpdate(USAGE_SQL, personBuckets.values().stream().toList(), personBuckets.size(),
                (PreparedStatement ps, UsageBucket bucket) -> {
                    ps.setString(1, bucket.personId());
                    ps.setString(2, bucket.provider());
                    ps.setTimestamp(3, Timestamp.valueOf(bucket.windowStart()));
                    ps.setInt(4, bucket.inputTokens());
                    ps.setInt(5, bucket.outputTokens());
                    ps.setTimestamp(6, now);
                    ps.setTimestamp(7, now);
                });
        jdbcTemplate.batchUpdate(MODEL_USAGE_SQL, modelBuckets.values().stream().toList(), modelBuckets.size(),
                (PreparedStatement ps, ModelUsageBucket bucket) -> {
                    ps.setString(1, bucket.personId());
                    ps.setString(2, bucket.model());
                    ps.setString(3, bucket.provider());
                    ps.setTimestamp(4, Timestamp.valueOf(bucket.windowStart()));
                    ps.setInt(5, bucket.inputTokens());
                    ps.setInt(6, bucket.outputTokens());
                    ps.setTimestamp(7, now);
                    ps.setTimestamp(8, now);
                });
    }

    /** 将供应商标识规范为数据库中的稳定字符串 */
    private static String providerName(String provider) {
        if (provider == null || provider.isBlank()) {
            return ProviderNames.AWS;
        }
        return provider.trim();
    }

    /** 个人 + 渠道维度 token 用量小时窗口聚合 key */
    private record UsageKey(String personId, String provider, LocalDateTime windowStart) {
    }

    /** 个人 + 渠道维度 token 用量小时窗口聚合值 */
    private record UsageBucket(String personId, String provider, LocalDateTime windowStart,
                               int inputTokens, int outputTokens) {
        UsageBucket add(int inputDelta, int outputDelta) {
            return new UsageBucket(personId, provider, windowStart, inputTokens + inputDelta, outputTokens + outputDelta);
        }
    }

    /** 个人 + 模型 + 渠道维度 token 用量小时窗口聚合 key */
    private record ModelUsageKey(String personId, String model, String provider, LocalDateTime windowStart) {
    }

    /** 个人 + 模型 + 渠道维度 token 用量小时窗口聚合值 */
    private record ModelUsageBucket(String personId, String model, String provider, LocalDateTime windowStart,
                                    int inputTokens, int outputTokens) {
        ModelUsageBucket add(int inputDelta, int outputDelta) {
            return new ModelUsageBucket(personId, model, provider, windowStart,
                    inputTokens + inputDelta, outputTokens + outputDelta);
        }
    }
}
