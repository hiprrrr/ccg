-- 为请求日志与 token 用量聚合表增加上游渠道字段，区分 AWS 与华为云
ALTER TABLE request_logs
    ADD COLUMN provider VARCHAR(16) NOT NULL DEFAULT 'AWS' COMMENT '上游渠道：AWS / HUAWEI' AFTER model;

CREATE INDEX idx_request_logs_provider ON request_logs (provider);

ALTER TABLE person_token_usage_hourly
    ADD COLUMN provider VARCHAR(16) NOT NULL DEFAULT 'AWS' COMMENT '上游渠道：AWS / HUAWEI' AFTER person_id;

ALTER TABLE person_token_usage_hourly
    DROP INDEX uk_person_window;

ALTER TABLE person_token_usage_hourly
    ADD UNIQUE KEY uk_person_provider_window (person_id, provider, window_start);

CREATE INDEX idx_person_token_usage_provider ON person_token_usage_hourly (provider);

ALTER TABLE person_model_token_usage_hourly
    ADD COLUMN provider VARCHAR(16) NOT NULL DEFAULT 'AWS' COMMENT '上游渠道：AWS / HUAWEI' AFTER model;

ALTER TABLE person_model_token_usage_hourly
    DROP INDEX uk_person_model_window;

ALTER TABLE person_model_token_usage_hourly
    ADD UNIQUE KEY uk_person_model_provider_window (person_id, model, provider, window_start);

CREATE INDEX idx_person_model_token_usage_provider ON person_model_token_usage_hourly (provider);
