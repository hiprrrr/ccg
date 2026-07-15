-- 扩宽 provider 列以支持 other-providers 中的供应商名称（不再限于 AWS/HUAWEI 枚举短名）
ALTER TABLE request_logs
    MODIFY COLUMN provider VARCHAR(64) NOT NULL DEFAULT 'aws' COMMENT '上游渠道：aws 或 other-providers 名';

ALTER TABLE person_token_usage_hourly
    MODIFY COLUMN provider VARCHAR(64) NOT NULL DEFAULT 'aws' COMMENT '上游渠道：aws 或 other-providers 名';

ALTER TABLE person_model_token_usage_hourly
    MODIFY COLUMN provider VARCHAR(64) NOT NULL DEFAULT 'aws' COMMENT '上游渠道：aws 或 other-providers 名';
