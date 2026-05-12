CREATE TABLE IF NOT EXISTS request_logs (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    username      VARCHAR(128)    NOT NULL,
    model         VARCHAR(128)    NOT NULL,
    provider_id   VARCHAR(64)     NOT NULL,
    success       TINYINT         NOT NULL DEFAULT 0,
    error_msg     TEXT            NULL,
    input_tokens  INT             NULL DEFAULT 0,
    output_tokens INT             NULL DEFAULT 0,
    duration_ms   INT             NOT NULL DEFAULT 0,
    created_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_username    (username),
    INDEX idx_created_at  (created_at),
    INDEX idx_model       (model),
    INDEX idx_success     (success)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
