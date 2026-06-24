CREATE TABLE IF NOT EXISTS person_model_token_usage_hourly (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    person_id     VARCHAR(128) NOT NULL,
    model         VARCHAR(128) NOT NULL,
    window_start  DATETIME     NOT NULL,
    input_tokens  BIGINT       NOT NULL DEFAULT 0,
    output_tokens BIGINT       NOT NULL DEFAULT 0,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_person_model_window (person_id, model, window_start),
    INDEX idx_window_start (window_start),
    INDEX idx_person_id (person_id),
    INDEX idx_model (model)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
