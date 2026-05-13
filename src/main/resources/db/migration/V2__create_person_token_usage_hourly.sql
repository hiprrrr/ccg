CREATE TABLE IF NOT EXISTS person_token_usage_hourly (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    person_id     VARCHAR(128) NOT NULL,
    window_start  DATETIME     NOT NULL,
    input_tokens  BIGINT       NOT NULL DEFAULT 0,
    output_tokens BIGINT       NOT NULL DEFAULT 0,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_person_window (person_id, window_start),
    INDEX idx_window_start (window_start),
    INDEX idx_person_id (person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
