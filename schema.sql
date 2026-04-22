CREATE TABLE IF NOT EXISTS raw_user_logs (
    raw_id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(100),
    timestamp_raw TEXT,
    action_type VARCHAR(100),
    metadata JSONB,
    source_file_name VARCHAR(255),
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS quarantine_user_logs (
    quarantine_id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(100),
    timestamp_raw TEXT,
    action_type VARCHAR(100),
    metadata JSONB,
    rejection_reason VARCHAR(255) NOT NULL,
    source_file_name VARCHAR(255),
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS dim_users (
    user_key     SERIAL PRIMARY KEY,
    user_id      VARCHAR(100) NOT NULL,
    device       VARCHAR(50),
    location     VARCHAR(100),
    CONSTRAINT uq_dim_users UNIQUE (user_id, device, location)
);

CREATE TABLE IF NOT EXISTS dim_actions (
    action_key   SERIAL PRIMARY KEY,
    action_type  VARCHAR(100) NOT NULL,
    CONSTRAINT uq_dim_actions UNIQUE (action_type)
);

CREATE TABLE IF NOT EXISTS fact_user_actions (
    fact_id          BIGSERIAL PRIMARY KEY,
    user_key         INT NOT NULL,
    action_key       INT NOT NULL,
    event_timestamp  TIMESTAMP NOT NULL,
    event_date       DATE NOT NULL,
    action_count     INT NOT NULL DEFAULT 1,
    CONSTRAINT fk_fact_user_actions_user
        FOREIGN KEY (user_key)
        REFERENCES dim_users (user_key),
    CONSTRAINT fk_fact_user_actions_action
        FOREIGN KEY (action_key)
        REFERENCES dim_actions (action_key),
    CONSTRAINT uq_fact_user_actions
        UNIQUE (user_key, action_key, event_timestamp),
    CONSTRAINT chk_fact_user_actions_count
        CHECK (action_count > 0)
);