-- Staging table
CREATE TABLE IF NOT EXISTS connect.f_calls_staging (
    init_contact_id VARCHAR(100),
    prev_contact_id VARCHAR(100),
    contact_id VARCHAR(100) NOT NULL,
    next_contact_id VARCHAR(100),
    channel VARCHAR(50),
    init_method VARCHAR(100),
    init_time TIMESTAMP,
    disconn_time TIMESTAMP,
    disconn_reason VARCHAR(100),
    last_update_time TIMESTAMP,
    agent_conn TIMESTAMP,
    agent_id VARCHAR(100),
    agent_username VARCHAR(100),
    agent_conn_att INTEGER,
    agent_afw_start TIMESTAMP,
    agent_afw_end TIMESTAMP,
    agent_afw_duration INTEGER,
    agent_interact_duration INTEGER,
    agent_holds INTEGER,
    agent_longest_hold INTEGER,
    queue_id VARCHAR(100),
    queue_name VARCHAR(100),
    in_queue_time TIMESTAMP,
    out_queue_time TIMESTAMP,
    queue_duration INTEGER,
    customer_voice VARCHAR(50),
    customer_hold_duration INTEGER,
    contact_duration NUMERIC(10,2),
    sys_phone VARCHAR(50),
    conn_to_sys TIMESTAMP,
    customer_phone VARCHAR(50),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Main fact table (add DISTSTYLE and SORTKEY in production)
CREATE TABLE IF NOT EXISTS connect.f_calls (
    LIKE connect.f_calls_staging
);
