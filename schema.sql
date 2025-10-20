-- Star Schema for Telecom Usage Analytics
-- Bronze -> Silver -> Gold Medallion Architecture

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone_number VARCHAR(20),
    account_type VARCHAR(50), -- prepaid, postpaid
    customer_segment VARCHAR(50), -- enterprise, consumer, SMB
    registration_date DATE,
    status VARCHAR(20), -- active, suspended, churned
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    effective_date DATE,
    expiration_date DATE
);

CREATE INDEX idx_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_customer_status ON dim_customer(status);
CREATE INDEX idx_customer_segment ON dim_customer(customer_segment);

-- Dimension: Network Tower/Cell
CREATE TABLE dim_network_cell (
    cell_key SERIAL PRIMARY KEY,
    cell_id VARCHAR(50) UNIQUE NOT NULL,
    tower_id VARCHAR(50),
    cell_name VARCHAR(100),
    technology VARCHAR(20), -- 4G, 5G, LTE
    frequency_band VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    coverage_radius_km DECIMAL(5, 2),
    max_capacity INT,
    region_id VARCHAR(50),
    status VARCHAR(20), -- active, maintenance, offline
    installation_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cell_id ON dim_network_cell(cell_id);
CREATE INDEX idx_tower_id ON dim_network_cell(tower_id);
CREATE INDEX idx_cell_status ON dim_network_cell(status);
CREATE INDEX idx_cell_location ON dim_network_cell USING gist(point(longitude, latitude));

-- Dimension: Region/Geography
CREATE TABLE dim_region (
    region_key SERIAL PRIMARY KEY,
    region_id VARCHAR(50) UNIQUE NOT NULL,
    region_name VARCHAR(100),
    region_type VARCHAR(50), -- city, state, country
    parent_region_id VARCHAR(50),
    population BIGINT,
    area_sq_km DECIMAL(10, 2),
    timezone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_region_id ON dim_region(region_id);
CREATE INDEX idx_parent_region ON dim_region(parent_region_id);

-- Dimension: Time (Precomputed)
CREATE TABLE dim_time (
    time_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    date_name VARCHAR(50),
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_number INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100),
    fiscal_year INT,
    fiscal_quarter INT
);

CREATE INDEX idx_full_date ON dim_time(full_date);
CREATE INDEX idx_year_month ON dim_time(year, month_number);

-- Dimension: Service Plan
CREATE TABLE dim_service_plan (
    plan_key SERIAL PRIMARY KEY,
    plan_id VARCHAR(50) UNIQUE NOT NULL,
    plan_name VARCHAR(100),
    plan_type VARCHAR(50), -- voice, data, bundle
    data_limit_gb INT,
    voice_minutes INT,
    sms_count INT,
    price DECIMAL(10, 2),
    validity_days INT,
    is_unlimited BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_plan_id ON dim_service_plan(plan_id);
CREATE INDEX idx_plan_type ON dim_service_plan(plan_type);

-- ============================================
-- FACT TABLES
-- ============================================

-- Fact: Call Detail Records (CDR)
CREATE TABLE fact_cdr (
    cdr_key BIGSERIAL PRIMARY KEY,
    customer_key INT REFERENCES dim_customer(customer_key),
    time_key INT REFERENCES dim_time(time_key),
    originating_cell_key INT REFERENCES dim_network_cell(cell_key),
    terminating_cell_key INT REFERENCES dim_network_cell(cell_key),
    plan_key INT REFERENCES dim_service_plan(plan_key),
    
    -- Call Details
    call_id VARCHAR(100) UNIQUE NOT NULL,
    call_type VARCHAR(20), -- voice, sms, data
    call_direction VARCHAR(10), -- inbound, outbound
    call_start_time TIMESTAMP,
    call_end_time TIMESTAMP,
    duration_seconds INT,
    
    -- Measurements
    data_volume_mb DECIMAL(12, 4),
    sms_count INT,
    voice_minutes DECIMAL(8, 2),
    
    -- Quality Metrics
    signal_strength_dbm INT,
    packet_loss_percent DECIMAL(5, 2),
    latency_ms INT,
    call_drop_flag BOOLEAN,
    
    -- Billing
    call_cost DECIMAL(10, 4),
    roaming_flag BOOLEAN,
    roaming_country VARCHAR(50),
    
    -- Metadata
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    batch_id VARCHAR(100)
);

-- Partitioning by month for performance
CREATE INDEX idx_cdr_customer ON fact_cdr(customer_key);
CREATE INDEX idx_cdr_time ON fact_cdr(time_key);
CREATE INDEX idx_cdr_call_start ON fact_cdr(call_start_time);
CREATE INDEX idx_cdr_call_type ON fact_cdr(call_type);
CREATE INDEX idx_cdr_batch ON fact_cdr(batch_id);

-- Fact: Network Events (Real-time stream)
CREATE TABLE fact_network_event (
    event_key BIGSERIAL PRIMARY KEY,
    cell_key INT REFERENCES dim_network_cell(cell_key),
    time_key INT REFERENCES dim_time(time_key),
    
    -- Event Details
    event_id VARCHAR(100) UNIQUE NOT NULL,
    event_type VARCHAR(50), -- congestion, outage, handover, interference
    event_severity VARCHAR(20), -- critical, high, medium, low
    event_timestamp TIMESTAMP NOT NULL,
    
    -- Metrics
    active_users INT,
    bandwidth_utilization_percent DECIMAL(5, 2),
    error_rate_percent DECIMAL(5, 2),
    avg_throughput_mbps DECIMAL(10, 2),
    
    -- Resolution
    resolved_flag BOOLEAN,
    resolution_time TIMESTAMP,
    root_cause VARCHAR(255),
    
    -- Metadata
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50)
);

CREATE INDEX idx_network_event_cell ON fact_network_event(cell_key);
CREATE INDEX idx_network_event_time ON fact_network_event(event_timestamp);
CREATE INDEX idx_network_event_type ON fact_network_event(event_type);
CREATE INDEX idx_network_event_severity ON fact_network_event(event_severity);

-- Fact: Customer Usage Summary (Aggregated Daily)
CREATE TABLE fact_customer_usage_daily (
    usage_key BIGSERIAL PRIMARY KEY,
    customer_key INT REFERENCES dim_customer(customer_key),
    time_key INT REFERENCES dim_time(time_key),
    plan_key INT REFERENCES dim_service_plan(plan_key),
    
    -- Daily Aggregates
    total_voice_minutes DECIMAL(10, 2),
    total_sms_count INT,
    total_data_mb DECIMAL(15, 2),
    total_calls INT,
    
    -- Quality Metrics
    avg_signal_strength_dbm INT,
    dropped_calls_count INT,
    avg_call_duration_seconds INT,
    
    -- Billing
    total_cost DECIMAL(12, 2),
    roaming_minutes DECIMAL(10, 2),
    
    -- Metadata
    calculation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(customer_key, time_key)
);

CREATE INDEX idx_usage_customer_time ON fact_customer_usage_daily(customer_key, time_key);
CREATE INDEX idx_usage_date ON fact_customer_usage_daily(time_key);

-- ============================================
-- AUDIT & METADATA TABLES
-- ============================================

-- Data Lineage Tracking
CREATE TABLE data_lineage (
    lineage_id SERIAL PRIMARY KEY,
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    transformation_name VARCHAR(100),
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_processed BIGINT,
    records_inserted BIGINT,
    records_updated BIGINT,
    records_failed BIGINT,
    status VARCHAR(20),
    error_message TEXT,
    execution_duration_seconds INT
);

-- Data Quality Checks
CREATE TABLE data_quality_log (
    check_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    check_name VARCHAR(100),
    check_type VARCHAR(50), -- completeness, accuracy, consistency, timeliness
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_checked BIGINT,
    records_passed BIGINT,
    records_failed BIGINT,
    pass_rate DECIMAL(5, 2),
    status VARCHAR(20),
    failure_details JSONB
);

-- ============================================
-- MATERIALIZED VIEWS FOR PERFORMANCE
-- ============================================

-- Top customers by usage
CREATE MATERIALIZED VIEW mv_top_customers_usage AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.customer_segment,
    SUM(u.total_data_mb) AS total_data_mb,
    SUM(u.total_voice_minutes) AS total_voice_minutes,
    SUM(u.total_cost) AS total_revenue,
    COUNT(DISTINCT u.time_key) AS active_days
FROM dim_customer c
JOIN fact_customer_usage_daily u ON c.customer_key = u.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment
ORDER BY total_revenue DESC;

CREATE UNIQUE INDEX idx_mv_top_customers ON mv_top_customers_usage(customer_id);

-- Network performance by cell
CREATE MATERIALIZED VIEW mv_network_performance AS
SELECT 
    nc.cell_id,
    nc.cell_name,
    nc.technology,
    r.region_name,
    COUNT(ne.event_key) AS total_events,
    SUM(CASE WHEN ne.event_severity = 'critical' THEN 1 ELSE 0 END) AS critical_events,
    AVG(ne.bandwidth_utilization_percent) AS avg_bandwidth_util,
    AVG(ne.error_rate_percent) AS avg_error_rate
FROM dim_network_cell nc
LEFT JOIN fact_network_event ne ON nc.cell_key = ne.cell_key
LEFT JOIN dim_region r ON nc.region_id = r.region_id
GROUP BY nc.cell_id, nc.cell_name, nc.technology, r.region_name;

CREATE UNIQUE INDEX idx_mv_network_perf ON mv_network_performance(cell_id);