-- Ensure ksqlDB reads from the beginning of newly created streams
SET 'auto.offset.reset' = 'earliest';

-- 1. Register the raw incoming telemetry/event stream
CREATE STREAM IF NOT EXISTS raw_telecom_events (
    event_id VARCHAR,
    timestamp VARCHAR,
    tower_id VARCHAR KEY,
    city VARCHAR,
    event_type VARCHAR,
    severity VARCHAR,
    affected_users INT,
    packet_loss_percent DOUBLE,
    network_type VARCHAR,
    operator VARCHAR
) WITH (
    KAFKA_TOPIC = 'telecom.events.raw',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
);

-- 2. Filter out routine 'heartbeat' noise & route actionable alerts to telecom.alerts.raw
CREATE STREAM IF NOT EXISTS filtered_telecom_alerts
WITH (
    KAFKA_TOPIC = 'telecom.alerts.raw',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
) AS
SELECT
    event_id,
    timestamp,
    tower_id,
    city,
    event_type,
    severity,
    affected_users,
    packet_loss_percent,
    network_type,
    operator
FROM raw_telecom_events
WHERE tower_id IS NOT NULL
  AND LCASE(TRIM(event_type)) != 'heartbeat'
  AND LCASE(TRIM(severity)) IN ('low', 'medium', 'high', 'critical')
EMIT CHANGES;

-- 3. Hot Path: Live 5-minute hopping window table per node
CREATE TABLE IF NOT EXISTS live_node_alerts_5m
WITH (
    KAFKA_TOPIC = 'telecom.alerts.live_5m',
    VALUE_FORMAT = 'JSON'
) AS
SELECT
    tower_id,
    COUNT(*) AS alert_count,
    SUM(CASE WHEN LCASE(TRIM(severity)) = 'critical' THEN 1 ELSE 0 END) AS critical_count,
    MAX(packet_loss_percent) AS peak_packet_loss
FROM filtered_telecom_alerts
WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)
GROUP BY tower_id
EMIT CHANGES;
