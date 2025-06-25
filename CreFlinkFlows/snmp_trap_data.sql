--
CREATE TABLE hive_catalog.snmp.snmp_trap_data (
    trap_id             VARCHAR(36) NOT NULL,                   -- Unique identifier for the trap message (e.g., UUID)
    device_id           VARCHAR(255) NOT NULL,                  -- Foreign key referencing snmp_device_info.device_id
    trap_oid            VARCHAR(255) NOT NULL,                  -- OID of the trap (e.g., specificTrap, genericTrap OIDs)
    agent_address       VARCHAR(45),                            -- IP address of the SNMP agent sending the trap
    enterprise_oid      VARCHAR(255),                           -- Enterprise OID from the trap
    generic_trap        INT,                                    -- Generic trap type (e.g., 0 for coldStart, 6 for enterpriseSpecific)
    specific_trap       INT,                                    -- Specific trap code (used with enterpriseSpecific traps)
    variable_bindings   MAP<VARCHAR(255), VARCHAR(1000)>        -- Key-value pairs of the variable bindings (OID: Value)
    ts                  TIMESTAMP(3),                           -- Timestamp when the trap was received
    WATERMARK FOR ts AS ts - INTERVAL '10' SECONDS,             -- Adjust watermark delay as needed - event time
    PROC_TIME AS PROCTIME()                                     -- Flink Process time
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'snmp_traps_topic',                                -- Replace with your Kafka topic name for traps
    'properties.bootstrap.servers'  = 'your_kafka_broker1:9092,your_kafka_broker2:9092', -- Replace with your Kafka brokers
    'properties.group.id'           = 'flink_snmp_trap_consumer',
    'format'                        = 'json',
    'json.fail-on-missing-field'    = 'false',
    'json.ignore-parse-errors'      = 'true',
    'scan.startup.mode'             = 'latest-offset'
);