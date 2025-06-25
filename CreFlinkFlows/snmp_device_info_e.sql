-- PostgreSQL CDC Example
CREATE TABLE hive_catalog.snmp.snmp_device_info (
    device_id           VARCHAR(255) PRIMARY KEY NOT ENFORCED,      -- Unique identifier for the device
    ip_address          VARCHAR(45) NOT NULL,                       -- IP address of the device (IPv4 or IPv6)
    hostname            VARCHAR(255),                               -- Hostname of the device
    device_location     VARCHAR(255),                               -- Physical location of the device
    device_type         VARCHAR(100),                               -- Type of device (e.g., "router", "switch", "server")
    vendor              VARCHAR(100),                               -- Device vendor (e.g., "Cisco", "Juniper", "HP")
    model               VARCHAR(100),                               -- Device model
    firmware_version    VARCHAR(100),                               -- Firmware or OS version
    last_updated_ts     TIMESTAMP(3)                                -- Timestamp of the last update to this device's info
) WITH (
    'connector'                         = 'postgresql-cdc',         -- Specifies the PostgreSQL CDC connector
    'hostname'                          = 'your_postgres_host',     -- Replace with your PostgreSQL host
    'port'                              = '5432',                   -- PostgreSQL default port
    'username'                          = 'your_cdc_username',      -- PostgreSQL user with replication privileges
    'password'                          = 'your_cdc_password',      -- Password for the CDC user
    'database-name'                     = 'snmp_metadata',          -- The database containing the device info table
    'schema-name'                       = 'public',                 -- The schema containing the device info table (e.g., 'public')
    'table-name'                        = 'snmp_device_info',       -- The table containing device information
    'replication-slot-name'             = 'flink_snmp_device_slot', -- Unique replication slot name
    'scan.startup.mode'                 = 'initial',                -- 'initial' for a snapshot + streaming, 'latest-offset' for streaming only
    'debezium.snapshot.mode'            = 'initial',                -- Debezium specific snapshot mode
    'debezium.heartbeat.interval.ms'    = '60000'                   -- Optional: Send heartbeats every 60 seconds
    -- You can add other Debezium properties as needed, e.g., 'debezium.decimal.handling.mode' = 'string'
);

-- device_location => BN:xxx/FN:xxx/NR:xxx/RN:xxx/UN:xxx
-- BN => Building name
-- FN => Floor Number
-- NR => Network Room Number
-- RN => Rack Number
-- UN => Top U Number


-- Prerequisites on your PostgreSQL Server:

-- To use the PostgreSQL CDC connector, your PostgreSQL database instance must be configured to support logical decoding:

-- Enable WAL Archiving and Logical Decoding:
-- In your postgresql.conf configuration file (typically found in your PostgreSQL data directory):

-- Ini, TOML
-- # --- Replication settings ---
-- wal_level = logical          # Enables logical decoding
-- max_wal_senders = 10         # Or more, depending on your needs (number of concurrent replication connections)
-- max_replication_slots = 10   # Or more, depending on the number of CDC connectors
-- # --- Optional: If you use the 'wal2json' plugin ---
-- # shared_preload_libraries = 'wal2json' # Uncomment and reload if using wal2json
--  (Remember to restart PostgreSQL after making changes to postgresql.conf.)

-- Create a Dedicated PostgreSQL User: This user needs REPLICATION privileges (specifically REPLICATION role) and SELECT privileges on the database and tables you want to capture:

-- SQL
-- CREATE USER your_cdc_username WITH PASSWORD 'your_cdc_password';
-- ALTER USER your_cdc_username WITH REPLICATION; -- Grant replication privileges
-- GRANT SELECT ON your_device_info_table TO your_cdc_username;
-- -- If your table is in a specific schema and the user doesn't have default access:
-- -- GRANT USAGE ON SCHEMA public TO your_cdc_username;
--  Flink PostgreSQL CDC Connector JAR: Download the appropriate Flink PostgreSQL CDC connector JAR (e.g., flink-connector-postgres-cdc-*-with-dependencies.jar) and place it in your Flink distribution's lib directory.

-- With these configurations, your Flink job can directly consume real-time changes from your PostgreSQL snmp_device_info table.