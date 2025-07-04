-- MySQL CDC Example
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
    'connector'              = 'mysql-cdc',                         -- Specifies the MySQL CDC connector
    'hostname'               = 'your_mysql_host',                   -- Replace with your MySQL host
    'port'                   = '3306',                              -- MySQL port
    'username'               = 'your_cdc_username',                 -- MySQL user with replication privileges
    'password'               = 'your_cdc_password',                 -- Password for the CDC user
    'database-name'          = 'snmp_metadata',                     -- The database containing the device info table
    'table-name'             = 'snmp_device_info',                  -- The table containing device information
    'server-id'              = '5400-5500',                         -- Unique server ID for the Debezium connector in MySQL
    'scan.startup.mode'      = 'initial',                           -- 'initial' for a snapshot + streaming, 'latest-offset' for streaming only
    'debezium.snapshot.mode' = 'initial',                           -- Debezium specific snapshot mode
    'debezium.snapshot.locking.mode' = 'none'                       -- Avoids locking tables during snapshot for read-heavy tables
    -- 'debezium.decimal.handling.mode' = 'string'                  -- Optional: configure how decimal types are handled
    -- 'properties.include.schema.changes' = 'false'                -- Optional: Do not emit schema change events
);

-- device_location => BN:xxx/FN:xxx/NR:xxx/RN:xxx/UN:xxx
-- BN => Building name
-- FN => Floor Number
-- NR => Network Room Number
-- RN => Rack Number
-- UN => Top U Number


-- Prerequisites on your MyQL Server:

-- 'connector' = 'mysql-cdc': This is the core change. It tells Flink to use the dedicated MySQL CDC connector.
-- 'hostname', 'port', 'username', 'password', 'database-name', 'table-name': These specify the connection details for your MySQL database and the specific table you want to stream changes from.
-- 'server-id': This is crucial for MySQL replication. It must be a unique integer within the range of 1-2^32 - 1 for each Debezium connector instance connected to a MySQL server. If you have multiple Flink jobs or other Debezium connectors, ensure distinct IDs. The range '5400-5500' allows the connector to pick an available ID within that range.
-- 'scan.startup.mode':
-- 'initial' (recommended for dimension tables): Performs an initial snapshot of the table's current state and then continues to stream all subsequent changes.
-- 'latest-offset': Starts streaming changes from the latest binlog position, effectively ignoring historical data. Useful if you've already processed the historical data or only care about new changes.
-- 'debezium.*' properties: These pass through directly to the underlying Debezium connector.
-- 'debezium.snapshot.mode': Controls how the initial snapshot is taken. 'initial' is standard.
-- 'debezium.snapshot.locking.mode' = 'none': This is often useful for production tables to prevent the connector from acquiring a read lock during the initial snapshot, minimizing impact on the source database. Be aware of potential consistency implications if data is being heavily written during the initial snapshot.
-- Prerequisites on your MySQL Server:

-- To use the MySQL CDC connector, your MySQL database instance must be configured for binary logging with a row-based format:

-- Enable Binary Logging: In your my.cnf (or my.ini for Windows) configuration file:

-- Ini, TOML
-- [mysqld]
-- log_bin = mysql_bin
-- server_id = 123456789 # A unique ID for this MySQL server instance
-- binlog_format = ROW
-- expire_logs_days = 7 # Or a suitable retention period
--  (Remember to restart MySQL after making changes to my.cnf).

-- Create a Dedicated MySQL User: This user needs REPLICATION SLAVE and SELECT privileges on the database and tables you want to capture:

-- SQL
-- CREATE USER 'your_cdc_username'@'%' IDENTIFIED BY 'your_cdc_password';
-- GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'your_cdc_username'@'%';
-- FLUSH PRIVILEGES;
--  (Adjust the host '%' to a more specific IP range if possible for security).

-- Flink MySQL CDC Connector JAR: Download the appropriate Flink MySQL CDC connector JAR (e.g., flink-connector-mysql-cdc-*-with-dependencies.jar) and place it in your Flink distribution's lib directory.

-- By using the MySQL CDC connector, Flink will continuously receive updates to your snmp_device_info table, allowing your dimension lookups to always use the freshest data without needing to periodically reload the entire table.