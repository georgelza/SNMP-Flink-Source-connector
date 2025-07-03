-- MySQL Example using a JDBC Connector
CREATE TABLE hive_catalog.snmp.snmp_device_info (
    device_id           VARCHAR(255) PRIMARY KEY NOT ENFORCED,  -- Unique identifier for the device
    ip_address          VARCHAR(45) NOT NULL,                   -- IP address of the device (IPv4 or IPv6)
    hostname            VARCHAR(255),                           -- Hostname of the device
    device_location     VARCHAR(255),                           -- Physical location of the device
    device_type         VARCHAR(100),                           -- Type of device (e.g., "router", "switch", "server")
    vendor              VARCHAR(100),                           -- Device vendor (e.g., "Cisco", "Juniper", "HP")
    model               VARCHAR(100),                           -- Device model
    firmware_version    VARCHAR(100),                           -- Firmware or OS version
    last_updated_ts     TIMESTAMP(3)                            -- Timestamp of the last update to this device's info
) WITH (
    'connector'             = 'jdbc',                           -- Example: Using JDBC connector for a relational database
    'url'                   = 'jdbc:mysql://your_mysql_host:3306/snmp_metadata', -- Replace with your DB URL
    'table-name'            = 'snmp_device_info',               -- The table name in your relational database
    'username'              = 'your_db_username',               -- Replace with your DB username
    'password'              = 'your_db_password',               -- Replace with your DB password
    'lookup.cache.max-rows' = '5000',                           -- Optional: Cache rows for faster lookups
    'lookup.cache.ttl'      = '10min'                           -- Optional: Time-to-live for cache entries
);

-- device_location => BN:xxx/FN:xxx/NR:xxx/RN:xxx/UN:xxx
-- BN => Building name
-- FN => Floor Number
-- NR => Network Room Number
-- RN => Rack Number
-- UN => Top U Number