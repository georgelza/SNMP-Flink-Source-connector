-- PostgreSQL Example using a JDBC Connector
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
    'connector'             = 'jdbc',
    'url'                   = 'jdbc:postgresql://your_postgres_host:5432/snmp_metadata', -- Key Change: 'jdbc:postgresql' and default port 5432
    'table-name'            = 'public.snmp_device_info',        -- Key Change: Often includes schema, e.g., 'public.your_table'
    'username'              = 'your_db_username',
    'password'              = 'your_db_password',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl'      = '10min'
);

-- device_location => BN:xxx/FN:xxx/NR:xxx/RN:xxx/UN:xxx
-- BN => Building name
-- FN => Floor Number
-- NR => Network Room Number
-- RN => Rack Number
-- UN => Top U Number