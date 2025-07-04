-- REDIS Example
CREATE TABLE hive_catalog.snmp.snmp_device_info (
    device_id               VARCHAR(255) PRIMARY KEY NOT ENFORCED,  -- Unique identifier for the device
    ip_address              VARCHAR(45) NOT NULL,                   -- IP address of the device (IPv4 or IPv6)
    hostname                VARCHAR(255),                           -- Hostname of the device
    device_location         VARCHAR(255),                           -- Physical location of the device
    device_type             VARCHAR(100),                           -- Type of device (e.g., "router", "switch", "server")
    vendor                  VARCHAR(100),                           -- Device vendor (e.g., "Cisco", "Juniper", "HP")
    model                   VARCHAR(100),                           -- Device model
    firmware_version        VARCHAR(100),                           -- Firmware or OS version
    last_updated_ts         TIMESTAMP(3)                            -- Timestamp of the last update to this device's info
) WITH (
    'connector'             = 'redis',                              -- This is a placeholder; connector name might vary
    'hostname'              = 'your_redis_host',                    -- Replace with your Redis host
    'port'                  = '6379',                               -- Replace with your Redis port
    'database'              = '0',                                  -- Redis database number
    'data-type'             = 'JSON',                               -- Or 'STRING' if you're storing full JSON strings
    'lookup.cache.max-rows' = '5000',                               -- Cache lookup results for performance
    'lookup.cache.ttl'      = '10min',                              -- How long cached entries are valid
    'lookup.max-retries'    = '3',                                  -- Max retries for failed lookups
    'key.prefix'            = 'device:',                            -- Prefix for your Redis keys (e.g., 'device:router-core-01')
    'value.format'          = 'json'                                -- Specify that the value is JSON
    -- Add any other Redis specific configurations like password, SSL, connection pool settings
    -- 'password' = 'your_redis_password'
);

-- device_location => BN:xxx/FN:xxx/NR:xxx/RN:xxx/UN:xxx
-- BN => Building name
-- FN => Floor Number
-- NR => Network Room Number
-- RN => Rack Number
-- UN => Top U Number