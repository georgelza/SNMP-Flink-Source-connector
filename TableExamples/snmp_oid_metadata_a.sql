-- NOTE: see master README.md with regard to the mib_parser utility
CREATE TABLE hive.snmp.snmp_oid_metadata (
     oid_string         VARCHAR(255) PRIMARY KEY NOT ENFORCED  -- The numerical OID (e.g., ".1.3.6.1.2.1.1.1.0")
    ,object_name        VARCHAR(255)                           -- The human-readable name (e.g., "sysDescr")
    ,data_type          VARCHAR(50)                            -- The data type (e.g., "DisplayString", "Integer32", "TimeTicks")
    ,info               VARCHAR(2000)                          -- The textual description from the MIB
    ,oid_type           VARCHAR(255)                           -- "scalar", "table", "notification", etc.
    ,mib_module         VARCHAR(50)                            -- Source file / Module Name
) WITH (
    'connector'             = 'redis',
    'hostname'              = 'redissnmp',
    'port'                  = '6379',
    'database'              = '0',
    'data-type'             = 'JSON',                       -- Or 'STRING' if storing full JSON strings
    'lookup.cache.max-rows' = '1000',
    'lookup.cache.ttl'      = '24h',                        -- OID metadata changes even less frequently
    'lookup.max-retries'    = '3',
    'key.prefix'            = 'oid:',                       -- Prefix for your Redis keys (e.g., 'oid:1.3.6.1.2.1.1.3.0')
    'value.format'          = 'json'
    -- 'password' = 'your_redis_password'
);


-- Redis Key: oid:.1.3.6.1.2.1.1.1.0
-- Redis Value (STRING - JSON):
-- JSON
-- {
--   "object_name": "sysDescr",
--   "data_type": "DisplayString",
--   "info": "A textual description of the entity.",
--   "oid_type": "Scalar"
--   "mib_module": "TRUENAS-MIB"
-- }