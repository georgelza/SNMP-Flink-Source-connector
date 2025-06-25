--
CREATE TABLE snmp_oid_metadata (
    oid_string          VARCHAR(255) PRIMARY KEY,           -- The numerical OID (e.g., ".1.3.6.1.2.1.1.1.0")
    oid_name            VARCHAR(255),                       -- The human-readable name (e.g., "sysDescr")
    description         VARCHAR(2000),                      -- The textual description from the MIB
    syntax_type         VARCHAR(255),                       -- The data type (e.g., "DisplayString", "Integer32")
    unit                VARCHAR(50),                        -- Optional: units (e.g., "seconds", "bytes")
    oid_type            VARCHAR(50),                        -- Optional: "scalar", "table", "notification", etc.
) WITH (
    'connector'             = 'redis',
    'hostname'              = 'your_redis_host',
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
--   "oid_name": "sysDescr",
--   "description": "A textual description of the entity.",
--   "syntax_type": "DisplayString",
--   "unit": null,
--   "oid_type": "Scalar"
-- }