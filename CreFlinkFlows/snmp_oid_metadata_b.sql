--
CREATE TABLE hive_catalog.snmp.snmp_oid_metadata (
    oid_string          VARCHAR(255) PRIMARY KEY,               -- The numerical OID (e.g., ".1.3.6.1.2.1.1.1.0")
    oid_name            VARCHAR(255),                           -- The human-readable name (e.g., "sysDescr")
    description         VARCHAR(2000),                          -- The textual description from the MIB
    syntax_type         VARCHAR(255),                           -- The data type (e.g., "DisplayString", "Integer32")
    unit                VARCHAR(50),                            -- Optional: units (e.g., "seconds", "bytes")
    oid_type            VARCHAR(50),                            -- Optional: "scalar", "table", "notification", etc.
) WITH (
    'connector'             = 'jdbc',
    'url'                   = 'jdbc:postgresql://your_postgres_host:5432/snmp_metadata', -- Key Change: 'jdbc:postgresql' and default port 5432
    'table-name'            = 'public.snmp_oid_metadata',       -- Key Change: Often includes schema, e.g., 'public.your_table'
    'username'              = 'your_db_username',
    'password'              = 'your_db_password',
    'lookup.cache.max-rows' = '1000',                           -- Cache OID metadata
    'lookup.cache.ttl'      = '24h'                             -- OID metadata changes rarely, so a long TTL is fine
);