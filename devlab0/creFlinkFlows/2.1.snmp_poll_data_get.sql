-- The IP is used to lookup snmp_device_info.device_id to then retrieve the device_id which is inserted with the data.
CREATE TABLE hive.snmp.snmp_poll_data1 (
     device_id                VARCHAR(255)   NULL                -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)   NULL                -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)  NULL                -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)    NULL                -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)   NULL                -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)   NULL                -- Timestamp when the data was collected/scraped
) WITH (
     'connector'                    = 'snmp'
    ,'target'                       = '172.16.10.2:161'         -- snmp agent:port
    ,'snmp.version'                 = 'SNMPv1'                 
    ,'snmp.community-string'        = 'abfr24'                 
    ,'snmp.poll_mode'               = 'GET'                 
    ,'oids'                         = '.1.3.6.1.2.1.1.5.0'      -- 1 OIDs  - SNMPv2-MIB::sysName.0
    ,'snmp.interval_seconds'        = '10'                          
    ,'snmp.timeout_seconds'         = '5'                          
    ,'snmp.retries'                 = '2'
);


CREATE TABLE hive.snmp.snmp_poll_data2 (
     device_id                VARCHAR(255)   NULL                -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)   NULL                -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)  NULL                -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)    NULL                -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)   NULL                -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)   NULL                -- Timestamp when the data was collected/scraped
) WITH (
     'connector'                    = 'snmp'
    ,'target'                       = '172.16.10.2:161'         -- snmp 1 agent:port
    ,'snmp.version'                 = 'SNMPv1'                 
    ,'snmp.community-string'        = 'abfr24'                 
    ,'snmp.poll_mode'               = 'GET'                 
    ,'oids'                         = '1.3.6.1.2.1.2.2.1.2.2,1.3.6.1.2.1.2.2.1.10.2,1.3.6.1.2.1.2.2.1.16.2,1.3.6.1.2.1.2.2.1.2.3,1.3.6.1.2.1.2.2.1.10.3,1.3.6.1.2.1.2.2.1.16.3'
    ,'snmp.interval_seconds'        = '10'                          
    ,'snmp.timeout_seconds'         = '5'                          
    ,'snmp.retries'                 = '2'
);


CREATE TABLE hive.snmp.snmp_poll_data3 (
     device_id                VARCHAR(255)   NULL                -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)   NULL                -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)  NULL                -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)    NULL                -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)   NULL                -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)   NULL                -- Timestamp when the data was collected/scraped
) WITH (
     'connector'                    = 'snmp'
    ,'target'                       = '172.16.10.2:161,172.16.10.3:161' -- snmp 1 agent:port
    ,'snmp.version'                 = 'SNMPv1'                 
    ,'snmp.community-string'        = 'abfr24'                 
    ,'snmp.poll_mode'               = 'GET'                 
    ,'oids'                         = '1.3.6.1.2.1.2.2.1.2.2,1.3.6.1.2.1.2.2.1.10.2,1.3.6.1.2.1.2.2.1.16.2,1.3.6.1.2.1.2.2.1.2.3,1.3.6.1.2.1.2.2.1.10.3,1.3.6.1.2.1.2.2.1.16.3'
    ,'snmp.interval_seconds'        = '10'                          
    ,'snmp.timeout_seconds'         = '5'                          
    ,'snmp.retries'                 = '2'
);

--
CREATE TABLE hive.snmp.snmp_poll_data4 (
     device_id                VARCHAR(255)   NULL                -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)   NULL                -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)  NULL                -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)    NULL                -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)   NULL                -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)   NULL                -- Timestamp when the data was collected/scraped
) WITH (
     'connector'                    = 'snmp'
    ,'target'                       = '172.16.10.2:161,172.16.10.3:161'     -- snmp agent:port
    ,'snmp.version'                 = 'SNMPv3'                 
    ,'snmp.username'                = 'snmp'                 
    ,'snmp.password'                = 'abfr24'                 
    ,'snmp.poll_mode'               = 'GET'                 
    ,'oids'                         = '1.3.6.1.2.1.2.2.1.2.2,1.3.6.1.2.1.2.2.1.10.2,1.3.6.1.2.1.2.2.1.16.2,1.3.6.1.2.1.2.2.1.2.3,1.3.6.1.2.1.2.2.1.10.3,1.3.6.1.2.1.2.2.1.16.3'
    ,'snmp.interval_seconds'        = '15'                          
    ,'snmp.timeout_seconds'         = '5'                          
    ,'snmp.retries'                 = '2'
    ,'snmp.auth-protocol'           = 'MD5'                     -- The SNMPv3 authentication protocol. Possible values: 'MD5', 'SHA', 'NONE'. Defaults to 'NONE'
    ,'snmp.priv-protocol'           = 'AES128'                  -- The SNMPv3 privacy protocol. Possible values: 'DES', 'AES', 'AES128', 'AES192', 'AES256', 'NONE'. Defaults to 'NONE'.
);
