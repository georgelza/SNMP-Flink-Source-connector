-- The IP is used to lookup snmp_device_info.device_id to then retrieve the device_id which is inserted with the data.
CREATE TABLE default_catalog.snmp.snmp_poll_data5 (
     device_id                VARCHAR(255)                       -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)                       -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)                      -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)                        -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)                       -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)                       -- Timestamp when the data was collected/scraped
    ,WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS               -- Adjust watermark delay as needed - event time
    ,PROC_TIME AS PROCTIME()                                     -- Flink Process time
) WITH (
     'connector'                    = 'snmp'
    ,'target'                       = '172.16.10.2:161'         -- snmp agent:port
    ,'snmp.version'                 = 'SNMPv1'                 
    ,'snmp.community-string'        = 'abfr24'                 
    ,'snmp.poll_mode'               = 'WALK'                 
    ,'oids'                         = '.1.3.6.1.2.1.1'                               
    ,'snmp.interval_seconds'        = '10'                          
    ,'snmp.timeout_seconds'         = '5'                          
    ,'snmp.retries'                 = '2'
);

--
CREATE TABLE default_catalog.snmp.snmp_poll_data6 (
     device_id                VARCHAR(255)                       -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)                       -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)                      -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)                        -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)                       -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)                       -- Timestamp when the data was collected/scraped
    ,WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS               -- Adjust watermark delay as needed - event time
    ,PROC_TIME AS PROCTIME()                                     -- Flink Process time
) WITH (
     'connector'                    = 'snmp'
    ,'target'                       = '172.16.10.2:161,172.16.10.3:161'     -- snmp agent:port
    ,'snmp.version'                 = 'SNMPv1'                 
    ,'snmp.community-string'        = 'abfr24'                 
    ,'snmp.poll_mode'               = 'WALK'                 
    ,'oids'                         = '.1.3.6.1.2.1.1'                               
    ,'snmp.interval_seconds'        = '10'                          
    ,'snmp.timeout_seconds'         = '5'                          
    ,'snmp.retries'                 = '2'
);

--
CREATE TABLE default_catalog.snmp.snmp_poll_data7 (
     device_id                VARCHAR(255)                       -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)                       -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)                      -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)                        -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)                       -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)                       -- Timestamp when the data was collected/scraped
    ,WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS               -- Adjust watermark delay as needed - event time
    ,PROC_TIME AS PROCTIME()                                     -- Flink Process time
) WITH (
     'connector'                    = 'snmp'
    ,'target'                       = '172.16.10.2:161'         -- snmp agent:port
    ,'snmp.version'                 = 'SNMPv3'                 
    ,'snmp.username'                = 'snmp'                 
    ,'snmp.password'                = 'abfr24'                 
    ,'snmp.poll_mode'               = 'WALK'                 
    ,'oids'                         = '1.3.6.1.2.1.2'
    ,'snmp.interval_seconds'        = '15'                          
    ,'snmp.timeout_seconds'         = '5'                          
    ,'snmp.retries'                 = '2'
    ,'snmp.auth-protocol'           = 'MD5'                     -- The SNMPv3 authentication protocol. Possible values: 'MD5', 'SHA', 'NONE'. Defaults to 'NONE'
    ,'snmp.priv-protocol'           = 'AES128'                  -- The SNMPv3 privacy protocol. Possible values: 'DES', 'AES', 'AES128', 'AES192', 'AES256', 'NONE'. Defaults to 'NONE'.
);

--
CREATE TABLE default_catalog.snmp.snmp_poll_data8 (
     device_id                VARCHAR(255)                       -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)                       -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)                      -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)                        -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)                       -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)                       -- Timestamp when the data was collected/scraped
    ,WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS               -- Adjust watermark delay as needed - event time
    ,PROC_TIME AS PROCTIME()                                     -- Flink Process time
) WITH (
     'connector'                    = 'snmp'
    ,'target'                       = '172.16.10.2:161,172.16.10.3'         -- snmp agent:port
    ,'snmp.version'                 = 'SNMPv3'                 
    ,'snmp.username'                = 'snmp'                 
    ,'snmp.password'                = 'abfr24'                 
    ,'snmp.poll_mode'               = 'WALK'                 
    ,'oids'                         = '1.3.6.1.2.1.2'
    ,'snmp.interval_seconds'        = '15'                          
    ,'snmp.timeout_seconds'         = '5'                          
    ,'snmp.retries'                 = '2'
    ,'snmp.auth-protocol'           = 'MD5'                     -- The SNMPv3 authentication protocol. Possible values: 'MD5', 'SHA', 'NONE'. Defaults to 'NONE'
    ,'snmp.priv-protocol'           = 'AES128'                  -- The SNMPv3 privacy protocol. Possible values: 'DES', 'AES', 'AES128', 'AES192', 'AES256', 'NONE'. Defaults to 'NONE'.
);