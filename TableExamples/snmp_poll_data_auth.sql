CREATE TABLE hive.snmp.snmp_poll_data8 (
     device_id                VARCHAR(255)   NOT NULL           -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)   NOT NULL           -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)  NOT NULL           -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)    NOT NULL           -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)   NULL               -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)   NOT NULL           -- Timestamp when the data was collected/scraped
    ,WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS              -- Adjust watermark delay as needed - event time
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

CREATE TABLE hive.snmp.snmp_poll_data9 (
     device_id                VARCHAR(255)   NOT NULL           -- Foreign key referencing snmp_device_info.device_id
    ,metric_oid               VARCHAR(255)   NOT NULL           -- Object Identifier (OID) of the polled metric
    ,metric_value             VARCHAR(1000)  NOT NULL           -- The value of the metric (store as string for flexibility)
    ,data_type                VARCHAR(50)    NOT NULL           -- The SNMP data type (e.g., "Gauge", "Counter", "Integer", "OctetString")
    ,instance_identifier      VARCHAR(255)   NULL               -- For table-based OIDs (e.g., interface index)
    ,ts                       TIMESTAMP(3)   NOT NULL           -- Timestamp when the data was collected/scraped
    ,WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS              -- Adjust watermark delay as needed - event time
) WITH (
     'connector'                    = 'snmp'
    ,'target'                       = '172.16.10.2:161,172.16.10.3'         -- snmp agent:port
    ,'snmp.version'                 = 'SNMPv3'                 
    ,'snmp.username'                = 'snmp'                 
    ,'snmp.password'                = 'abfr24'                 
    ,'snmp.poll_mode'               = 'WALK'                 
    ,'oids'                         = '1.3.6.1.2.1'             -- 1 OIDs
    ,'snmp.interval_seconds'        = '15'                          
    ,'snmp.timeout_seconds'         = '5'                          
    ,'snmp.retries'                 = '2'
    ,'snmp.auth-protocol'           = 'MD5'                     -- The SNMPv3 authentication protocol. Possible values: 'MD5', 'SHA', 'NONE'. Defaults to 'NONE'
    ,'snmp.priv-protocol'           = 'AES128'                  -- The SNMPv3 privacy protocol. Possible values: 'DES', 'AES', 'AES128', 'AES192', 'AES256', 'NONE'. Defaults to 'NONE'.
);