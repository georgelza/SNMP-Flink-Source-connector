### Some potential table structures

Some direct Flink Based, some REDIS look based.

Assuming snmp_device_info is a JDBC or Redis lookup table defined in Flink

- snmp_device_info is to manually maintained.
- snmp_oid_metadata is populated using snmp-mib-loader
- snmp_poll_data is populated by the snmp source connector and snmp job


```SQL
-- Assuming snmp_device_info is a lookup table (e.g., JDBC table)
SELECT
    device_info.device_id AS canonical_device_id, -- The device_id from your dimension table
    device_info.hostname,
    device_info.device_location,
    p.metric_oid,
    p.metric_value,
    p.data_type,
    p.instance_identifier,
    p.ts
FROM
    hive_catalog.snmp.snmp_poll_data AS p
JOIN
    hive_catalog.snmp.snmp_device_info FOR SYSTEM_TIME AS OF p.ts AS device_info
ON
    p.device_id = device_info.ip_address; -- Join using the IP address from the poll data
```