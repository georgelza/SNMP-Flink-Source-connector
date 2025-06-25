/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpData.java
/
/       Description     :   SNMP Source connector
/
/       Created     	:   June 2025
/
/       copyright       :   Copyright 2025, - G Leonard, georgelza@gmail.com
/                       
/       GIT Repo        :   
/
/       Blog            :   
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents a single record of SNMP polled data, matching the schema
 * defined in the Flink SQL `CREATE TABLE` statement.
 */
public class SnmpData implements Serializable {

    private static final long serialVersionUID = 1L;

    private String deviceId;
    private String metricOid;
    private String metricValue;
    private String dataType;
    private String instanceIdentifier; // For table-based OIDs
    private LocalDateTime ts; // Timestamp when the data was collected

    // Default constructor for serialization frameworks (e.g., Avro, Kryo)
    public SnmpData() {
    }

    /**
     * Constructs a new SnmpData object.
     *
     * @param deviceId The ID of the device (e.g., IP:Port).
     * @param metricOid The full OID of the polled metric.
     * @param metricValue The value of the metric.
     * @param dataType The SNMP data type (e.g., "Gauge", "Counter").
     * @param instanceIdentifier An identifier for table-based OIDs (e.g., interface index). Can be null.
     * @param ts The timestamp when the data was collected.
     */
    public SnmpData(
            String deviceId,
            String metricOid,
            String metricValue,
            String dataType,
            String instanceIdentifier,
            LocalDateTime ts) {
        this.deviceId = deviceId;
        this.metricOid = metricOid;
        this.metricValue = metricValue;
        this.dataType = dataType;
        this.instanceIdentifier = instanceIdentifier;
        this.ts = ts;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getMetricOid() {
        return metricOid;
    }

    public void setMetricOid(String metricOid) {
        this.metricOid = metricOid;
    }

    public String getMetricValue() {
        return metricValue;
    }

    public void setMetricValue(String metricValue) {
        this.metricValue = metricValue;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getInstanceIdentifier() {
        return instanceIdentifier;
    }

    public void setInstanceIdentifier(String instanceIdentifier) {
        this.instanceIdentifier = instanceIdentifier;
    }

    public LocalDateTime getTs() {
        return ts;
    }

    public void setTs(LocalDateTime ts) {
        this.ts = ts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnmpData snmpData = (SnmpData) o;
        return Objects.equals(deviceId, snmpData.deviceId) &&
                Objects.equals(metricOid, snmpData.metricOid) &&
                Objects.equals(metricValue, snmpData.metricValue) &&
                Objects.equals(dataType, snmpData.dataType) &&
                Objects.equals(instanceIdentifier, snmpData.instanceIdentifier) &&
                Objects.equals(ts, snmpData.ts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, 
            metricOid, 
            metricValue, 
            dataType, 
            instanceIdentifier, 
            ts
        );
    }

    @Override
    public String toString() {
        return "SnmpData{" +
                "deviceId='"             + deviceId + '\'' +
                ", metricOid='"          + metricOid + '\'' +
                ", metricValue='"        + metricValue + '\'' +
                ", dataType='"           + dataType + '\'' +
                ", instanceIdentifier='" + instanceIdentifier + '\'' +
                ", ts="                  + ts +
                '}';
    }
}
