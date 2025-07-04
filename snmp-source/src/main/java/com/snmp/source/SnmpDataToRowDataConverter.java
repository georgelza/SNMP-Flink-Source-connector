/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpDataToRowDataConverter.java
/
/       Description     :   Converts SnmpData objects to Flink's RowData format.
/
/       Created     	:   July 2025
/
/       copyright       :   Copyright 2025, - G Leonard, georgelza@gmail.com
/
/       GIT Repo        :   https://github.com/georgelza/SNMP-Flink-Source-connector
/
/       Blog            :\\\
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
//import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * A utility class to convert {@link SnmpData} objects into Flink's {@link RowData} format.
 * This converter handles the mapping of {@link SnmpData} fields to the schema defined
 * by the `producedDataType` (which corresponds to the Flink SQL `CREATE TABLE` statement).
 */
public class SnmpDataToRowDataConverter {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpDataToRowDataConverter.class);

    private SnmpDataToRowDataConverter() {
        // Private constructor to prevent instantiation
    }

    /**
     * Converts an {@link SnmpData} object into a Flink {@link RowData} object
     * based on the provided {@link DataType} schema.
     *
     * @param snmpData The {@link SnmpData} object to convert.
     * @param producedDataType The Flink {@link DataType} representing the schema of the output table.
     * @return A {@link RowData} object populated with data from the {@link SnmpData}.
     */
    public static RowData convert(SnmpData snmpData, DataType producedDataType) {
        if (snmpData == null) {
            LOG.warn("Attempted to convert a null SnmpData object to RowData. Returning null RowData.");
            return null;
        }

        RowType rowType         = (RowType) producedDataType.getLogicalType();
        GenericRowData rowData  = new GenericRowData(rowType.getFieldCount());

        // Map SnmpData fields to RowData based on the expected schema order from 2.1.creSNMPSource.sql
        // Columns in 2.1.creSNMPSource.sql:
        // device_id, metric_oid, metric_value, data_type, instance_identifier, ts, PROC_TIME

        // Note: PROC_TIME is a computed column in Flink SQL, so we only need to populate
        // the fields that come directly from SnmpData.

        for (int i = 0; i < rowType.getFieldCount(); i++) {

            String fieldName = rowType.getFields().get(i).getName();
           
            try {
                switch (fieldName) {
                    case "device_id":
                        if (snmpData.getDeviceId() != null) {
                            rowData.setField(i, StringData.fromString(snmpData.getDeviceId()));
                        } else {
                            rowData.setField(i, null);
                            LOG.warn("{} SnmpDataToRowDataConverter() convert() - SnmpData 'deviceId' is null for RowData field '{}'. Setting to NULL.", 
                                Thread.currentThread().getName(),
                                fieldName
                            );
                        }
                        break;

                    case "metric_oid":
                        if (snmpData.getMetricOid() != null) {
                            rowData.setField(i, StringData.fromString(snmpData.getMetricOid()));

                        } else {
                            rowData.setField(i, null);
                            LOG.warn("{} SnmpDataToRowDataConverter() convert() - SnmpData 'metricOid' is null for RowData field '{}'. Setting to NULL.", 
                                Thread.currentThread().getName(),
                                fieldName
                            );
                        }
                        break;

                    case "metric_value":
                        if (snmpData.getMetricValue() != null) {
                            rowData.setField(i, StringData.fromString(snmpData.getMetricValue()));
                        } else {
                            rowData.setField(i, null);
                            LOG.warn("{} SnmpDataToRowDataConverter() convert() - SnmpData 'metricValue' is null for RowData field '{}'. Setting to NULL.", 
                                Thread.currentThread().getName(),
                                fieldName
                            );
                        }
                        break;

                    case "data_type":
                        if (snmpData.getDataType() != null) {
                            rowData.setField(i, StringData.fromString(snmpData.getDataType()));
                        } else {
                            rowData.setField(i, null);
                            LOG.warn("{} SnmpDataToRowDataConverter() convert() - SnmpData 'dataType' is null for RowData field '{}'. Setting to NULL.", 
                                Thread.currentThread().getName(),
                                fieldName
                            );
                        }
                        break;

                    case "instance_identifier":
                        if (snmpData.getInstanceIdentifier() != null) {
                            rowData.setField(i, StringData.fromString(snmpData.getInstanceIdentifier()));
                        } else {
                            rowData.setField(i, null);
                            LOG.warn("{} SnmpDataToRowDataConverter() convert() - SnmpData 'instanceIdentifier' is null for RowData field '{}'. Setting to NULL.", 
                                Thread.currentThread().getName(),
                                fieldName
                            );
                        }
                        break;

                    case "ts":
                        LocalDateTime ts = snmpData.getTs();
                        if (ts != null) {
                            // The precision for TIMESTAMP(3) is milliseconds.
                            // TimestampData.fromLocalDateTime handles the conversion with appropriate precision.
                            rowData.setField(i, TimestampData.fromLocalDateTime(ts));
                        } else {
                            rowData.setField(i, null);
                            LOG.warn("{} SnmpDataToRowDataConverter() convert() - SnmpData 'ts' is null for RowData field '{}'. Setting to NULL.", 
                                Thread.currentThread().getName(),
                                fieldName
                            );
                        }
                        break;
                        
                    case "PROC_TIME":
                        // PROC_TIME is a Flink internal computed column (PROCTIME()), do not set it here.
                        // Flink runtime will handle its population.
                        rowData.setField(i, null); // Setting to null here is fine, Flink will overwrite.
                        break;

                    default:
                        // For any other unexpected field, set to null and log a warning
                        rowData.setField(i, null);
                        LOG.warn("{} SnmpDataToRowDataConverter() convert() - Unexpected field '{}' encountered in producedDataType. Setting to NULL.", 
                            Thread.currentThread().getName(),
                            fieldName
                        );
                        break;

                }
            } catch (Exception e) {
                LOG.error("{} SnmpDataToRowDataConverter() convert() - Error converting field '{}' to RowData for SnmpData: {}. Setting to NULL. Error: {} {}",
                    Thread.currentThread().getName(),
                    fieldName, 
                    snmpData.toString(), 
                    e.getMessage(), 
                    e
                );
                rowData.setField(i, null);
            }
        }
        return rowData;
    }
}