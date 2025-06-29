/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/ 
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpDynamicTableSource.java
/
/       Description     :   SNMP Source connector
/
/       Created     	:   June 2025
/
/       copyright       :   Copyright 2025, - G Leonard, georgelza@gmail.com
/
/       GIT Repo        :   https://github.com/georgelza/SNMP-Flink-Source-connector
/
/       Blog            :
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.connector.source.SourceProvider; // Added import for SourceProvider

import java.util.Collections;
import java.util.Objects;
import java.util.List;
import java.util.stream.Collectors; // Added import for Collectors

/**
 * A {@link DynamicTableSource} for the SNMP connector, implementing {@link ScanTableSource}.
 * This class translates the Flink SQL CREATE TABLE definition into an actual
 * Flink {@link Source} that can read data from SNMP agents.
 */
public class SnmpDynamicTableSource implements ScanTableSource {

    private final DataType producedDataType;
    private final SnmpAgentInfo snmpAgentInfo;

    /**
     * Constructor for SnmpDynamicTableSource.
     *
     * @param producedDataType The data type (schema) of the records this source will produce.
     * This comes from the Flink SQL CREATE TABLE statement.
     * @param snmpAgentInfo The configuration for the SNMP agent to poll.
     */
    public SnmpDynamicTableSource(DataType producedDataType, SnmpAgentInfo snmpAgentInfo) {

        System.out.println("SnmpDynamicTableSource: Constructor called. (Direct System.out)");
        
        this.producedDataType = Objects.requireNonNull(producedDataType, "Produced data type cannot be null.");
        this.snmpAgentInfo = Objects.requireNonNull(snmpAgentInfo, "SNMP agent info cannot be null.");
        
        System.out.println("SnmpDynamicTableSource: Initialized with data type: " + 
            producedDataType.toString() + " and agent: " + 
            snmpAgentInfo.getHost() + ":" + 
            snmpAgentInfo.getPort() + " (Direct System.out)");
    }

    /**
     * Specifies the changelog mode that this table source produces.
     * For a continuous polling source like SNMP, we typically produce inserts.
     *
     * @return The {@link ChangelogMode} this source supports.
     */
    @Override
    public ChangelogMode getChangelogMode() {
       
        System.out.println("SnmpDynamicTableSource: getChangelogMode() called. Returning 'INSERT_ONLY'. (Direct System.out)");
        
        return ChangelogMode.insertOnly();
    }

    /**
     * Creates a {@link ScanRuntimeProvider} which is responsible for providing the actual
     * runtime implementation for reading the data (i.e., the {@link SnmpSource}).
     *
     * @param runtimeProviderContext The context for creating the runtime provider.
     * @return A {@link ScanRuntimeProvider} instance.
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        
        System.out.println("SnmpDynamicTableSource: getScanRuntimeProvider() called. (Direct System.out)");

        // Explicitly cast the TypeInformation to RowData to resolve incompatible types
        @SuppressWarnings("unchecked") // Suppress warning as we expect RowData
        TypeInformation<RowData> typeInfo = (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(producedDataType);
        
        System.out.println("SnmpDynamicTableSource: Converted producedDataType to TypeInformation: " + 
            typeInfo.toString() + " (Direct System.out)"
        );

        // SnmpSource constructor now accepts (List<SnmpAgentInfo>, DataType)
        SnmpSource snmpSource = new SnmpSource(Collections.singletonList(snmpAgentInfo), producedDataType);
        System.out.println("SnmpDynamicTableSource: Created SnmpSource instance. (Direct System.out)");

        // Corrected: Use org.apache.flink.table.connector.source.SourceProvider.of()
        return SourceProvider.of(snmpSource);
    }

    /**
     * Creates a copy of this {@link DynamicTableSource} instance.
     * This is required by Flink for internal optimizations and operations.
     *
     * @return A deep copy of this source instance.
     */
    @Override
    public DynamicTableSource copy() {
        
        System.out.println("SnmpDynamicTableSource: copy() called. (Direct System.out)");
        
        return new SnmpDynamicTableSource(producedDataType, snmpAgentInfo);
    }

    @Override
    public boolean equals(Object o) {
        
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnmpDynamicTableSource that = (SnmpDynamicTableSource) o;
        
        return producedDataType.equals(that.producedDataType) &&
               snmpAgentInfo.equals(that.snmpAgentInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producedDataType, snmpAgentInfo);
    }
    /**
     * Returns a human-readable description of this table source.
     * Used for Flink's EXPLAIN PLAN output.
     *
     * @return A string description.
     */
    @Override
    public String asSummaryString() { // Corrected method name: asSummaryString()
        return "SNMP Polling Source";
    }
}