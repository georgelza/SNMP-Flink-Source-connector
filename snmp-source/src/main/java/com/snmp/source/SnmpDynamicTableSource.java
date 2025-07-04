// Updated SnmpDynamicTableSource.java
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
/       Blog            :\
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.connector.source.SourceProvider;

import java.util.Objects;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;


public class SnmpDynamicTableSource implements ScanTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpDynamicTableSource.class);

    private final DataType producedDataType;
    private final List<SnmpAgentInfo> snmpAgentInfoList;

    public SnmpDynamicTableSource(DataType producedDataType, List<SnmpAgentInfo> snmpAgentInfoList) {

        this.producedDataType = Objects.requireNonNull(producedDataType, "Produced data type must not be null.");
        this.snmpAgentInfoList = Objects.requireNonNull(snmpAgentInfoList, "SNMP agent info list must not be null.");
        
        LOG.debug("{} SnmpDynamicTableSource(): Initialized with producedDataType: {} and {} SNMP agents.",
            Thread.currentThread().getName(),
            producedDataType, 
            snmpAgentInfoList.size()
        );
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // For a polling source that only produces new data, ChangelogMode.insertOnly() is appropriate.
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // No change needed for how producedDataType is defined, it should already be available.
        // private final DataType producedDataType;

        // Pass producedDataType directly to SnmpSource
        Source<RowData, ?, ?> snmpSource = new SnmpSource(producedDataType, snmpAgentInfoList);

        return SourceProvider.of(snmpSource);
    }

    @Override
    public DynamicTableSource copy() {
        // Removed temporary debug log for copy method as well, assuming it was for one-off debugging
        return new SnmpDynamicTableSource(producedDataType, new ArrayList<>(snmpAgentInfoList));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnmpDynamicTableSource that = (SnmpDynamicTableSource) o;

        return producedDataType.equals(that.producedDataType) &&
               snmpAgentInfoList.equals(that.snmpAgentInfoList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producedDataType, snmpAgentInfoList);
    }

    @Override
    public String asSummaryString() {

        return "SNMP Polling Source (Targets: " + snmpAgentInfoList.stream()
            .map(agent -> agent.getHost() + ":" + agent.getPort())
            .collect(Collectors.joining(", ")) + ")";
    }
}