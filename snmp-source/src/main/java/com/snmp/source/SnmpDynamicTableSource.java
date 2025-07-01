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

    static {
        LOG.debug("{} SnmpDynamicTableSource: Static initializer called.",
            Thread.currentThread().getName()
        );
        System.out.println("SnmpDynamicTableSource: Static initializer called for Thread: "
            + Thread.currentThread().getName()
            + " (Direct System.out)"
        );
    }

    public SnmpDynamicTableSource(DataType producedDataType, List<SnmpAgentInfo> snmpAgentInfoList) {
        this.producedDataType  = producedDataType;
        this.snmpAgentInfoList = snmpAgentInfoList;
        LOG.debug("{} SnmpDynamicTableSource: Constructor called with {} agents.",
            Thread.currentThread().getName(),
            snmpAgentInfoList.size()
        );
        System.out.println("SnmpDynamicTableSource: Constructor called for Thread: "
            + Thread.currentThread().getName()
            + " with " + snmpAgentInfoList.size() + " agents (Direct System.out)"
        );
    }

    @Override
    public ChangelogMode getChangelogMode() {
        LOG.debug("{} SnmpDynamicTableSource: getChangelogMode() called.",
            Thread.currentThread().getName()
        );
        System.out.println("SnmpDynamicTableSource: getChangelogMode() called for Thread: "
            + Thread.currentThread().getName()
            + " (Direct System.out)"
        );
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

        LOG.debug("{} SnmpDynamicTableSource: copy() called.",
            Thread.currentThread().getName()
        );

        System.out.println("SnmpDynamicTableSource: copy() called for Thread: "
            + Thread.currentThread().getName()
            + " (Direct System.out)"
        );

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