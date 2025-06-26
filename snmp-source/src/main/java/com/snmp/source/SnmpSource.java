/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   Snmpsource.java
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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import java.util.List;

/**
 * A Flink {@link Source} implementation for polling data from SNMP agents.
 * This source is unbounded and continuously polls the configured agents.
 */
public class SnmpSource implements Source<RowData, SnmpSourceSplit, List<SnmpSourceSplit>> {

    private static final long serialVersionUID = 1L;

    private final List<SnmpAgentInfo> agentInfos;

    /**
     * Constructs a new SnmpSource.
     *
     * @param agentInfos The list of SNMP agent configurations.
     */
    public SnmpSource(List<SnmpAgentInfo> agentInfos) {
        this.agentInfos = agentInfos;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, SnmpSourceSplit> createReader(SourceReaderContext readerContext) {
        return new SnmpSourceReader(readerContext);
    }

    @Override
    public SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> createEnumerator(SplitEnumeratorContext<SnmpSourceSplit> enumContext) {
        // For a new enumerator (first start), no restored state is provided.
        return new SnmpSourceSplitEnumerator(agentInfos, enumContext, null); // Pass null for initial state
    }

    @Override
    public SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<SnmpSourceSplit> enumContext, List<SnmpSourceSplit> checkpointedState) {
            // When restoring, pass the checkpointedState directly to the enumerator's constructor.
            SnmpSourceSplitEnumerator enumerator = new SnmpSourceSplitEnumerator(agentInfos, enumContext, checkpointedState);
        return enumerator;
    }

    @Override
    public SimpleVersionedSerializer<SnmpSourceSplit> getSplitSerializer() {
        // It's highly recommended to use a custom serializer for splits
        // instead of SerializableSerializer for robustness and efficiency.
        // You need to implement SnmpSourceSplitSerializer.
        return new SnmpSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<List<SnmpSourceSplit>> getEnumeratorCheckpointSerializer() {
        // Similarly, use a custom serializer for the enumerator's checkpointed state.
        // You need to implement SnmpSourceSplitListSerializer.
        return new SnmpSourceSplitListSerializer();
    }
}