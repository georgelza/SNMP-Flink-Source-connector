/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/ 
/ 
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpSource.java
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
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink {@link Source} implementation for polling SNMP agents.
 * This source produces {@link RowData} records.
 */
public class SnmpSource implements Source<RowData, SnmpSourceSplit, List<SnmpSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSource.class);

    private final DataType producedDataType;
    private final List<SnmpAgentInfo> snmpAgentInfoList;

    static {
        LOG.debug("{} SnmpSource: Static initializer called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSource: Static initializer called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );
    }

    /**
     * Constructor for SnmpSource.
     * @param producedDataType The {@link DataType} that this source will produce.
     * @param snmpAgentInfoList A list of {@link SnmpAgentInfo} objects, each representing an SNMP agent
     * that needs to be polled.
     */
    public SnmpSource(DataType producedDataType, List<SnmpAgentInfo> snmpAgentInfoList) {
              
        LOG.debug("{} SnmpSource: Constructor called. Number of agents: {}.",
            Thread.currentThread().getName(),
            snmpAgentInfoList.size()
        );

        // System.out.println("SnmpSource: Constructor called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " Number of agents: "
        //     + snmpAgentInfoList.size()
        //     + " (Direct System.out)"
        // );

        this.producedDataType   = producedDataType;
        this.snmpAgentInfoList  = snmpAgentInfoList;
    }

    @Override
    public Boundedness getBoundedness() {

        LOG.debug("{} SnmpSource: getBoundedness() called. Returning UNBOUNDED.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSource: getBoundedness() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        // This source is unbounded as it continuously polls SNMP agents.
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * Creates a {@link SourceReader} for the given reader context.
     * @param readerContext The context for the source reader.
     * @return A new {@link SnmpSourceReader} instance.
     */
    @Override
    public SourceReader<RowData, SnmpSourceSplit> createReader(SourceReaderContext readerContext) {

        LOG.debug("{} SnmpSource: createReader() called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSource: createReader() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        // Create and return an instance of your SnmpSourceReader
        return new SnmpSourceReader(readerContext, producedDataType);
    }

    /**
     * Creates a {@link SplitEnumerator} for this source.
     *
     * @param enumContext The context for the split enumerator.
     * @param currentEnumeratorCheckpoint The current checkpointed state of the enumerator, or null if no checkpoint.
     * @return A new {@link SnmpSourceSplitEnumerator} instance.
     */
    @Override
    public SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<SnmpSourceSplit> enumContext,
            List<SnmpSourceSplit> currentEnumeratorCheckpoint) throws Exception { // Add throws Exception

        LOG.debug("{} SnmpSource: restoreEnumerator() called. Restoring {} splits.",
            Thread.currentThread().getName(),
            currentEnumeratorCheckpoint != null ? currentEnumeratorCheckpoint.size() : 0
        );

        // System.out.println("SnmpSource: createEnumerator() called for Thread: " 
        //     + Thread.currentThread().getName() 
        //     + " (Direct System.out)"
        // );

        // The currentEnumeratorCheckpoint is null on initial creation,
        // and Flink handles passing the restored checkpoint state to the constructor
        // during recovery/restart, which your enumerator constructor already handles.
        return new SnmpSourceEnumerator(enumContext, producedDataType, snmpAgentInfoList, currentEnumeratorCheckpoint);
    }

  
    /**
     * Returns the serializer for the splits.
     *
     * @return A serializer for {@link SnmpSourceSplit} objects.
     */
    @Override
    public SimpleVersionedSerializer<SnmpSourceSplit> getSplitSerializer() {

        LOG.debug("{} SnmpSource: getSplitSerializer() called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSource: getSplitSerializer() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        // Return your custom serializer for SnmpSourceSplit
        return new SnmpSourceSplitSerializer();
    }

    /**
     * Returns the serializer for the enumerator's checkpointed state (a list of splits).
     *
     * @return A serializer for the enumerator's checkpointed state.
     */
    @Override
    public SimpleVersionedSerializer<List<SnmpSourceSplit>> getEnumeratorCheckpointSerializer() {

        LOG.debug("{} SnmpSource: getEnumeratorCheckpointSerializer() called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSource: getEnumeratorCheckpointSerializer() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        // Similarly, use a custom serializer for the enumerator's checkpointed state.
        // You need to implement SnmpSourceSplitListSerializer.
        return new SnmpSourceSplitListSerializer();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnmpSource that = (SnmpSource) o;

        // Compare producedDataType and the list of SnmpAgentInfo
        return Objects.equals(producedDataType, that.producedDataType) &&
               Objects.equals(snmpAgentInfoList, that.snmpAgentInfoList);
    }

    @Override
    public int hashCode() {

        // Calculate hash code based on producedDataType and the list of SnmpAgentInfo
        return Objects.hash(producedDataType, snmpAgentInfoList);
    }
}