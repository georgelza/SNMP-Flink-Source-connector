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
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink {@link Source} implementation for polling SNMP agents.
 * This source produces {@link RowData} records.
 */
public class SnmpSource implements Source<RowData, SnmpSourceSplit, List<SnmpSourceSplit>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSource.class);

    private final DataType producedDataType;
    private final List<SnmpAgentInfo> snmpAgentInfoList;

    /**
     * Constructs a new SnmpSource.
     *
     * @param producedDataType  The data type of the records produced by this source.
     * @param snmpAgentInfoList A list of SNMP agent configurations to poll.
     */
    public SnmpSource(DataType producedDataType, List<SnmpAgentInfo> snmpAgentInfoList) {

        this.producedDataType  = Objects.requireNonNull(producedDataType, "Produced data type cannot be null.");
        this.snmpAgentInfoList = Objects.requireNonNull(snmpAgentInfoList, "SNMP agent info list cannot be null.");

        if (snmpAgentInfoList.isEmpty()) {
            throw new IllegalArgumentException("SNMP agent info list cannot be empty.");
        }

        LOG.debug("{} SnmpSource: Initialized with {} SNMP agents.",
            Thread.currentThread().getName(),
            snmpAgentInfoList.size()
        );

        System.out.println("SnmpSource: Initialized with " 
            + snmpAgentInfoList.size()
            + " agents"
            + " for Thread: " + Thread.currentThread().getName() 
            + " (Direct System.out)"
        );

    }

    /**
     * Creates a {@link SourceReader} for the SNMP source.
     *
     * @param readerContext The context for the source reader.
     * @return A new instance of {@link SnmpSourceReader}.
     */
    @Override
    public SourceReader<RowData, SnmpSourceSplit> createReader(SourceReaderContext readerContext) {

        LOG.debug("{} SnmpSource: createReader() called.",
            Thread.currentThread().getName()
        );

        System.out.println("SnmpSource: createReader() called" 
            + " for Thread: " + Thread.currentThread().getName() 
            + " (Direct System.out)"
        );

        return new SnmpSourceReader(readerContext, producedDataType);
    }

    /**
     * Creates a {@link SplitEnumerator} for the SNMP source.
     * This method is called during job initialization.
     *
     * @param enumContext The context for the split enumerator.
     * @return A new instance of {@link SnmpSourceEnumerator}.
     */
    @Override
    public SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> createEnumerator(
            SplitEnumeratorContext<SnmpSourceSplit> enumContext) {

        LOG.debug("{} SnmpSource: createEnumerator() called for initial setup.",
            Thread.currentThread().getName()
        );

        return new SnmpSourceEnumerator(enumContext, snmpAgentInfoList, true);
    }

    /**
     * Restores a {@link SplitEnumerator} from checkpointed state.
     * This method is called when a job is restored from a checkpoint.
     *
     * @param enumContext The context for the split enumerator.
     * @param checkpointedState The state restored from the checkpoint.
     * @return A new instance of {@link SnmpSourceEnumerator}.
     * @throws IOException If an I/O error occurs during state restoration.
     */
    @Override
    public SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<SnmpSourceSplit> enumContext,
            List<SnmpSourceSplit> checkpointedState) throws IOException {

        LOG.debug("{} SnmpSource: restoreEnumerator() called with {} splits.",
            Thread.currentThread().getName(),
            checkpointedState.size()
        );

        return new SnmpSourceEnumerator(enumContext, checkpointedState);
    }

    /**
     * Returns the boundedness of the source (e.g., Bounded or Unbounded).
     * For an SNMP polling source, this will typically be {@link Boundedness#CONTINUOUS_UNBOUNDED}.
     *
     * @return The boundedness of the source.
     */
    @Override
    public Boundedness getBoundedness() {

        LOG.debug("{} SnmpSource: getBoundedness() called. Returning CONTINUOUS_UNBOUNDED.",
            Thread.currentThread().getName()
        );

        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * Returns the serializer for {@link SnmpSourceSplit} instances.
     *
     * @return A serializer for splits.
     */
    @Override
    public SimpleVersionedSerializer<SnmpSourceSplit> getSplitSerializer() {

        LOG.debug("{} SnmpSource: getSplitSerializer() called.",
            Thread.currentThread().getName()
        );

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

        return new SnmpSourceSplitListSerializer();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnmpSource that = (SnmpSource) o;

        return Objects.equals(producedDataType, that.producedDataType) &&
               Objects.equals(snmpAgentInfoList, that.snmpAgentInfoList);
    }

    @Override
    public int hashCode() {

        return Objects.hash(producedDataType, snmpAgentInfoList);
    }
}