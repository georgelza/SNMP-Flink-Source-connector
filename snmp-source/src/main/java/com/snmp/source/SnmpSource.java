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
import org.apache.flink.table.types.DataType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;
import java.util.Objects;

/**
 * A Flink {@link Source} implementation for polling data from SNMP agents.
 * This source is unbounded and continuously polls the configured agents.
 */
public class SnmpSource implements Source<RowData, SnmpSourceSplit, List<SnmpSourceSplit>> {

    private static final long serialVersionUID = 1L;

    private final List<SnmpAgentInfo> agentInfos;
    private final DataType producedDataType;

    /**
     * Constructor for SnmpSource.
     *
     * @param agentInfos A list of {@link SnmpAgentInfo} objects, each representing an SNMP agent to poll.
     * @param producedDataType The {@link DataType} that this source will produce, derived from the SQL table schema.
     */
    public SnmpSource(List<SnmpAgentInfo> agentInfos, DataType producedDataType) {
        
        System.out.println("SnmpSource: Constructor called. (Direct System.out)");

        this.agentInfos         = Objects.requireNonNull(agentInfos, "Agent information list cannot be null.");
        this.producedDataType   = Objects.requireNonNull(producedDataType, "Produced data type cannot be null.");

        System.out.println("SnmpSource: Initialized with " + agentInfos.size() + " agent(s) and data type: " + producedDataType.toString() + " (Direct System.out)");
    }

    /**
     * Defines the boundedness of this source.
     * For a continuous polling source, this should be {@link Boundedness#CONTINUOUS_UNBOUNDED}.
     *
     * @return The boundedness of the source.
     */
    @Override
    public Boundedness getBoundedness() {

        System.out.println("SnmpSource: getBoundedness() called. Returning 'CONTINUOUS_UNBOUNDED'. (Direct System.out)");
        
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * Creates a {@link SourceReader} for this source.
     * The reader is responsible for fetching data from the actual SNMP agents.
     *
     * @param readerContext The context for the source reader.
     * @return A new instance of {@link SnmpSourceReader}.
     */
    @Override
    public SourceReader<RowData, SnmpSourceSplit> createReader(SourceReaderContext readerContext) {

        System.out.println("SnmpSource: createReader() called. (Direct System.out)");
        // Convert the DataType to TypeInformation which the SnmpSourceReader might implicitly need
        @SuppressWarnings("unchecked")
        TypeInformation<RowData> typeInfo = (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(producedDataType);
        System.out.println("SnmpSource: Converted producedDataType to TypeInformation for SnmpSourceReader: " + typeInfo.toString() + " (Direct System.out)");
        
        return new SnmpSourceReader(readerContext); // SnmpSourceReader takes SourceReaderContext
    }

    /**
     * Creates a {@link SplitEnumerator} for this source.
     * The enumerator is responsible for discovering and assigning splits (SNMP agents)
     * to the source readers.
     *
     * @param enumContext The context for the split enumerator.
     * @return A new instance of {@link SnmpSourceSplitEnumerator}.
     */
    @Override
    public SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> createEnumerator(SplitEnumeratorContext<SnmpSourceSplit> enumContext) {
    
        System.out.println("SnmpSource: createEnumerator() called (initial start). (Direct System.out)");
    
        // For a new enumerator (first start), no restored state is provided.
        // The enumerator will use the agentInfos provided in the constructor to discover splits.
        return new SnmpSourceSplitEnumerator(agentInfos, enumContext, null); // Pass null for initial state
    }

    /**
     * Restores a {@link SplitEnumerator} from checkpointed state.
     * This is used during recovery from failures.
     *
     * @param enumContext The context for the split enumerator.
     * @param checkpointedState The checkpointed state of the enumerator.
     * @return A restored instance of {@link SnmpSourceSplitEnumerator}.
     */
    @Override
    public SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<SnmpSourceSplit> enumContext, List<SnmpSourceSplit> checkpointedState) {

        System.out.println("SnmpSource: restoreEnumerator() called (recovery). (Direct System.out)");

        // When restoring, pass the checkpointedState directly to the enumerator's constructor.
        SnmpSourceSplitEnumerator enumerator = new SnmpSourceSplitEnumerator(agentInfos, enumContext, checkpointedState);

        System.out.println("SnmpSource: SnmpSourceSplitEnumerator restored with " + 
            checkpointedState.size() + 
            " splits. (Direct System.out)"
        );

        return enumerator;
    }

    /**
     * Returns the serializer for {@link SnmpSourceSplit} objects.
     * This is used by Flink for checkpointing and communication.
     *
     * @return A serializer for splits.
     */
    @Override
    public SimpleVersionedSerializer<SnmpSourceSplit> getSplitSerializer() {
        
        System.out.println("SnmpSource: getSplitSerializer() called. (Direct System.out)");
        
        // It's highly recommended to use a custom serializer for splits
        // instead of SerializableSerializer for robustness and efficiency.
        // You need to implement SnmpSourceSplitSerializer.
        return new SnmpSourceSplitSerializer();
    }

    /**
     * Returns the serializer for the enumerator's checkpointed state (a list of splits).
     *
     * @return A serializer for the enumerator's checkpointed state.
     */
    @Override
    public SimpleVersionedSerializer<List<SnmpSourceSplit>> getEnumeratorCheckpointSerializer() {

        System.out.println("SnmpSource: getEnumeratorCheckpointSerializer() called. (Direct System.out)");
        
        // Similarly, use a custom serializer for the enumerator's checkpointed state.
        // You need to implement SnmpSourceSplitListSerializer.
        return new SnmpSourceSplitListSerializer();
    }
}
