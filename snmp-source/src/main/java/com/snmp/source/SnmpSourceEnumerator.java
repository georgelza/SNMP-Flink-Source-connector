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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Collection;


/**
 * The {@link SnmpSourceEnumerator} is responsible for coordinating the splits for the SNMP source.
 */
public class SnmpSourceEnumerator implements SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSourceEnumerator.class);

    private final SplitEnumeratorContext<SnmpSourceSplit> enumContext;
    private final DataType producedDataType;
    private final List<SnmpAgentInfo> snmpAgentInfoList;
    private final List<SnmpSourceSplit> currentEnumeratorCheckpoint;


    public SnmpSourceEnumerator(
            SplitEnumeratorContext<SnmpSourceSplit> enumContext,
            DataType producedDataType,
            List<SnmpAgentInfo> snmpAgentInfoList,
            @Nullable List<SnmpSourceSplit> currentEnumeratorCheckpoint) {

        LOG.debug("{} SnmpSourceEnumerator: Constructor called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSourceEnumerator: Constructor called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        this.enumContext                    = enumContext;
        this.producedDataType               = producedDataType;
        this.snmpAgentInfoList              = snmpAgentInfoList;
        this.currentEnumeratorCheckpoint    = currentEnumeratorCheckpoint != null ?
            new ArrayList<>(currentEnumeratorCheckpoint) :
            new ArrayList<>();
    }

    @Override
    public void start() {
        
        LOG.debug("{} SnmpSourceEnumerator: start() called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSourceEnumerator: start() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        if (currentEnumeratorCheckpoint.isEmpty()) {
            
            List<SnmpSourceSplit> initialSplits = new ArrayList<>();
            
            for (int i = 0; i < snmpAgentInfoList.size(); i++) {
                SnmpAgentInfo agentInfo = snmpAgentInfoList.get(i);
                initialSplits.add(new SnmpSourceSplit(String.valueOf(i), agentInfo));
            }

            enumContext.assignSplits(
                SplitsAssignment.<SnmpSourceSplit>newBuilder()
                    .addUnassignedSplits(initialSplits)
                    .build()
            );

        } else {

            enumContext.assignSplits(
                SplitsAssignment.<SnmpSourceSplit>newBuilder()
                    .addUnassignedSplits(currentEnumeratorCheckpoint)
                    .build()
            );

        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

        LOG.debug("{} SnmpSourceEnumerator: Reader {} at {} requested splits. All initial splits assigned or restored.",
            Thread.currentThread().getName(),
            subtaskId,
            requesterHostname
        );
    }

    @Override
    public void addSplitsBack(List<SnmpSourceSplit> splits, int subtaskId) {

        LOG.debug("{} SnmpSourceEnumerator: Splits added back from subtask {}: {}.",
            Thread.currentThread().getName(),
            subtaskId,
            splits.size()
        );

        enumContext.assignSplits(
            SplitsAssignment.<SnmpSourceSplit>newBuilder()
                .addUnassignedSplits(splits)
                .build()
        );
    }

    @Override
    public void addReader(int subtaskId) {

        LOG.debug("{} SnmpSourceEnumerator: Reader {} added.",
            Thread.currentThread().getName(),
            subtaskId
        );
    }

    @Override
    public List<SnmpSourceSplit> snapshotState(long checkpointId) throws IOException {

        LOG.debug("{} SnmpSourceEnumerator: snapshotState() called for checkpointId {}.",
            Thread.currentThread().getName(),
            checkpointId
        );

        // System.out.println("SnmpSourceEnumerator: snapshotState() called for checkpointId: "
        //     + checkpointId
        //     + " (Direct System.out)"
        // );

        List<SnmpSourceSplit> splitsToSnapshot = new ArrayList<>();
        for (int i = 0; i < snmpAgentInfoList.size(); i++) {
             splitsToSnapshot.add(new SnmpSourceSplit(String.valueOf(i), snmpAgentInfoList.get(i)));
        }

        return splitsToSnapshot;
    }

    @Override
    public void close() throws IOException {

        LOG.debug("{} SnmpSourceEnumerator: close() called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSourceEnumerator: close() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );
    }
}