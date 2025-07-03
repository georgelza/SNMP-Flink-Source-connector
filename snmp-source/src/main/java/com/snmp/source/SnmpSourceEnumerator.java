/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpSourceEnumerator.java
/
/       Description     :   SNMP Source connector
/
/       Created     	:   June 2025
/
/       copyright       :   Copyright 2025, - G Leonard, georgelza@gmail.com
/
/       GIT Repo        :   https://github.com/georgelza/SNMP-Flink-Source-connector
/
/       Blog            :\\\
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.SourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * The {@link SnmpSourceEnumerator} is responsible for coordinating the splits for the SNMP source.
 * It discovers SNMP agents (splits) and assigns them to {@link SnmpSourceReader}s.
 */
public class SnmpSourceEnumerator implements SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSourceEnumerator.class);

    private final SplitEnumeratorContext<SnmpSourceSplit>   context;
    private final Queue<SnmpSourceSplit>                    pendingSplits;
    private final Map<Integer, List<SnmpSourceSplit>>       assignedSplits;
    private final List<SnmpAgentInfo>                       snmpAgentInfoList;          // A list to handle the potential multiple snmp agents

    /**
     * Constructor for initial setup of the enumerator.
     * Added 'isInitial' boolean to differentiate from the restore constructor due to type erasure.
     *
     * @param context           The context for the split enumerator.
     * @param snmpAgentInfoList The initial list of SNMP agents to discover splits from.
     * @param isInitial         A dummy boolean to resolve constructor ambiguity.
     */
    public SnmpSourceEnumerator(SplitEnumeratorContext<SnmpSourceSplit> context, List<SnmpAgentInfo> snmpAgentInfoList, boolean isInitial) {
        this.context            = context;
        this.snmpAgentInfoList  = snmpAgentInfoList;
        this.pendingSplits      = new ConcurrentLinkedQueue<>();
        this.assignedSplits     = new HashMap<>();
        
        // Initially, all agents become splits. This might be where you discover new agents too.
        snmpAgentInfoList.forEach(agentInfo -> pendingSplits.add(new SnmpSourceSplit(agentInfo.getHost() + ":" + agentInfo.getPort(), agentInfo)));

        LOG.debug("{} SnmpSourceEnumerator: Initialized with {} agents, creating {} splits.",
            Thread.currentThread().getName(),
            snmpAgentInfoList.size(),
            pendingSplits.size()
        );

        System.out.println("SnmpSourceEnumerator:"
            + " Initialized with:"  + snmpAgentInfoList.size()
            + " agents, creating"   + pendingSplits.size()
            + " splits."
            + " for Thread: "       + Thread.currentThread().getName() 
            + " (Direct System.out)"
        );
    }

    /**
     * Constructor for restoring the enumerator from a checkpointed state.
     *
     * @param context           The context for the split enumerator.
     * @param checkpointedState The list of splits saved during the last checkpoint.
     */
    public SnmpSourceEnumerator(SplitEnumeratorContext<SnmpSourceSplit> context, List<SnmpSourceSplit> checkpointedState) {

        this.context        = context;
        this.pendingSplits  = new ConcurrentLinkedQueue<>(checkpointedState);
        this.assignedSplits = new HashMap<>();
        
        // Reconstruct snmpAgentInfoList from checkpointedState.
        this.snmpAgentInfoList = checkpointedState.stream()
                                    .map(SnmpSourceSplit::getAgentInfo)
                                    .distinct()
                                    .collect(Collectors.toList());

        LOG.info("{} SnmpSourceEnumerator: Restored from checkpoint with {} splits.",
            Thread.currentThread().getName(),
            checkpointedState.size()
        );
    }

    @Override
    public void start() {
        LOG.debug("{} SnmpSourceEnumerator: start() called.",
            Thread.currentThread().getName()
        );

        System.out.println("SnmpSourceEnumerator: start() called"
            + " for Thread: " + Thread.currentThread().getName() 
            + " (Direct System.out)"
        );
    
    }

    @Override
    public void handleSplitRequest(int subtaskID, @Nullable String requesterHostname) {

        LOG.debug("{} SnmpSourceEnumerator: handleSplitRequest() from subtask {} (hostname: {}).",
            Thread.currentThread().getName(),
            subtaskID,
            requesterHostname
        );

        // Assign a split if available
        SnmpSourceSplit split = pendingSplits.poll();

        if (split != null) {

            // Create an ArrayList for the splits to assign
            List<SnmpSourceSplit> splitsForAssignment = new ArrayList<>();
            splitsForAssignment.add(split);

            // Corrected: Use the constructor that takes a Map<Integer, Collection<SplitT>>
            context.assignSplits(new SplitsAssignment<SnmpSourceSplit>(Collections.singletonMap(subtaskID, splitsForAssignment)));
            
            assignedSplits.computeIfAbsent(subtaskID, k -> new ArrayList<>()).add(split);

            LOG.debug("{} SnmpSourceEnumerator: Assigned split '{}' to reader {}. Remaining pending splits: {}.",
                Thread.currentThread().getName(),
                split.splitId(),
                subtaskID,
                pendingSplits.size()
            );

        } else {
            LOG.debug("{} SnmpSourceEnumerator: No pending splits to assign to reader {}.",
                Thread.currentThread().getName(),
                subtaskID
            );
        }
    }

    @Override
    public void handleSourceEvent(int subtaskID, SourceEvent sourceEvent) {

        LOG.debug("{} SnmpSourceEnumerator: handleSourceEvent() from subtask {} with event {}.",
            Thread.currentThread().getName(),
            subtaskID,
            sourceEvent
        );
        // Handle custom source events from readers if any.
    }

    @Override
    public void addSplitsBack(List<SnmpSourceSplit> splits, int subtaskID) {

        LOG.info("{} SnmpSourceEnumerator: addSplitsBack() called for subtask {}. Adding {} splits back to pending.",
            Thread.currentThread().getName(),
            subtaskID,
            splits.size()
        );

        pendingSplits.addAll(splits);

        // Also remove them from assignedSplits map for the given subtask
        if (assignedSplits.containsKey(subtaskID)) {
            assignedSplits.get(subtaskID).removeAll(splits);
        }
    }

    @Override
    public void addReader(int subtaskID) {

        LOG.debug("{} SnmpSourceEnumerator: addReader() called for subtask {}. (Thread: {})",
            Thread.currentThread().getName(),
            subtaskID,
            Thread.currentThread().getName()
        );
        // When a new reader registers, we can try to assign it a split immediately.
        handleSplitRequest(subtaskID, null);
    }

    @Override
    public List<SnmpSourceSplit> snapshotState(long checkpointId) throws IOException {

        LOG.info("{} SnmpSourceEnumerator: snapshotState() called for checkpointId {}. Total pending splits: {}. Total assigned splits: {}.",
            Thread.currentThread().getName(),
            checkpointId,
            pendingSplits.size(),
            assignedSplits.values().stream().mapToInt(Collection::size).sum()
        );

        List<SnmpSourceSplit> state = new ArrayList<>();
        state.addAll(pendingSplits);
        assignedSplits.values().forEach(state::addAll);
        
        return state;
    }

    @Override
    public void close() throws IOException {
        
        LOG.debug("{} SnmpSourceEnumerator: close() called.",
            Thread.currentThread().getName()
        );

        pendingSplits.clear();
        assignedSplits.clear();
    }
}