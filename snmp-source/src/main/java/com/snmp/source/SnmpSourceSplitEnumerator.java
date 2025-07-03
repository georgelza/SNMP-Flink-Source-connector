/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/ 
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpSourceSplitEnumerator.java
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

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Callable;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SnmpSourceSplitEnumerator} is responsible for discovering SNMP agents
 * (which are treated as splits) and assigning them to {@link SnmpSourceReader}s.
 */
public class SnmpSourceSplitEnumerator implements SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSourceSplitEnumerator.class);

    private final SplitEnumeratorContext<SnmpSourceSplit> context;
    private final List<SnmpAgentInfo> snmpAgentInfoList;
    private final Queue<SnmpSourceSplit> remainingSplits;
    private final Set<SnmpSourceSplit> assignedSplits;


    /**
     * Constructor for SnmpSourceSplitEnumerator (fresh start).
     *
     * @param context The context for the split enumerator, allowing interaction with Flink.
     * @param snmpAgentInfoList A list of {@link SnmpAgentInfo} objects, each representing an SNMP agent
     * that needs to be polled. These will be converted into splits.
     */
    public SnmpSourceSplitEnumerator(
            SplitEnumeratorContext<SnmpSourceSplit> context,
            List<SnmpAgentInfo> snmpAgentInfoList) {

        this.context            = context;
        this.snmpAgentInfoList  = snmpAgentInfoList;
        this.remainingSplits    = new ConcurrentLinkedQueue<>();
        this.assignedSplits     = new HashSet<>();
        
        LOG.debug("{} SnmpSourceSplitEnumerator: Constructor called (fresh start). Number of agents: {}.",
            Thread.currentThread().getName(),
            snmpAgentInfoList.size()
        );
    }

    /**
     * Constructor for SnmpSourceSplitEnumerator (restoring from checkpoint).
     *
     * @param context The context for the split enumerator.
     * @param snmpAgentInfoList The initial configuration list of SNMP agents.
     * @param restoredSplits The list of splits restored from a checkpoint.
     */
    public SnmpSourceSplitEnumerator(
            SplitEnumeratorContext<SnmpSourceSplit> context,
            List<SnmpAgentInfo> snmpAgentInfoList,
            List<SnmpSourceSplit> restoredSplits) {

        // Call the primary constructor for common initialization
        this(context, snmpAgentInfoList); 
        
        LOG.debug("{} SnmpSourceSplitEnumerator: Constructor called (restoring state). Restoring {} splits.",
            Thread.currentThread().getName(),
            restoredSplits.size()
        );

        // Add the restored splits to the remaining splits queue
        this.remainingSplits.addAll(restoredSplits);
    }


    @Override
    public void start() {
        LOG.debug("{} SnmpSourceSplitEnumerator: start() called.", 
            Thread.currentThread().getName()
        );

        // If starting fresh, convert all configured agents into splits.
        // If restoring, 'remainingSplits' is already populated by the restoring constructor.
        if (remainingSplits.isEmpty() && !snmpAgentInfoList.isEmpty()) {
            for (SnmpAgentInfo agentInfo : snmpAgentInfoList) {
                String splitId = agentInfo.getHost() + ":" + agentInfo.getPort();
                remainingSplits.offer(new SnmpSourceSplit(splitId, agentInfo));

                LOG.debug("{} SnmpSourceSplitEnumerator: Created split {}.", 
                    Thread.currentThread().getName(),
                    splitId
                );
            }
        }

        context.callAsync(
            (Callable<Void>) this::discoverUnassignedSplits,
            (Void result, Throwable error) -> {
                if (error != null) {
                    LOG.error("{} SnmpSourceSplitEnumerator: Error during async split discovery: {} {}", 
                        Thread.currentThread().getName(), 
                        error.getMessage(), 
                        error
                    );
                
                } else {
                    LOG.debug("{} SnmpSourceSplitEnumerator: Async split discovery complete.", 
                        Thread.currentThread().getName()
                    );
                }
            }
        );
    }

    private Void discoverUnassignedSplits() throws Exception {
        LOG.debug("{} SnmpSourceSplitEnumerator: discoverUnassignedSplits() called.", 
            Thread.currentThread().getName()
        );

        Set<Integer> readers = context.registeredReaders().keySet();

        if (readers.isEmpty()) {
            LOG.warn("{} SnmpSourceSplitEnumerator: No readers registered yet. Waiting for readers to connect.", 
                Thread.currentThread().getName()
            );
            return null;
        }

        int readerCount = readers.size();
        // A simple round-robin assignment for initial splits
        int currentReaderIndex = 0;
        
        // Use a temporary list to avoid ConcurrentModificationException if remainingSplits is modified
        // during iteration (e.g., if another thread adds splits back).
        List<SnmpSourceSplit> splitsToAssign = new ArrayList<>();
        // Manually drain the queue to the list
        while (!remainingSplits.isEmpty()) {
            splitsToAssign.add(remainingSplits.poll());
        }

        Map<Integer, List<SnmpSourceSplit>> assignments = new HashMap<>();
        for (SnmpSourceSplit split : splitsToAssign) {
            int targetReader = (currentReaderIndex % readerCount);
            // Add split to the list for the targetReader
            assignments.computeIfAbsent(targetReader, k -> new ArrayList<>()).add(split);
            assignedSplits.add(split); // Keep track of assigned splits

            LOG.info("{} SnmpSourceSplitEnumerator: Assigned split {} to reader {}.", 
                Thread.currentThread().getName(),
                split.splitId(), 
                targetReader
            );

            currentReaderIndex++;
        }
        if (!assignments.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(assignments));
        }
        
        if (remainingSplits.isEmpty() && splitsToAssign.isEmpty()) {
             for (int subtaskID : readers) {
                 context.signalNoMoreSplits(subtaskID);
             }
        }

        return null;
    }


    @Override
    public void handleSplitRequest(int subtaskID, String requesterHostname) {
        LOG.debug("{} SnmpSourceSplitEnumerator: handleSplitRequest() from subtask {} on {}.", 
            Thread.currentThread().getName(),
            subtaskID, 
            requesterHostname
        );

        SnmpSourceSplit split = remainingSplits.poll();
        if (split != null) {

            Map<Integer, List<SnmpSourceSplit>> assignments = new HashMap<>();
            assignments.put(subtaskID, Collections.singletonList(split));
            context.assignSplits(new SplitsAssignment<>(assignments));
            assignedSplits.add(split);

            LOG.info("{} SnmpSourceSplitEnumerator: Assigned requested split {} to subtask {}.", 
                Thread.currentThread().getName(),
                split.splitId(), 
                subtaskID
            );

        } else {
            LOG.warn("{} SnmpSourceSplitEnumerator: No more splits available for subtask {}.", 
                Thread.currentThread().getName(),
                subtaskID
            );
            context.signalNoMoreSplits(subtaskID);

        }
    }

    @Override
    public void addSplitsBack(List<SnmpSourceSplit> splits, int subtaskID) {
        LOG.warn("{} SnmpSourceSplitEnumerator: Add splits back from subtask {}. Splits: {}", 
            Thread.currentThread().getName(), 
            subtaskID, 
            splits.size()
        );

        this.remainingSplits.addAll(splits);
        this.assignedSplits.removeAll(splits);

        context.callAsync(
            (Callable<Void>) this::discoverUnassignedSplits,
            (Void result, Throwable error) -> {
                if (error != null) {
                    LOG.error("{} SnmpSourceSplitEnumerator: Error during re-assignment after addSplitsBack: {} {}", 
                        Thread.currentThread().getName(), 
                        error.getMessage(), 
                        error
                    );

                } else {
                    LOG.debug("{} SnmpSourceSplitEnumerator: Re-assignment after addSplitsBack complete.", 
                        Thread.currentThread().getName());

                }
            }
        );
    }

    @Override
    public void addReader(int subtaskID) {
        LOG.debug("{} SnmpSourceSplitEnumerator: Reader {} registered.", 
            Thread.currentThread().getName(),
            subtaskID
        );

        context.callAsync(
            (Callable<Void>) this::discoverUnassignedSplits,
            (Void result, Throwable error) -> {
                if (error != null) {
                    LOG.error("{} SnmpSourceSplitEnumerator: Error during assignment to new reader {}: {} {}", 
                        Thread.currentThread().getName(), 
                        subtaskID, 
                        error.getMessage(), 
                        error
                    );
               
                } else {
                    LOG.debug("{} SnmpSourceSplitEnumerator: Assignment to new reader {} complete.", 
                        Thread.currentThread().getName(),
                        subtaskID
                    );
                }
            }
        );
    }

    @Override
    public void close() {
        LOG.debug("{} SnmpSourceSplitEnumerator: close() called.", 
            Thread.currentThread().getName()
        );

        remainingSplits.clear();
        assignedSplits.clear();
    }

    @Override
    public List<SnmpSourceSplit> snapshotState(long checkpointId) throws Exception {

        LOG.debug("{} SnmpSourceSplitEnumerator: snapshotState() for checkpointId {}. Remaining splits: {}. Assigned splits: {}",
            Thread.currentThread().getName(),
            checkpointId,
            remainingSplits.size(),
            assignedSplits.size()
        );

        List<SnmpSourceSplit> state = new ArrayList<>();
        state.addAll(remainingSplits);
        state.addAll(assignedSplits);

        return state;
    }
}