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
/       GIT Repo        :   
/
/       Blog            :   
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class SnmpSourceSplitEnumerator implements SplitEnumerator<SnmpSourceSplit, List<SnmpSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSourceSplitEnumerator.class);

    private final List<SnmpAgentInfo> agentInfos;
    private final SplitEnumeratorContext<SnmpSourceSplit> context;

    // State to manage:
    // A map from subtask index to the splits currently assigned to it
    private final Map<Integer, List<SnmpSourceSplit>> assignments;
    // Splits that are discovered but not yet assigned
    private final Map<Integer, List<SnmpSourceSplit>> pendingSplits; // Using subtask 0 for simplicity, could be a single list
    // Keep track of assigned split IDs to avoid re-assigning if needed
    private final Set<String> assignedSplitIds;

    private ScheduledExecutorService discoveryScheduler;


    public SnmpSourceSplitEnumerator(
            List<SnmpAgentInfo> agentInfos,
            SplitEnumeratorContext<SnmpSourceSplit> context) {
        this(agentInfos, context, null); // Call the new main constructor
    }

    // New or modified constructor to handle restored state directly
    public SnmpSourceSplitEnumerator(
            List<SnmpAgentInfo> agentInfos,
            SplitEnumeratorContext<SnmpSourceSplit> context,
            Collection<SnmpSourceSplit> restoredState) { // Use Collection here
        this.agentInfos = agentInfos;
        this.context = context;
        this.assignments = new ConcurrentHashMap<>();
        this.pendingSplits = new ConcurrentHashMap<>();
        this.assignedSplitIds = new HashSet<>();

        if (restoredState != null && !restoredState.isEmpty()) {
            LOG.info("Initializing enumerator with {} splits from restored state.", restoredState.size());
            // Directly add to pending splits and assigned IDs
            this.pendingSplits.computeIfAbsent(0, k -> new ArrayList<>()).addAll(restoredState);
            restoredState.forEach(split -> assignedSplitIds.add(split.splitId()));
        } else {
            LOG.info("No restored state provided for enumerator initialization.");
        }
    }


    @Override
    public void start() {
        LOG.info("SnmpSourceSplitEnumerator starting.");

        // If no state was restored via the constructor, discover initial splits
        if (pendingSplits.isEmpty()) {
            LOG.info("No splits restored via constructor. Discovering initial splits.");
            discoverInitialSplits();
        } else {
            LOG.info("Splits already initialized via restored state.");
        }

        // Start split discovery periodically
        discoveryScheduler = Executors.newSingleThreadScheduledExecutor();
        discoveryScheduler.scheduleAtFixedRate(
            this::discoverAndAssignSplits,
            0, // initial delay
            60, // polling interval
            TimeUnit.SECONDS
        );
    }

    private void discoverInitialSplits() {
        // Implement initial split discovery based on agentInfos
        // This is called when the job starts for the first time.
        LOG.info("Discovering initial splits for {} agents.", agentInfos.size());
        List<SnmpSourceSplit> initialSplits = new ArrayList<>();
        for (SnmpAgentInfo agent : agentInfos) {
            // Create a split for each agent, or more granular splits based on your design
            SnmpSourceSplit newSplit = new SnmpSourceSplit(
                agent.getHost() + "_" + System.currentTimeMillis(), // Unique split ID
                agent // The agent info for this split
            );
            initialSplits.add(newSplit);
            assignedSplitIds.add(newSplit.splitId()); // Mark as discovered/initial assigned
        }

        if (!initialSplits.isEmpty()) {
            pendingSplits.computeIfAbsent(0, k -> new ArrayList<>()).addAll(initialSplits);
            LOG.info("Discovered {} initial splits.", initialSplits.size());
        } else {
            LOG.warn("No initial splits discovered from provided agent infos.");
        }
    }

    private void discoverAndAssignSplits() {
        // This method is called periodically by the scheduler.
        // It should:
        // 1. Discover new splits (if your source can dynamically add agents/data)
        // 2. Assign existing pending splits to available readers.

        LOG.info("Running scheduled split discovery and assignment.");

        // --- 1. Discover New Splits (if applicable) ---
        // For SNMP, this might involve re-scanning a config or a service discovery mechanism
        // to find new agents.
        // Example: If agentInfos can change or you want to auto-discover new agents.
        // List<SnmpAgentInfo> newlyDiscoveredAgents = findNewAgents();
        // for (SnmpAgentInfo newAgent : newlyDiscoveredAgents) {
        //    SnmpSourceSplit newSplit = new SnmpSourceSplit(newAgent.getHost() + "_" + UUID.randomUUID(), newAgent);
        //    if (assignedSplitIds.add(newSplit.splitId())) { // Only add if not already known
        //        pendingSplits.computeIfAbsent(0, k -> new ArrayList<>()).add(newSplit);
        //        LOG.info("Discovered new split: {}", newSplit.splitId());
        //    }
        // }


        // --- 2. Assign Pending Splits ---
        assignSplits();
    }


    @Override
    public void close() { // MODIFIED: Removed 'throws Exception'
        if (discoveryScheduler != null) {
            discoveryScheduler.shutdown();
            try { // MODIFIED: Added try-catch block for InterruptedException
                if (!discoveryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn("Split discovery scheduler did not terminate in time.");
                    discoveryScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted while waiting for split discovery scheduler to terminate.", e);
                discoveryScheduler.shutdownNow();
                Thread.currentThread().interrupt(); // Preserve interrupt status
            }
        }
        LOG.info("SnmpSourceSplitEnumerator closed.");
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Reader {} added. Attempting to assign splits.", subtaskId);
        // Track the active readers
        assignments.putIfAbsent(subtaskId, new ArrayList<>());
        assignSplits(); // Try to assign splits when a new reader connects
    }

    @Override
    public void handleSplitRequest(int subtaskId, String hostname) {
        LOG.info("Reader {} at {} requested splits. Attempting to assign splits.", subtaskId, hostname);
        // The 'hostname' parameter can be used for location-aware split assignment if needed.
        // For now, we'll proceed with general assignment.
        assignSplits(); // Assign splits when a reader explicitly asks for more
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent event) {
        // Handle custom source events if you define any
        LOG.debug("Received source event from subtask {}: {}", subtaskId, event);
    }

    @Override
    public void addSplitsBack(List<SnmpSourceSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}. Re-adding to pending splits.", splits.size(), subtaskId);
        // When a reader fails or is shut down, Flink returns its assigned splits here.
        // Re-add them to the pending splits queue for re-assignment.
        pendingSplits.computeIfAbsent(0, k -> new ArrayList<>()).addAll(splits);
        assignSplits(); // Try to re-assign immediately
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        LOG.info("Checkpoint {} completed.", checkpointId);
        // You can use this to clean up state related to previous checkpoints if needed.
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        LOG.warn("Checkpoint {} aborted.", checkpointId);
    }

    @Override
    public List<SnmpSourceSplit> snapshotState(long checkpointId) throws Exception {
        LOG.info("Snapshotting enumerator state for checkpoint {}. Current pending splits: {}. Current assigned splits: {}",
            checkpointId,
            pendingSplits.getOrDefault(0, new ArrayList<>()).size(),
            assignments.values().stream().mapToLong(List::size).sum()
        );

        // This method should return ALL splits that are currently managed by the enumerator
        // and need to be restored. This includes:
        // 1. All pending splits
        // 2. All splits currently assigned to readers (as they might fail and need to be re-assigned)
        List<SnmpSourceSplit> stateToSnapshot = new ArrayList<>();
        stateToSnapshot.addAll(pendingSplits.getOrDefault(0, new ArrayList<>())); // Add all pending
        // Add all splits that are currently assigned to readers
        assignments.values().forEach(stateToSnapshot::addAll);

        LOG.info("Snapshotting {} splits for checkpoint {}.", stateToSnapshot.size(), checkpointId);
        return stateToSnapshot;
    }

    // Helper method to assign splits to available readers
    private void assignSplits() {
        if (pendingSplits.getOrDefault(0, new ArrayList<>()).isEmpty()) {
            LOG.debug("No pending splits to assign.");
            return;
        }

        // Use registeredReaders() for compatibility with newer Flink versions
        Set<Integer> availableReaders = context.registeredReaders().keySet();
        if (availableReaders.isEmpty()) {
            LOG.warn("No readers registered to assign splits to.");
            return;
        }

        // Simple round-robin assignment strategy
        List<SnmpSourceSplit> splitsToAssign = pendingSplits.get(0);
        if (splitsToAssign == null || splitsToAssign.isEmpty()) {
            return;
        }

        Map<Integer, List<SnmpSourceSplit>> newAssignments = new HashMap<>();
        int readerIndex = 0;
        int numReaders = availableReaders.size();
        List<Integer> readerIds = new ArrayList<>(availableReaders);

        List<SnmpSourceSplit> toRemoveFromPending = new ArrayList<>();
        for (SnmpSourceSplit split : new ArrayList<>(splitsToAssign)) { // Iterate on a copy to allow modification
            int targetReaderId = readerIds.get(readerIndex % numReaders);
            newAssignments.computeIfAbsent(targetReaderId, k -> new ArrayList<>()).add(split);
            assignments.computeIfAbsent(targetReaderId, k -> new ArrayList<>()).add(split); // Update enumerator's state
            toRemoveFromPending.add(split); // Mark for removal from pending
            readerIndex++;
        }
        splitsToAssign.removeAll(toRemoveFromPending); // Remove assigned splits from pending

        if (!newAssignments.isEmpty()) {
            LOG.info("Assigning {} splits to readers: {}", newAssignments.values().stream().mapToLong(List::size).sum(), newAssignments.keySet());
            context.assignSplits(new SplitsAssignment<>(newAssignments));
        }
    }
}