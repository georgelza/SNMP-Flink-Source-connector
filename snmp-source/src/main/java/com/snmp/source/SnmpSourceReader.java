/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpSourceReader.java
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

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.api.connector.source.ReaderOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.UserTarget;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.PrivAES128;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.TreeEvent;
import org.snmp4j.util.TreeUtils;
import org.snmp4j.util.DefaultPDUFactory;
import org.snmp4j.Target;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link SourceReader} that connects to SNMP agents and polls data.
 * Each reader handles one or more {@link SnmpSourceSplit}s.
 */
public class SnmpSourceReader implements SourceReader<RowData, SnmpSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSourceReader.class);

    private final SourceReaderContext readerContext;
    private final Queue<SnmpSourceSplit> splits;
    private final Queue<RowData> fetchedRecords;

    private Snmp currentSnmp;
    private SnmpAgentInfo currentAgentInfo;
    private long lastPollTime = 0;
    private final AtomicBoolean noMoreSplits;

    /**
     * Constructs a new SnmpSourceReader.
     *
     * @param readerContext The context for the source reader.
     */
    public SnmpSourceReader(SourceReaderContext readerContext) {
        this.readerContext  = readerContext;
        this.splits         = new LinkedList<>();
        this.fetchedRecords = new LinkedList<>();
        this.noMoreSplits   = new AtomicBoolean(false);

        LOG.info("SnmpSourceReader initialized.");
    }

    @Override
    public void start() {
        LOG.info("SNMP Source Reader started. Requesting initial splits from enumerator.");
        readerContext.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        LOG.debug("pollNext called. fetchedRecords size: {}, currentAgentInfo: {}, noMoreSplits: {}",
                fetchedRecords.size(), 
                currentAgentInfo != null ? currentAgentInfo.getHost() : "N/A", noMoreSplits.get()
        );

        if (!fetchedRecords.isEmpty()) {
            output.collect(fetchedRecords.poll());
            LOG.debug("Collected a record. Remaining in buffer: {}. Returning MORE_AVAILABLE.", 
                fetchedRecords.size()
            );
            return InputStatus.MORE_AVAILABLE;
        }

        // If there are no fetched records, check for more splits or end of input.
        if (splits.isEmpty()) {
            if (noMoreSplits.get()) {
                // If no more splits will be assigned AND no records are left, then we are at END_OF_INPUT.
                LOG.info("No more splits will be assigned, and no more records in buffer. Finishing input.");
                return InputStatus.END_OF_INPUT;
            } else {
                // No current split and no pending splits, so request more from coordinator.
                LOG.debug("No active split and no pending splits. Requesting more splits from coordinator. Returning NOTHING_AVAILABLE.");
                readerContext.sendSplitRequest();
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        // If there's an available split but no current agent, assign the next split.
        if (currentAgentInfo == null) {
            LOG.info("Assigning a new split as currentAgentInfo is null and splits are available.");
            assignNextSplit();
            lastPollTime = 0; // Reset last poll time for the new agent
            if (currentAgentInfo == null) {
                LOG.warn("Failed to assign a new split. No valid agent info. Returning NOTHING_AVAILABLE.");
                return InputStatus.NOTHING_AVAILABLE;
            }
            // After assigning a new split, we can try to poll immediately in the next cycle.
            LOG.debug("New split assigned. Will attempt immediate poll in next pollNext cycle. Returning MORE_AVAILABLE.");
            return InputStatus.MORE_AVAILABLE;
        }

        // If we have an active agent, check if it's time to poll.
        long currentTime = System.currentTimeMillis();
        long intervalMillis = currentAgentInfo.getIntervalSeconds() * 1000L;

        if (currentTime - lastPollTime >= intervalMillis) {
            LOG.info("Time to poll. Polling SNMP agent: {} (Poll Mode: {}). Current Time: {}, Last Poll Time: {}, Interval: {}ms",
                    currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort(),
                    currentAgentInfo.getPollMode(),
                    currentTime,
                    lastPollTime,
                    intervalMillis
            );
    
            try {
                pollSnmpAgent();
                lastPollTime = currentTime;
                if (!fetchedRecords.isEmpty()) {
                    LOG.debug("Poll successful, new records fetched. Returning MORE_AVAILABLE.");
                    return InputStatus.MORE_AVAILABLE;
               
                } else {
                    LOG.debug("Poll successful but no records fetched. Returning NOTHING_AVAILABLE, waiting for next poll interval or more splits.");
                    return InputStatus.NOTHING_AVAILABLE;
        
                }
            } catch (Exception e) {
                LOG.error("Error during SNMP GET to {}:{}: {}", 
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort(), 
                    e.getMessage(), 
                    e
                );
                return InputStatus.NOTHING_AVAILABLE;
            }
        } else {
            LOG.debug("Not yet time to poll agent {}:{}. Remaining until next poll: {}ms. Returning NOTHING_AVAILABLE.",
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort(), 
                    intervalMillis - (currentTime - lastPollTime)
            );
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (!fetchedRecords.isEmpty() || !splits.isEmpty() || (currentAgentInfo != null && !noMoreSplits.get())) {
            LOG.trace("isAvailable() called. Reader is ready to emit or process. Returning completedFuture.");
            return CompletableFuture.completedFuture(null);

        } else {
            LOG.trace("isAvailable() called. No data immediately available but not END_OF_INPUT. Returning completedFuture (waiting state).");
            return CompletableFuture.completedFuture(null);
        
        }
    }

    @Override
    public void addSplits(List<SnmpSourceSplit> newSplits) {
        LOG.info("Reader received {} new splits.", newSplits.size());
        for (SnmpSourceSplit split : newSplits) {
            if (!splits.contains(split)) { // Avoid adding duplicates if Flink sometimes resends
                splits.add(split);
                LOG.debug("Added split: {}", split.splitId());
            } else {
                LOG.debug("Split {} already exists, skipping.", split.splitId());
            }
        }
        LOG.info("Total splits in reader queue: {}. Requesting pollNext.", splits.size());
    }


    @Override
    public void notifyNoMoreSplits() {
        LOG.info("Enumerator notified this reader that no more splits will be assigned.");
        noMoreSplits.set(true);
    }

    @Override
    public List<SnmpSourceSplit> snapshotState(long checkpointId) {
        LOG.info("Snapshotting reader state for checkpoint {}. Current fetched records in buffer: {}. Current splits in queue: {}",
            checkpointId,
            fetchedRecords.size(),
            splits.size()
        );

        List<SnmpSourceSplit> state = new ArrayList<>();
        // Add all splits that are currently in the reader's queue and haven't been processed yet.
        state.addAll(splits);

        // If there's an actively assigned split, include it in the snapshot as well.
        // This ensures that if the reader fails mid-processing a split, it can resume.
        if (currentAgentInfo != null) {
            // Create a split representing the current agent, if it's not already in the queue.
            // A simple approach is to create a new split using the current agent info.
            SnmpSourceSplit currentSplitSnapshot = new SnmpSourceSplit(
                "retained-" + currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort() + "-" + checkpointId, // Unique ID for snapshot
                currentAgentInfo
            );
            // Only add if not already present to avoid duplicates if it was just added to the queue for some reason.
            if (!state.contains(currentSplitSnapshot)) {
                 state.add(currentSplitSnapshot);
                 LOG.debug("Snapshotting current active split: {}. Total state splits: {}", 
                    currentSplitSnapshot.splitId(), 
                    state.size()
                );
            }
        }
        LOG.info("Snapshotting state for checkpoint {}. Total splits in state: {}", 
            checkpointId, state.size()
        );
        return state;
    }


    private void assignNextSplit() {
        SnmpSourceSplit split = splits.poll();
        if (split != null) {
            currentAgentInfo = split.getAgentInfo();
            LOG.info("Assigned new split: {}. Agent: {}:{}", 
                split.splitId(), 
                currentAgentInfo.getHost(), 
                currentAgentInfo.getPort()
            );
            try {
                if (currentSnmp != null) {
                    LOG.debug("Closing previous SNMP session.");
                    currentSnmp.close();
                }
                currentSnmp = new Snmp(new DefaultUdpTransportMapping());
                if ("SNMPv3".equalsIgnoreCase(currentAgentInfo.getSnmpVersion()) &&
                    currentAgentInfo.getUsername() != null && currentAgentInfo.getPassword() != null) {
                    currentSnmp.getUSM().addUser(
                            new OctetString(currentAgentInfo.getUsername()),
                            new UsmUser(
                                    new OctetString(currentAgentInfo.getPassword()),
                                    AuthSHA.ID,
                                    new OctetString(currentAgentInfo.getPassword()),
                                    PrivAES128.ID,
                                    new OctetString(currentAgentInfo.getPassword())
                            ));
                    LOG.debug("Added SNMPv3 user for {}.", 
                        currentAgentInfo.getUsername()
                    );
                }
                currentSnmp.listen();
                LOG.info("SNMP session initialized for {}:{}", 
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort()
                );

            } catch (IOException e) {
                LOG.error("Failed to initialize SNMP session for {}:{}: {}. Marking agent as null.", 
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort(), 
                    e.getMessage(), 
                    e
                );
                currentAgentInfo = null;

            }
        } else {
            currentAgentInfo = null;
            LOG.info("No more splits to assign from the queue. currentAgentInfo set to null.");
        }
    }

    private void pollSnmpAgent() throws IOException {
        if (currentSnmp == null || currentAgentInfo == null) {
            LOG.warn("SNMP session not initialized or no agent info available when pollSnmpAgent was called. Cannot poll.");
            return;
        }

        LOG.debug("Starting SNMP poll for agent: {}:{} with version: {} and poll mode: {}",
                currentAgentInfo.getHost(), 
                currentAgentInfo.getPort(), 
                currentAgentInfo.getSnmpVersion(), 
                currentAgentInfo.getPollMode()
        );

        Target<UdpAddress> target;
        if ("SNMPv3".equalsIgnoreCase(currentAgentInfo.getSnmpVersion())) {
            UserTarget<UdpAddress> userTarget = new UserTarget<>(); 
            userTarget.setAddress(new UdpAddress(currentAgentInfo.getHost() + "/" + currentAgentInfo.getPort()));
            userTarget.setRetries(currentAgentInfo.getRetries());
            userTarget.setTimeout(currentAgentInfo.getTimeoutSeconds() * 1000L);
            userTarget.setSecurityLevel(SecurityLevel.AUTH_PRIV);
            userTarget.setSecurityName(new OctetString(currentAgentInfo.getUsername()));
            target = userTarget;
            LOG.debug("Configured SNMPv3 UserTarget for {}:{} with security level {}.",
                      currentAgentInfo.getHost(), 
                      currentAgentInfo.getPort(), 
                      userTarget.getSecurityLevel()
            );
        } else {
            CommunityTarget<UdpAddress> communityTarget = new CommunityTarget<>();
            communityTarget.setCommunity(new OctetString(currentAgentInfo.getCommunityString()));
            communityTarget.setAddress(new UdpAddress(currentAgentInfo.getHost() + "/" + currentAgentInfo.getPort()));
            communityTarget.setRetries(currentAgentInfo.getRetries());
            communityTarget.setTimeout(currentAgentInfo.getTimeoutSeconds() * 1000L);
            if ("SNMPv1".equalsIgnoreCase(currentAgentInfo.getSnmpVersion())) {
                communityTarget.setVersion(SnmpConstants.version1);
                LOG.debug("Configured SNMPv1 CommunityTarget for {}:{}", 
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort()
                );

            } else {
                communityTarget.setVersion(SnmpConstants.version2c);
                LOG.debug("Configured SNMPv2c CommunityTarget for {}:{}", 
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort()
                );
            }
            target = communityTarget;
        }

        if ("GET".equalsIgnoreCase(currentAgentInfo.getPollMode())) {
            PDU pdu = new PDU();
            for (String oid : currentAgentInfo.getOids()) {
                pdu.add(new VariableBinding(new OID(oid)));
                LOG.debug("Adding OID {} to GET PDU.", 
                    oid
                );
            }
            pdu.setType(PDU.GET);

            try {
                LOG.debug("Sending SNMP GET request to {}:{} for OIDs: {}",
                        currentAgentInfo.getHost(), 
                        currentAgentInfo.getPort(), 
                        currentAgentInfo.getOids()
                );

                ResponseEvent<UdpAddress> responseEvent = currentSnmp.send(pdu, target);
                if (responseEvent != null && responseEvent.getResponse() != null) {
                    PDU responsePDU = responseEvent.getResponse();
                    LOG.debug("Received GET response from {}:{}. Variable Bindings count: {}",
                            currentAgentInfo.getHost(), 
                            currentAgentInfo.getPort(), 
                            responsePDU.getVariableBindings().size()
                    );
                    for (VariableBinding vb : responsePDU.getVariableBindings()) {
                        processVariableBinding(vb);
                    }
                } else {
                    LOG.warn("GET request to {}:{} timed out or returned no response for OIDs: {}. ResponseEvent: {}, ResponsePDU: {}",
                            currentAgentInfo.getHost(), 
                            currentAgentInfo.getPort(), 
                            currentAgentInfo.getOids(), 
                            responseEvent, 
                            (responseEvent != null ? responseEvent.getResponse() : "null")
                    );
                }
            } catch (Exception e) {
                LOG.error("Error during SNMP GET to {}:{}: {}", 
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort(), 
                    e.getMessage(), 
                    e
                );
            }
        } else if ("WALK".equalsIgnoreCase(currentAgentInfo.getPollMode())) {
            if (currentAgentInfo.getOids().isEmpty()) {
                LOG.warn("No root OID specified for WALK mode for agent {}:{}. Skipping WALK.", 
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort()
                );
                return;
            }
            OID rootOid = new OID(currentAgentInfo.getOids().get(0));
            LOG.debug("Starting SNMP WALK request to {}:{} for root OID: {}",
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort(), 
                    rootOid.toDottedString()
            );

            TreeUtils treeUtils = new TreeUtils(currentSnmp, new DefaultPDUFactory());
            List<TreeEvent> treeEvents = treeUtils.walk(target, new OID[]{rootOid});
            LOG.debug("SNMP WALK finished for {}:{} on OID {}. Number of TreeEvents: {}",
                    currentAgentInfo.getHost(), 
                    currentAgentInfo.getPort(), 
                    rootOid, 
                    treeEvents.size()
            );

            int processedBindings = 0;
            for (TreeEvent event : treeEvents) {
                if (event == null) continue;
                if (event.getVariableBindings() != null) {
                    for (VariableBinding vb : event.getVariableBindings()) {
                        if (vb != null && vb.getOid().startsWith(rootOid)) {
                            processVariableBinding(vb);
                            processedBindings++;
                        }
                    }
                } else if (event.getException() != null) {
                    LOG.error("Error during SNMP WALK on {}:{} for OID {}: {}",
                            currentAgentInfo.getHost(), 
                            currentAgentInfo.getPort(), 
                            rootOid, 
                            event.getException().getMessage(), 
                            event.getException()
                    );
                }
            }
            LOG.debug("Total VariableBindings processed during WALK: {}", processedBindings);
        } else {
            LOG.error("Unsupported SNMP poll mode: {}. Skipping poll for agent {}:{}.",
                    currentAgentInfo.getPollMode(), 
                    currentAgentInfo.getPort(), 
                    currentAgentInfo.getSnmpVersion(), 
                    currentAgentInfo.getPollMode()
            );
        }
    }

    /**
     * Processes a VariableBinding and converts it into a SnmpData object, then adds it to fetchedRecords.
     */
    private void processVariableBinding(VariableBinding vb) {
        String fullOid = vb.getOid().toDottedString();
        String metricValue = vb.getVariable().toString();
        String dataType = vb.getVariable().getSyntaxString();

        String instanceIdentifier = "";
        if (fullOid.contains(".")) {
            if (!currentAgentInfo.getOids().isEmpty()) {
                OID baseOid = new OID(currentAgentInfo.getOids().get(0));
                if ("WALK".equalsIgnoreCase(currentAgentInfo.getPollMode())) {
                    if (vb.getOid().startsWith(baseOid) && vb.getOid().size() > baseOid.size()) {
                        StringBuilder sb = new StringBuilder();
                        for (int i = baseOid.size(); i < vb.getOid().size(); i++) {
                            sb.append(vb.getOid().get(i));
                            if (i < vb.getOid().size() - 1) {
                                sb.append(".");
                            }
                        }
                        instanceIdentifier = sb.toString();
                    }
                } else {
                    if (fullOid.endsWith(".0")) {
                         instanceIdentifier = "";
                    } else {
                        int lastDot = fullOid.lastIndexOf('.');
                        if (lastDot != -1) {
                            instanceIdentifier = fullOid.substring(lastDot + 1);
                        }
                    }
                }
            } else {
                LOG.warn("currentAgentInfo.getOids() is empty when trying to determine instanceIdentifier for OID: {}. No base OID for comparison.", 
                    fullOid
                );
            }
        }
        
        LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneOffset.UTC);

        SnmpData snmpData = new SnmpData(
                currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort(),
                fullOid,
                metricValue,
                dataType,
                instanceIdentifier,
                now
        );
        fetchedRecords.add(convertToRowData(snmpData));
        LOG.debug("Processed VB: OID={}, Value={}, DataType={}, InstanceIdentifier={}. Added to fetchedRecords. Current size: {}",
                fullOid, 
                metricValue, 
                dataType, 
                instanceIdentifier, 
                fetchedRecords.size()
        );
    }

    /**
     * Converts an {@link SnmpData} object to a Flink {@link RowData}.
     * The order of fields must match the `CREATE TABLE` definition:
     * device_id, metric_oid, metric_value, data_type, instance_identifier, ts, PROC_TIME
     */
    private RowData convertToRowData(SnmpData snmpData) {
        GenericRowData row = new GenericRowData(6);
        row.setField(0, StringData.fromString(snmpData.getDeviceId()));
        row.setField(1, StringData.fromString(snmpData.getMetricOid()));
        row.setField(2, StringData.fromString(snmpData.getMetricValue()));
        row.setField(3, StringData.fromString(snmpData.getDataType()));
        row.setField(4, snmpData.getInstanceIdentifier() != null ? StringData.fromString(snmpData.getInstanceIdentifier()) : StringData.fromString(""));
        row.setField(5, TimestampData.fromLocalDateTime(snmpData.getTs()));
        LOG.trace("Converted SnmpData to RowData: DeviceId={}, OID={}, Value={}", 
            snmpData.getDeviceId(), 
            snmpData.getMetricOid(), 
            snmpData.getMetricValue()
        );
        return row;
    }

    @Override
    public void close() throws Exception {
        LOG.info("SNMP Source Reader closing.");
        if (currentSnmp != null) {
            currentSnmp.close();
            LOG.debug("SNMP session closed.");
            currentSnmp = null;
        }
        fetchedRecords.clear();
        splits.clear();
        currentAgentInfo = null;
        LOG.info("SNMP Source Reader resources cleared.");
    }

    // This method is not part of the SourceReader interface, it's an internal helper
    // for restoring the splits queue from a snapshot. The actual snapshotState and
    // addSplits methods are defined in the SourceReader interface.
    // Therefore, the @Override annotation was correctly removed in the previous step.
    public void restoreState(List<SnmpSourceSplit> state) {
        LOG.info("Restoring state with {} splits. Clearing current splits and fetched records.", 
            state.size()
        );

        this.splits.clear();
        this.fetchedRecords.clear();
        this.splits.addAll(state);
        LOG.info("State restored. Total pending splits after restore: {}. Attempting to re-assign if necessary.", 
            this.splits.size()
        );

        if (!this.splits.isEmpty() && currentAgentInfo == null) {
            LOG.info("Assigning a split immediately after state restore as currentAgentInfo is null.");
            assignNextSplit();
            lastPollTime = 0;
        }
    }

}