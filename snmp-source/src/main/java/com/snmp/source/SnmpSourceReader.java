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
/       GIT Repo        :   https://github.com/georgelza/SNMP-Flink-Source-connector
/
/       Blog            :
/
/
/    In your Flink SNMP Source connector, the primary function responsible for retrieving values from the SNMP agent is:
/
/    SnmpSourceReader.pollSnmpAgent()
/
/    This method initiates the SNMP communication (either GET or WALK) using the snmp4j library. Inside pollSnmpAgent(), for each variable binding (OID-value pair) received in the SNMP response, it calls:
/
/    SnmpSourceReader.processVariableBinding(VariableBinding vb)
/
/    The processVariableBinding function then extracts the actual value using vb.getVariable().toString() and constructs a SnmpData object with it.
/
/    So, to summarize:
/    pollSnmpAgent(): Orchestrates the SNMP request (either GET or WALK).
/    processVariableBinding(): Extracts the OID and its corresponding value from the SNMP response and formats it into a SnmpData object.
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.core.io.InputStatus;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.TransportMapping;
import org.snmp4j.UserTarget;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.PrivDES;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.smi.Null;
import org.snmp4j.transport.DefaultUdpTransportMapping;



/**
 * A {@link SourceReader} for the SNMP source. This reader polls the assigned
 * SNMP agent (split) and emits {@link RowData} records.
 */
public class SnmpSourceReader implements SourceReader<RowData, SnmpSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSourceReader.class);

    private final RowType rowType;
    private final Collector<RowData> collector;
    private final Deque<SnmpSourceSplit> splits;
    private final Queue<RowData> fetchedRecords;
    private final AtomicBoolean noMoreSplits;
    private final CompletableFuture<Void> availabilityFuture;   // Signifies data availability

    private SourceReaderContext readerContext;
    private RowType producedRowType; 
    private SnmpAgentInfo currentAgentInfo;                     // The SNMP agent this reader is currently polling
    private Snmp snmp;
    private long lastPollTime;                                  // Timestamp of the last successful poll
    private OID lastWalkedOid;                                  // For SNMP WALK to track progress
    private AtomicBoolean isPolling;

    public SnmpSourceReader(SourceReaderContext readerContext, DataType producedDataType) {

        LOG.debug("{} SnmpSourceReader: Constructor called with readerContext and producedDataType.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSourceReader: Constructor called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        this.readerContext      = readerContext;
        this.rowType            = (RowType) producedDataType.getLogicalType();      // Cast DataType to LogicalType.RowType
        this.fetchedRecords     = new ConcurrentLinkedQueue<>();
        this.splits             = new ConcurrentLinkedDeque<>();
        this.noMoreSplits       = new AtomicBoolean(false);
        this.availabilityFuture = new CompletableFuture<>();
        this.collector          = new RecordCollector();
        this.isPolling          = new AtomicBoolean(false);

        LOG.debug("{} SnmpSourceReader: Initialized with producedRowType: {}.",
            Thread.currentThread().getName(),
            this.producedRowType
        );

        // System.out.println("SnmpSourceReader: Initializing for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );
    }

    @Override
    public void start() {

        LOG.debug("{} SnmpSourceReader: start() called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSourceReader: start() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        // Signal availability initially if there are splits already assigned from a restore
        if (!splits.isEmpty()) {
            availabilityFuture.complete(null);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {

        LOG.debug("{} SnmpSourceReader: pollNext() called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSourceReader: pollNext() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        // If there are already fetched records, emit them first
        RowData record;
        while ((record = fetchedRecords.poll()) != null) {
            output.collect(record);
            return InputStatus.MORE_AVAILABLE;
        }

        // Check if a split is assigned and it's time to poll
        if (currentAgentInfo == null && !splits.isEmpty()) {

            LOG.debug("{} SnmpSourceReader: No current agent, assigning next split.",
                Thread.currentThread().getName()
            );

            assignNextSplit();
            lastPollTime = 0; // Reset last poll time to trigger immediate poll
        }

        if (currentAgentInfo != null) {
            long currentTime = System.currentTimeMillis();
            long intervalMillis = currentAgentInfo.getIntervalSeconds() * 1000L;

            if (currentTime - lastPollTime >= intervalMillis) {
                LOG.debug("{} SnmpSourceReader: Polling SNMP agent {}:{} (Mode: {}).",
                    Thread.currentThread().getName(),
                    currentAgentInfo.getHost(),
                    currentAgentInfo.getPort(),
                    currentAgentInfo.getPollMode()
                );

                try {
                    pollSnmpAgent(currentAgentInfo);
                    lastPollTime = currentTime; // Update last poll time only on success
                } catch (IOException e) {
                    LOG.error("{} SnmpSourceReader: Error polling SNMP agent {}:{}: {}",
                        Thread.currentThread().getName(),
                        currentAgentInfo.getHost(),
                        currentAgentInfo.getPort(),
                        e.getMessage(),
                        e
                    );
                    // Depending on error handling strategy, you might want to:
                    // 1. Mark split as failed and request a new one (for transient errors)
                    // 2. Fail the reader (for persistent configuration errors)
                    // For now, we just log and continue, waiting for the next poll interval.
                }
            }
        }

        // If no records were fetched and no more splits are expected, signal end of input
        if (fetchedRecords.isEmpty() && noMoreSplits.get() && splits.isEmpty() && currentAgentInfo == null) {

            LOG.debug("{} SnmpSourceReader: No more records, no more splits, signaling END_OF_INPUT.",
                Thread.currentThread().getName()
            );

            return InputStatus.END_OF_INPUT;
        }

        // If no records were available immediately, return PENDING and complete the future when data is available
        return InputStatus.NOTHING_AVAILABLE;
    }

    private void assignNextSplit() {

        if (splits.isEmpty()) {
            currentAgentInfo = null;
            lastWalkedOid = null; // Clear last walked OID when no agent is assigned
            return;
        }

        SnmpSourceSplit split   = splits.poll();
        currentAgentInfo        = split.getAgentInfo();
        lastWalkedOid           = null; // Reset for each new split/agent

        LOG.debug("{} SnmpSourceReader: Assigned new split for agent {}:{}",
            Thread.currentThread().getName(),
            currentAgentInfo.getHost(),
            currentAgentInfo.getPort()
        );
    }

    private void pollSnmpAgent(SnmpAgentInfo agentInfo) throws IOException {

        Target<Address> target                  = null;
        TransportMapping<UdpAddress> transport  = null;
        Snmp snmpInstance                       = null;

        try {
            Address targetAddress = GenericAddress.parse(
                agentInfo.getHost() + "/" + agentInfo.getPort()
            );

            transport       = new DefaultUdpTransportMapping(new UdpAddress(0)); 
            snmpInstance    = new Snmp(transport);
            transport.listen();

            // Configure target based on SNMP version and credentials
            if ("SNMPV1".equalsIgnoreCase(agentInfo.getSnmpVersion()) || "SNMPV2C".equalsIgnoreCase(agentInfo.getSnmpVersion())) {

                CommunityTarget<Address> comtarget = new CommunityTarget<>();

                comtarget.setCommunity(new OctetString(agentInfo.getCommunityString()));
                comtarget.setAddress(targetAddress);
                comtarget.setRetries(agentInfo.getRetries());
                comtarget.setTimeout(agentInfo.getTimeoutSeconds() * 1000L);
                comtarget.setVersion("SNMPV1".equalsIgnoreCase(agentInfo.getSnmpVersion()) ? SnmpConstants.version1 : SnmpConstants.version2c);
                
                target = comtarget;

            } else if ("SNMPV3".equalsIgnoreCase(agentInfo.getSnmpVersion())) {

                if (agentInfo.getUserName() == null || agentInfo.getUserName().isEmpty()) {
                    throw new IllegalArgumentException("SNMPv3 requires a username.");
                }

                USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(snmpInstance.getLocalEngineID()), 0);
                SecurityModels.getInstance().addSecurityModel(usm);

                OctetString userName = new OctetString(agentInfo.getUserName());
                OctetString authPass = agentInfo.getPassword() != null ? new OctetString(agentInfo.getPassword()) : null;
                OctetString privPass = agentInfo.getPassword() != null ? new OctetString(agentInfo.getPassword()) : null; // Assuming privPass is same as authPass for simplicity

                UsmUser usmUser = null;
                // Determine authentication and privacy protocols
                if (authPass != null && authPass.length() > 0) {
                    if (privPass != null && privPass.length() > 0) {

                        // Auth and Privacy
                        usmUser = new UsmUser(userName, AuthMD5.ID, authPass, PrivDES.ID, privPass); // Example: MD5/DES
                        // More options: AuthSHA, PrivAES128, PrivAES192, PrivAES256

                    } else {
                        // Auth No-Privacy
                        usmUser = new UsmUser(userName, AuthMD5.ID, authPass, null, null); // Example: MD5
                    }
                } else {
                    // No Auth No-Privacy (not recommended for SNMPv3)
                    usmUser = new UsmUser(userName, null, null, null, null);
                }

                if (usmUser != null) {
                    snmpInstance.getUSM().addUser(usmUser);
                }

                UserTarget<Address> userTarget = new UserTarget<>();

                userTarget.setAddress(targetAddress);
                userTarget.setRetries(agentInfo.getRetries());
                userTarget.setTimeout(agentInfo.getTimeoutSeconds() * 1000L);
                userTarget.setVersion(SnmpConstants.version3);
                userTarget.setSecurityLevel(SecurityLevel.AUTH_PRIV); // Or AUTH_NOPRIV, NOAUTH_NOPRIV
                userTarget.setSecurityName(userName);
                target = userTarget;

            } else {
                throw new IllegalArgumentException("Unsupported SNMP version: " + agentInfo.getSnmpVersion());

            }

            for (String oidString : agentInfo.getOids()) {

                OID oid = new OID(oidString);
                PDU pdu = new PDU();

                if ("GET".equalsIgnoreCase(agentInfo.getPollMode())) {
                    pdu.add(new VariableBinding(oid));
                    pdu.setType(PDU.GET);

                    ResponseEvent<Address> responseEvent = snmpInstance.send(pdu, target); 

                    if (responseEvent != null) {
                        PDU responsePDU = responseEvent.getResponse();
                        if (responsePDU != null) {
                            if (responsePDU.getErrorStatus() == PDU.noError) {
                                for (VariableBinding vb : responsePDU.getVariableBindings()) {
                                    processVariableBinding(vb, agentInfo);
                                    
                                }
                            } else {
                                LOG.warn("{} SNMP GET Error: {} for OID {} on {}:{}",
                                    Thread.currentThread().getName(),
                                    responsePDU.getErrorStatusText(),
                                    oidString,
                                    agentInfo.getHost(),
                                    agentInfo.getPort()
                                );
                            }
                        } else {
                            LOG.warn("{} SNMP GET Timeout or No Response for OID {} on {}:{}",
                                Thread.currentThread().getName(),
                                oidString,
                                agentInfo.getHost(),
                                agentInfo.getPort()
                            );
                        }
                    } else {
                        LOG.warn("{} SNMP GET No ResponseEvent for OID {} on {}:{}",
                            Thread.currentThread().getName(),
                            oidString,
                            agentInfo.getHost(),
                            agentInfo.getPort()
                        );
                    }
                } else if ("WALK".equalsIgnoreCase(agentInfo.getPollMode())) {

                    OID rootOid     = oid;
                    OID currentOid  = (lastWalkedOid != null && lastWalkedOid.startsWith(rootOid)) ? lastWalkedOid : rootOid;

                    // Implement a simple GETNEXT loop for WALK functionality
                    // For a full-fledged WALK, snmp4j's TreeUtils or TableUtils are better.
                    // This is a basic illustration.
                    while (true) {

                        pdu = new PDU();
                        pdu.add(new VariableBinding(currentOid));
                        pdu.setType(PDU.GETNEXT);

                        ResponseEvent<Address> responseEvent = snmpInstance.send(pdu, target);

                        if (responseEvent == null || responseEvent.getResponse() == null) {
                            LOG.warn("{} SNMP WALK Timeout or No Response starting from OID {} on {}:{}",
                                Thread.currentThread().getName(),
                                currentOid.toString(),
                                agentInfo.getHost(),
                                agentInfo.getPort()
                            );
                            break; // Exit loop on timeout/no response
                        }

                        PDU responsePDU = responseEvent.getResponse();

                        if (responsePDU.getErrorStatus() != PDU.noError) {
                            LOG.warn("{} SNMP WALK Error: {} for OID {} on {}:{}",
                                Thread.currentThread().getName(),
                                responsePDU.getErrorStatusText(),
                                currentOid.toString(),
                                agentInfo.getHost(),
                                agentInfo.getPort()
                            );
                            break; // Exit loop on error
                        }

                        // Fix for line 340
                        VariableBinding vb = responsePDU.getVariableBindings().get(0);

                        // Fix for line 348
                        if (!vb.getOid().startsWith(rootOid) || (vb.getVariable() instanceof Null)) {
                            LOG.debug("{} SNMP WALK: Reached end of MIB view or walked out of scope for OID {}",
                                Thread.currentThread().getName(),
                                rootOid.toString()
                            );
                            break;                                      // End of MIB view or walked out of the original OID's subtree

                        }

                        processVariableBinding(vb, agentInfo);
                        currentOid      = vb.getOid();                  // Continue from the OID of the received variable binding
                        lastWalkedOid   = currentOid;                   // Store for state snapshot

                    }
                } else {
                    LOG.error("{} Unsupported poll mode: {}. Only GET and WALK are supported.",
                        Thread.currentThread().getName(),
                        agentInfo.getPollMode()
                    );
                }
            }
        } finally {
            if (snmpInstance != null) {
                try {
                    snmpInstance.close();

                } catch (IOException e) {
                    LOG.error("{} Error closing SNMP instance: {} {}", 
                        Thread.currentThread().getName(), 
                        e.getMessage(), 
                        e
                    );
                }
            }
            if (transport != null) {
                try {
                    transport.close();

                } catch (IOException e) {
                    LOG.error("{} Error closing SNMP transport: {} {}", 
                        Thread.currentThread().getName(), 
                        e.getMessage(), 
                        e
                    );
                }
            }
        }
    }

    private void processVariableBinding(VariableBinding vb, SnmpAgentInfo agentInfo) {

        String deviceId                 = agentInfo.getHost();                                  // Device ID could be host or a configured ID
        String metricOid                = vb.getOid().toString();
        String metricValue              = vb.getVariable().toString();
        String dataType                 = vb.getVariable().getSyntaxString();
        String instanceIdentifier       = "";                                                   // For table-based OIDs, this would be derived from the OID suffix

        // Parse instance identifier if the OID indicates a table entry
        // This is a basic example; more robust parsing might be needed for complex OIDs
        if (vb.getOid().size() > 2 && metricOid.startsWith(agentInfo.getOids().get(0))) { // Assuming first OID is root for table
            try {
                // The instance identifier is the part of the OID after the base OID
                //instanceIdentifier = vb.getOid().trim(agentInfo.getOids().get(0).length()).toString();
                OID oid = vb.getOid();
                oid.trim(agentInfo.getOids().get(0).length());
                instanceIdentifier = oid.toString();

            } catch (Exception e) {
                LOG.warn("{} Error parsing instance identifier for OID {}: {}",
                    Thread.currentThread().getName(),
                    metricOid,
                    e.getMessage()
                );
            }
        }

        SnmpData snmpData = new SnmpData(
            deviceId,
            metricOid,
            metricValue,
            dataType,
            instanceIdentifier,
            LocalDateTime.now()
        );

        // Convert SnmpData to Flink's RowData based on the table schema
        GenericRowData rowData = new GenericRowData(rowType.getFieldCount());

        for (int i = 0; i < rowType.getFieldCount(); i++) {

            String fieldName        = rowType.getFieldNames().get(i);

            switch (fieldName) {
                case "deviceId":
                    rowData.setField(i, StringData.fromString(snmpData.getDeviceId()));
                    break;

                case "metricOid":
                    rowData.setField(i, StringData.fromString(snmpData.getMetricOid()));
                    break;

                case "metricValue":
                    rowData.setField(i, StringData.fromString(snmpData.getMetricValue()));
                    break;

                case "dataType":
                    rowData.setField(i, StringData.fromString(snmpData.getDataType()));
                    break;

                case "instanceIdentifier":
                    rowData.setField(i, StringData.fromString(snmpData.getInstanceIdentifier()));
                    break;

                case "ts":
                    rowData.setField(i, TimestampData.fromLocalDateTime(snmpData.getTs()));
                    break;

                default:
                    // Handle other fields or set to null
                    rowData.setField(i, null);
                    break;
            }
        }
        collector.collect(rowData);
    }

    @Override
    public List<SnmpSourceSplit> snapshotState(long checkpointId) {

        LOG.debug("{} SnmpSourceReader: snapshotState() for checkpointId {}.",
            Thread.currentThread().getName(),
            checkpointId
        );

        // System.out.println("SnmpSourceReader: snapshotState() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        List<SnmpSourceSplit> state = new ArrayList<>(splits);
        if (currentAgentInfo != null) {
            // Re-create the split to include the last walked OID for resuming WALK
            SnmpSourceSplit currentSplitWithProgress = new SnmpSourceSplit(
                currentAgentInfo.getHost() + "_" + currentAgentInfo.getPort() + "_WALK", // A unique split ID
                currentAgentInfo
            );

            // Although SnmpSourceSplit doesn't directly store lastWalkedOid, it will be implicitly
            // managed by the reader upon restore if it re-assigns the split.
            // For true progress, SnmpSourceSplit would need to be extended to hold this.
            state.add(currentSplitWithProgress);
        }

        // Also add any records not yet emitted
        while (!fetchedRecords.isEmpty()) {
            // This is a simplified approach. In a real scenario, fetchedRecords
            // should probably be part of the checkpointed state of the reader itself,
            // or handled via Flink's outputting mechanism directly without an internal buffer.
            // For now, we'll just acknowledge that they aren't part of split state directly.
            fetchedRecords.clear(); // Clear to avoid re-emitting
        }
        return state;
    }

    @Override
    public void addSplits(List<SnmpSourceSplit> splitsToAdd) {

        LOG.debug("{} SnmpSourceReader: addSplits() called with {} splits.",
            Thread.currentThread().getName(),
            splitsToAdd.size()
        );

        // System.out.println("SnmpSourceReader: addSplits() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        splits.addAll(splitsToAdd);
        // If the reader was waiting for splits, signal availability
        if (!availabilityFuture.isDone() && !splits.isEmpty()) {
            availabilityFuture.complete(null);
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {

        LOG.debug("{} SnmpSourceReader: isAvailable() called.",
            Thread.currentThread().getName()
        );
        return availabilityFuture;
    }

    @Override
    public void close() throws Exception {

        LOG.debug("{} SnmpSourceReader: close() called.",
            Thread.currentThread().getName()
        );

        // System.out.println("SnmpSourceReader: close() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

        if (snmp != null) {
            snmp.close();
        }
        splits.clear();
        fetchedRecords.clear();
        availabilityFuture.complete(null); // Ensure any pending futures are completed
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

        LOG.debug("{} SnmpSourceReader: notifyCheckpointComplete() for checkpointId {}.",
            Thread.currentThread().getName(),
            checkpointId
        );

        // System.out.println("SnmpSourceReader: notifyCheckpointComplete() called for Thread: "
        //     + Thread.currentThread().getName()
        //     + " (Direct System.out)"
        // );

    }

    // This method is not part of the SourceReader interface, it's an internal helper
    // for restoring the splits queue from a snapshot. The actual snapshotState and
    // addSplits methods are defined in the SourceReader interface.
    // Therefore, the @Override annotation was correctly removed in the previous step.\
    public void restoreState(List<SnmpSourceSplit> state) {

        LOG.debug("{} Called, Restoring state with {} splits. Clearing current splits and fetched records.",
            Thread.currentThread().getName(),
            state.size()
        );

        this.splits.clear();
        this.fetchedRecords.clear();
        this.splits.addAll(state);

        LOG.debug("{} Called, State restored. Total pending splits after restore: {}. Attempting to re-assign if necessary.",
            Thread.currentThread().getName(),
            this.splits.size()
        );

        if (!this.splits.isEmpty() && currentAgentInfo == null) {
            
            LOG.debug("{} Called, Assigning a split immediately after state restore as currentAgentInfo is null.",
                Thread.currentThread().getName()
            );
            assignNextSplit();
            lastPollTime = 0;
        }
    }

    @Override
    public void notifyNoMoreSplits() {

        LOG.debug("{} notifyNoMoreSplits called. No more splits expected.",
            Thread.currentThread().getName()
        );
        
        // System.out.println("SnmpSourceReader: notifyNoMoreSplits called for Thread: " 
        //     + Thread.currentThread().getName() 
        //     + " (Direct System.out)"
        // );

        // For a continuous source, this method typically does not signal end of input,
        // as data might still arrive from existing assigned splits via polling.
        // If it was a bounded source, this would signal the end of input once current splits are exhausted.
        noMoreSplits.set(true);
        availabilityFuture.complete(null); // Ensure pollNext can observe this change
    }

    /**
     * A simple collector to bridge the output of the reader to the Flink framework.
     */
    private class RecordCollector implements Collector<RowData> {
        @Override
        public void collect(RowData record) {
            fetchedRecords.offer(record);
        }

        @Override
        public void close() {
            // No-op for this simple collector
        }
    }
}