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
/    pollSnmpAgent(): Orchestrates the SNMP request (GET or WALK).\
/    processVariableBinding(): Extracts the OID and its corresponding value from the SNMP response and formats it into a SnmpData object.\
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.TreeEvent;
import org.snmp4j.util.TreeUtils;
import org.snmp4j.util.PDUFactory;              // pls take note of the new location of the PDUFactory classes.
import org.snmp4j.util.DefaultPDUFactory;       // https://javadoc.io/doc/org.snmp4j/snmp4j/3.3.0/org/snmp4j/util/DefaultPDUFactory.html
import org.snmp4j.smi.Address;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CompletableFuture;

public class SnmpSourceReader implements SourceReader<RowData, SnmpSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpSourceReader.class);

    private final SourceReaderContext readerContext;
    private final Queue<SnmpSourceSplit> splits;
    private final Queue<RowData> fetchedRecords;

    private SnmpAgentInfo currentAgentInfo;
    private Snmp currentSnmp;
    private long lastPollTime; // Timestamp of the last successful poll for the current agent

    // Added: CompletableFuture to signal availability
    private CompletableFuture<Void> availabilityFuture;


    public SnmpSourceReader(SourceReaderContext readerContext) {
        this.readerContext      = readerContext;
        this.splits             = new ConcurrentLinkedQueue<>();
        this.fetchedRecords     = new ConcurrentLinkedQueue<>();
        this.lastPollTime       = 0; // Initialize last poll time
        this.availabilityFuture = new CompletableFuture<>(); // Initialize the future

        LOG.debug("{} SNMP Source Reader initialized.", Thread.currentThread().getName());
    }

    @Override
    public void start() {

        LOG.debug("{} SNMP Source Reader started. Requesting splits...", Thread.currentThread().getName());
        
        // Request splits from the enumerator.
        readerContext.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) {
        
        LOG.debug("{} pollNext called. Current fetchedRecords size: {}", Thread.currentThread().getName(), fetchedRecords.size());

        try {
            // If there are already records fetched, emit them.
            while (!fetchedRecords.isEmpty()) {
                output.collect(fetchedRecords.poll());
        
                LOG.debug("{} Emitted a record. Remaining in queue: {}", Thread.currentThread().getName(), fetchedRecords.size());
                // If we just emitted a record, we might have more, or we might need to poll again.
                // For a continuous source, we indicate MORE_AVAILABLE if there's any chance of more data.
                // We also ensure the availability future is completed as data has been made available.
                if (!availabilityFuture.isDone()) {
                    availabilityFuture.complete(null);
                }
        
                return InputStatus.MORE_AVAILABLE;
            }

            // If no current agent info, assign a new split
            if (currentAgentInfo == null) {
        
                LOG.debug("{} currentAgentInfo is null. Attempting to assign next split.", Thread.currentThread().getName());
                assignNextSplit();
                if (currentAgentInfo == null) {
                    LOG.debug("{} No splits available or assigned. (NOTHING_AVAILABLE)", Thread.currentThread().getName());
                    // If no splits and no records, we are currently not available.
                    // The availability future should remain incomplete until a split is assigned or new data is ready.
        
                    return InputStatus.NOTHING_AVAILABLE;
                }
            }

            long currentTime        = System.currentTimeMillis();
            long requiredInterval   = (long) currentAgentInfo.getIntervalSeconds() * 1000;

            // Check if it's time to poll the SNMP agent again
            if (currentTime - lastPollTime >= requiredInterval) {
                LOG.info("{} Time to poll SNMP agent: {}. Last poll: {} ms ago, required: {} ms.",
                    Thread.currentThread().getName(),
                    currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort(),
                    (currentTime - lastPollTime),
                    requiredInterval
                );
                pollSnmpAgent(); // Perform the SNMP poll
                lastPollTime = currentTime; // Update last poll time

                // After polling, check if records were fetched and immediately make them available
                if (!fetchedRecords.isEmpty()) {
                    output.collect(fetchedRecords.poll()); // Emit at least one record if available
                    if (!availabilityFuture.isDone()) {
                        availabilityFuture.complete(null); // Complete the future as data is available
                    }
                    return InputStatus.MORE_AVAILABLE;
                }
            } else {
                LOG.debug("{} Not yet time to poll. Next poll in {} ms for agent {}. (NOTHING_AVAILABLE)",
                    Thread.currentThread().getName(),
                    (requiredInterval - (currentTime - lastPollTime)),
                    currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort()
                );
            }

            // If no records were fetched and it's not time to poll, reset availabilityFuture
            // to an incomplete state so Flink can wait for it.
            if (availabilityFuture.isDone()) {
                this.availabilityFuture = new CompletableFuture<>(); // Reset for next wait
            }
            return InputStatus.NOTHING_AVAILABLE; // No records currently available, but might be later
        } catch (Exception e) {
            LOG.error("{} Critical error in pollNext: {}.", Thread.currentThread().getName(), e.getMessage(), e);

            // On critical error, fail the availability future and propagate the error.
            availabilityFuture.completeExceptionally(e);
            // Propagate the error by throwing a RuntimeException
            throw new RuntimeException("Critical error in SNMP Source Reader.", e);
        }
    }

    /**
     * Returns a {@link CompletableFuture} that is completed when the source has more data available.
     * This method is crucial for Flink's backpressure mechanism.
     *
     * @return A CompletableFuture that completes when more data is available.
     */
    @Override
    public CompletableFuture<Void> isAvailable() {
        // If there are fetched records, we are immediately available.
        // Otherwise, return the future that will be completed when new data is fetched.
        if (!fetchedRecords.isEmpty()) {
            if (!availabilityFuture.isDone()) {
                availabilityFuture.complete(null); // Ensure future is completed if data is ready
            }
        }
        return availabilityFuture;
    }


    private void assignNextSplit() {
        SnmpSourceSplit split = splits.poll();
        if (split != null) {
            LOG.info("{} Assigning new split: {}", Thread.currentThread().getName(), split.splitId());
            currentAgentInfo = split.getAgentInfo();

            // Initialize SNMP object for the current agent if not already initialized or if agent changed
            if (currentSnmp == null) {
                try {
                    TransportMapping<UdpAddress> transport = new DefaultUdpTransportMapping();
                    currentSnmp = new Snmp(transport);
                    transport.listen();
                    LOG.info("{} SNMP TransportMapping listening.", Thread.currentThread().getName());

                    // A new split means we are now ready to poll, so complete the availability future
                    if (availabilityFuture.isDone()) {
                        this.availabilityFuture = new CompletableFuture<>(); // Create new future if previous was done
                    }
                    availabilityFuture.complete(null); // Signal availability
                } catch (IOException e) {
                    LOG.error("{} Failed to initialize SNMP transport: {}", Thread.currentThread().getName(), e.getMessage(), e);
                    currentSnmp = null; // Clear currentSnmp to reattempt initialization
                    availabilityFuture.completeExceptionally(e); // Fail availability future
                    throw new RuntimeException("Failed to initialize SNMP transport mapping.", e);
                }
            } else {
                 // If a split is assigned and SNMP is already initialized, signal availability
                 // This handles the case where pollNext() asks for splits, and we assign one
                 // but SNMP transport was already set up.
                 if (availabilityFuture.isDone()) {
                    this.availabilityFuture = new CompletableFuture<>();
                 }
                 availabilityFuture.complete(null);
            }
        } else {
            LOG.debug("{} No more splits in queue. (currentAgentInfo set to null)", Thread.currentThread().getName());
            currentAgentInfo = null; // No splits to assign
            // If no splits, we might not be available until new splits arrive.
            // The future should remain incomplete unless explicitly signaled by addSplits.
        }
    }


    private void pollSnmpAgent() {
        if (currentAgentInfo == null || currentSnmp == null) {
            LOG.warn("{} Cannot poll SNMP agent: currentAgentInfo or currentSnmp is null. Skipping poll.", Thread.currentThread().getName());
            return;
        }

        LOG.debug("{} Polling agent: {}", Thread.currentThread().getName(), currentAgentInfo.getHost());

        try {
            CommunityTarget<UdpAddress> target = new CommunityTarget<>();
            target.setCommunity(new OctetString(currentAgentInfo.getCommunityString()));
            target.setAddress(new UdpAddress(currentAgentInfo.getHost() + "/" + currentAgentInfo.getPort()));
            target.setRetries(currentAgentInfo.getRetries());
            target.setTimeout(currentAgentInfo.getTimeoutSeconds() * 1000L); // Milliseconds
            target.setVersion(SnmpConstants.version2c); // Defaulting to SNMPv2c for simplicity as per common use

            // Handle SNMP version specific settings if needed (e.g., for SNMPv3)
            if (currentAgentInfo.getSnmpVersion().equalsIgnoreCase("SNMPv1")) {
                target.setVersion(SnmpConstants.version1);
            } else if (currentAgentInfo.getSnmpVersion().equalsIgnoreCase("SNMPv3")) {
                // SNMPv3 setup is more complex, requires UserTarget and UserTable
                // For simplicity, we are focusing on v1/v2c for now as per current examples.
                LOG.warn("{} SNMPv3 is not fully implemented in this example. Falling back to SNMPv2c.", Thread.currentThread().getName());
                target.setVersion(SnmpConstants.version2c);
            }

            PDU pdu = new PDU();
            List<OID> oidsToRequest = new ArrayList<>();
            for (String oidString : currentAgentInfo.getOids()) {
                oidsToRequest.add(new OID(oidString));
            }

            if (currentAgentInfo.getPollMode().equalsIgnoreCase("GET")) {
                for (OID oid : oidsToRequest) {
                    pdu.add(new VariableBinding(oid));
                }
                pdu.setType(PDU.GET);

                LOG.debug("{} Sending SNMP GET request for OIDs: {}", Thread.currentThread().getName(), oidsToRequest);
                ResponseEvent<UdpAddress> responseEvent = currentSnmp.send(pdu, target);
                if (responseEvent != null && responseEvent.getResponse() != null) {
                    PDU responsePDU = responseEvent.getResponse();
                    LOG.debug("{} Received SNMP GET response with {} variable bindings.", Thread.currentThread().getName(), responsePDU.size());
                    for (int i = 0; i < responsePDU.size(); i++) {
                        VariableBinding vb = responsePDU.get(i);
                        processVariableBinding(vb);
                    }
                } else {
                    LOG.warn("{} No response or empty response received for GET request from agent {}. (ResponseEvent was null or response was null)",
                        Thread.currentThread().getName(),
                        currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort()
                    );
                }
            } else if (currentAgentInfo.getPollMode().equalsIgnoreCase("WALK")) {
                LOG.debug("{} Sending SNMP WALK request for OIDs: {}", Thread.currentThread().getName(), oidsToRequest);
                // For WALK, usually one root OID is provided per walk
                for (OID rootOid : oidsToRequest) {
                    // Corrected: TreeUtils constructor takes Snmp and PDUFactory
                    TreeUtils treeUtils = new TreeUtils(currentSnmp, new DefaultPDUFactory());
                    List<TreeEvent> events = treeUtils.walk(target, new OID[]{rootOid});

                    if (events == null || events.isEmpty()) {
                        LOG.warn("{} No WALK events received for OID {} from agent {}.",
                            Thread.currentThread().getName(),
                            rootOid.toString(),
                            currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort()
                        );
                        continue;
                    }

                    for (TreeEvent event : events) {
                        if (event.getVariableBindings() == null) {
                            LOG.warn("{} WALK event for OID {} has null variable bindings.",
                                Thread.currentThread().getName(),
                                rootOid.toString()
                            );
                            continue;
                        }
                        for (VariableBinding vb : event.getVariableBindings()) {
                            processVariableBinding(vb);
                        }
                    }
                }
            } else {
                LOG.error("{} Unsupported SNMP poll mode: {}. Only GET and WALK are supported.",
                    Thread.currentThread().getName(),
                    currentAgentInfo.getPollMode()
                );
            }
        } catch (Exception e) {
            LOG.error("{} Critical error during SNMP polling for agent {}: {}",
                Thread.currentThread().getName(),
                currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort(),
                e.getMessage(),
                e
            );
            // Re-throw as RuntimeException to fail the task, as checked exceptions are generally not allowed here.
            throw new RuntimeException("Error during SNMP polling.", e);
        }
    }


    private void processVariableBinding(VariableBinding vb) {
        if (vb == null) {
            LOG.warn("{} Received null VariableBinding. Skipping processing.", Thread.currentThread().getName());
            return;
        }

        String deviceId = currentAgentInfo.getHost();
        String metricOid = vb.getOid() != null ? vb.getOid().toString() : null;
        String metricValue = null;
        String dataType = null;
        String instanceIdentifier = null;

        if (vb.getVariable() != null) {
            LOG.debug("{} Processing VariableBinding: OID={}, Variable={}, Type={}",
                Thread.currentThread().getName(),
                metricOid,
                vb.getVariable().toString(),
                vb.getVariable().getClass().getSimpleName()
            );

            try {
                metricValue = vb.getVariable().toString();
                dataType = vb.getVariable().getClass().getSimpleName();

                if (metricOid != null && metricOid.lastIndexOf('.') > 0) {
                    instanceIdentifier = metricOid.substring(metricOid.lastIndexOf('.') + 1);
                }

            } catch (Exception e) {
                LOG.error("{} Error converting SNMP Variable to String for OID {}: {}",
                    Thread.currentThread().getName(),
                    metricOid,
                    e.getMessage(),
                    e
                );
                metricValue = "ERROR_CONVERTING";
            }
        } else {
            LOG.warn("{} Variable is null for OID {}. Setting metricValue to null.",
                Thread.currentThread().getName(),
                metricOid
            );
            metricValue = null;
        }

        if (deviceId == null) {
            LOG.error("{} deviceId is null. Cannot create SnmpData record. Source Agent: {}",
                Thread.currentThread().getName(), currentAgentInfo.getHost()
            );
            return;
        }
        if (metricOid == null) {
            LOG.error("{} metricOid is null. Cannot create SnmpData record. Device: {}",
                Thread.currentThread().getName(), deviceId
            );
            return;
        }

        SnmpData snmpData = new SnmpData(
                deviceId,
                metricOid,
                metricValue,
                dataType,
                instanceIdentifier,
                LocalDateTime.now()
        );

        LOG.info("{} Fetched SnmpData: {}", Thread.currentThread().getName(), snmpData.toString());

        GenericRowData rowData = new GenericRowData(6);
        rowData.setField(0, StringData.fromString(snmpData.getDeviceId()));
        rowData.setField(1, StringData.fromString(snmpData.getMetricOid()));
        
        rowData.setField(2, snmpData.getMetricValue() != null ? StringData.fromString(snmpData.getMetricValue()) : null);
        rowData.setField(3, snmpData.getDataType() != null ? StringData.fromString(snmpData.getDataType()) : null);
        rowData.setField(4, snmpData.getInstanceIdentifier() != null ? StringData.fromString(snmpData.getInstanceIdentifier()) : null);
        
        rowData.setField(5, TimestampData.fromLocalDateTime(snmpData.getTs()));

        fetchedRecords.offer(rowData);
        LOG.debug("{} Added RowData to fetchedRecords queue: {}", Thread.currentThread().getName(), rowData.toString());
    }

    @Override
    public List<SnmpSourceSplit> snapshotState(long checkpointId) {
        LOG.debug("{} Called, snapshotState for checkpointId {}. Current splits queue size: {}. Current agent: {}",
            Thread.currentThread().getName(),
            checkpointId,
            splits.size(),
            (currentAgentInfo != null ? currentAgentInfo.getHost() : "none")
        );
        List<SnmpSourceSplit> state = new ArrayList<>(splits);
        if (currentAgentInfo != null) {
            state.add(new SnmpSourceSplit(currentAgentInfo.getHost() + "_" + System.currentTimeMillis(), currentAgentInfo));
            LOG.debug("{} Added current agent info to snapshot state. Total: {}", Thread.currentThread().getName(), state.size());
        }
        return state;
    }

    @Override
    public void addSplits(List<SnmpSourceSplit> newSplits) {
        LOG.debug("{} Called, Adding {} new splits.", Thread.currentThread().getName(), newSplits.size());
        splits.addAll(newSplits);
        LOG.debug("{} Total splits in queue after add: {}. Attempting to assign if needed.", Thread.currentThread().getName(), splits.size());
        if (currentAgentInfo == null && !splits.isEmpty()) {
            assignNextSplit();
        }
    }

    @Override
    public void handleSourceEvents(org.apache.flink.api.connector.source.SourceEvent sourceEvent) {
        LOG.debug("{} Called, Handling source event: {}", Thread.currentThread().getName(), sourceEvent.getClass().getSimpleName());
    }

    @Override
    public void close() throws Exception {
        LOG.debug("{} Called, Closing SNMP Source Reader.", Thread.currentThread().getName());
        if (currentSnmp != null) {
            currentSnmp.close();
            currentSnmp = null;
        }
        fetchedRecords.clear();
        splits.clear();
        currentAgentInfo = null;
        LOG.debug("{} Called, SNMP Source Reader resources cleared.", Thread.currentThread().getName());
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
        LOG.debug("{} notifyNoMoreSplits called. No more splits expected.", Thread.currentThread().getName());
        // For a continuous source, this method typically does nothing as new data might still arrive from polling.
    }
}