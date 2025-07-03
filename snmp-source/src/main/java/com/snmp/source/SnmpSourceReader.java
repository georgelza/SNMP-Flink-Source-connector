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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.mp.MPv3;

import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.Priv3DES;
import org.snmp4j.security.PrivAES128;
import org.snmp4j.security.PrivAES192;
import org.snmp4j.security.PrivAES256;
import org.snmp4j.security.PrivDES;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityModel;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;

import org.snmp4j.smi.Address;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;

import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.WorkerPool;
import org.snmp4j.util.PDUFactory;
import org.snmp4j.util.TableEvent;
import org.snmp4j.util.TableUtils;
import org.snmp4j.UserTarget;
import org.snmp4j.agent.DefaultMOServer;


/**
 * A {@link SourceReader} that polls SNMP agents and emits {@link RowData}.
 */
public class SnmpSourceReader implements SourceReader<RowData, SnmpSourceSplit> {

    private static final Logger LOG                             = LoggerFactory.getLogger(SnmpSourceReader.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER  = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final SourceReaderContext    readerContext;
    private final DataType               producedDataType;
    private final RowType                producedRowType;
    private final ReentrantLock          lock = new ReentrantLock();
    private final RecordCollector        recordCollector;
    private final Queue<SnmpSourceSplit> splits;
    private final Queue<RowData>         fetchedRecords;
    private final java.util.concurrent.atomic.AtomicBoolean noMoreSplits;

    @Nullable
    private SnmpSourceSplit currentSplit;

    @Nullable
    private SnmpAgentInfo           currentAgentInfo;
    private long                    lastPollTime;
    private CompletableFuture<Void> availabilityFuture;
    private String                  errorMessage;

    // Add a public getter method
    public String getErrorMessage() {
        return errorMessage;
    }

    public SnmpSourceReader(SourceReaderContext readerContext, DataType producedDataType) {

        this.readerContext          = readerContext;
        this.producedDataType       = producedDataType;
        this.producedRowType        = (RowType) producedDataType.getLogicalType();
        this.splits                 = new ConcurrentLinkedQueue<>();
        this.fetchedRecords         = new ConcurrentLinkedQueue<>();
        this.noMoreSplits           = new java.util.concurrent.atomic.AtomicBoolean(false);
        this.availabilityFuture     = new CompletableFuture<>();
        this.recordCollector        = new RecordCollector();
        this.lastPollTime           = 0;                                // Initialize last poll time

        LOG.debug("{} SnmpSourceReader: Initialized with producedDataType: {}.",
            Thread.currentThread().getName(),
            producedDataType
        );

        System.out.println("SnmpSourceReader: SNMP Source Reader initialized, called" 
            + " for Thread: " + Thread.currentThread().getName() 
            + " (Direct System.out)"
        );
    }

    @Override
    public void start() {

        LOG.debug("{} SnmpSourceReader: start() called. Requesting splits...",
            Thread.currentThread().getName()
        );

        // Request a split from the enumerator if none are assigned yet.
        // This is important for the initial startup where no splits have been
        // assigned via `addSplits` yet.
        readerContext.sendSplitRequest();

        // Check if there are any splits restored from a previous execution
        if (!splits.isEmpty()) {

            LOG.debug("{} SnmpSourceReader: {} splits found during startup. Assigning the first one.",
                Thread.currentThread().getName(),
                splits.size()
            );
            assignNextSplit(); // Assign the first split immediately

        } else {
            LOG.debug("{} SnmpSourceReader: No splits found during startup. Waiting for splits.",
                Thread.currentThread().getName()
            );
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {

        LOG.debug("{} SnmpSourceReader: pollNext() called. fetchedRecords size: {}", 
            Thread.currentThread().getName(),
            fetchedRecords.size()
        );

        System.out.println("SnmpSourceReader: pollNext()" 
            + " fetchedRecords size... " + fetchedRecords.size()
            + " for Thread: "            + Thread.currentThread().getName() 
            + " (Direct System.out)"
        );

        try {
            // Emit fetched records first
            while (fetchedRecords.peek() != null) {
                output.collect(fetchedRecords.poll());

                LOG.debug("{} SnmpSourceReader: pollNext() called. Remaining in queue: {}", 
                    Thread.currentThread().getName(), 
                    fetchedRecords.size()
                );

                System.out.println("SnmpSourceReader: pollNext()" 
                    + " Remaining in queue... " + fetchedRecords.size()
                    + " for Thread: "           + Thread.currentThread().getName() 
                    + " (Direct System.out)"
                );

                // If we just emitted a record, we might have more, or we might need to poll again.
                // For a continuous source, we indicate MORE_AVAILABLE if there's any chance of more data.
                // We also ensure the availability future is completed as data has been made available.
                if (!availabilityFuture.isDone()) {
                    availabilityFuture.complete(null);
                }
        
                return InputStatus.MORE_AVAILABLE;
            }

            // If no current split is assigned, try to assign one
            if (currentAgentInfo == null) {

                LOG.debug("{} SnmpSourceReader: pollNext() currentAgentInfo is null. Attempting to assign next split.",
                    Thread.currentThread().getName()
                );

                assignNextSplit();
                if (currentAgentInfo == null) {
                    // If still no split after assignment attempt, and no more splits expected
                    if (noMoreSplits.get() && splits.isEmpty()) {

                        LOG.debug("{} SnmpSourceReader: pollNext() No more splits and no current split. Signaling FINISHED.",
                            Thread.currentThread().getName()
                        );
                        return InputStatus.END_OF_INPUT;

                    } else {
                        LOG.debug("{} SnmpSourceReader: pollNext() No current split, waiting for more splits.",
                            Thread.currentThread().getName()
                        );
                        return InputStatus.NOTHING_AVAILABLE; // Or WAITING_FOR_DATA
                    }
                }
            }

            long currentTime        = System.currentTimeMillis(); 
            long requiredInterval   = (long) currentAgentInfo.getIntervalSeconds() * 1000;

            //long elapsedSeconds     = (currentTime - lastPollTime) / 1000;

            // Poll only if the interval has passed
            if (currentTime - lastPollTime >= requiredInterval) {

                LOG.debug("{} SnmpSourceReader: pollNext() Polling SNMP agent {}:{}. Elapsed seconds: {}. {}",
                    Thread.currentThread().getName(),
                    currentAgentInfo.getHost(),
                    currentAgentInfo.getPort(),
                    (currentTime - lastPollTime),
                    requiredInterval
                );

                System.out.println("SnmpSourceReader: pollNext(): Polling" 
                    + " Agent: "    + currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort()
                    + " for Thread "+ Thread.currentThread().getName()
                    + " (Direct System.out)"
                );

                pollSnmpAgent(currentAgentInfo, recordCollector);
                lastPollTime = System.currentTimeMillis();
            
                System.out.println("SnmpSourceReader: pollNext(): "
                    + " Finished polling "  + lastPollTime
                    + " Agent: "            + currentAgentInfo.getHost() + ":" + currentAgentInfo.getPort()
                    + " for Thread "        + Thread.currentThread().getName()
                    + " (Direct System.out)"
                );

                LOG.debug("{} SnmpSourceReader: pollNext() Finished polling SNMP agent {}:{}.",
                    Thread.currentThread().getName(),
                    currentAgentInfo.getHost(),
                    currentAgentInfo.getPort()
                );

                // After polling, check if new records were fetched
                // if (fetchedRecords.peek() != null) {
                //     LOG.debug("{} SnmpSourceReader: pollNext() Fetched new records. Returning MORE_AVAILABLE.",
                //         Thread.currentThread().getName()
                //     );
                //     return InputStatus.MORE_AVAILABLE;
                // }

                // After polling, check if records were fetched and immediately make them available
                if (!fetchedRecords.isEmpty()) {
                    output.collect(fetchedRecords.poll());              // Emit at least one record if available
                    if (!availabilityFuture.isDone()) {
                        availabilityFuture.complete(null);        // Complete the future as data is available

                    }
                    return InputStatus.MORE_AVAILABLE;
                }

            } else {
                LOG.debug("{} SnmpSourceReader: pollNext() Waiting to poll SNMP agent {}:{}. Elapsed: {}s, Required: {}s.",
                    Thread.currentThread().getName(),
                    currentAgentInfo.getHost(),
                    currentAgentInfo.getPort(),
                    (currentTime - lastPollTime),
                    requiredInterval
                );
            }

            // If no records are available and no more splits are expected
            if (noMoreSplits.get() && splits.isEmpty() && fetchedRecords.isEmpty()) {
                LOG.debug("{} SnmpSourceReader: pollNext() No more splits, no current split, and no fetched records. Signaling FINISHED.",
                    Thread.currentThread().getName()
                );
                return InputStatus.END_OF_INPUT;
            }

            LOG.debug("{} SnmpSourceReader: pollNext() No data available now, waiting for next poll or splits.",
                Thread.currentThread().getName()
            );

            // Indicate that no data is currently available but more might come
            // The future will be completed when new data is available or splits are added.
            return InputStatus.NOTHING_AVAILABLE;
        
        } catch (Exception e) {
            LOG.error("{} SnmpSourceReader: Critical error in pollNext: {}. {}", 
                Thread.currentThread().getName(),
                e.getMessage(), 
                e
            );

            // On critical error, fail the availability future and propagate the error.
            availabilityFuture.completeExceptionally(e);

            // Propagate the error by throwing a RuntimeException
            throw new RuntimeException("Critical error in SNMP Source Reader.", e);
        }
    }

    @Override
    public List<SnmpSourceSplit> snapshotState(long checkpointId) {
        LOG.info("{} SnmpSourceReader: snapshotState() called for checkpointId {}. Current split: {}. Remaining splits: {}.",
            Thread.currentThread().getName(),
            checkpointId,
            currentSplit != null ? currentSplit.splitId() : "none",
            splits.size()
        );

        final List<SnmpSourceSplit> state = new LinkedList<>();
        // Add the current split back to the list of splits to be checkpointed
        if (currentSplit != null) {
            state.add(currentSplit);
        }
        state.addAll(splits); // Add any pending splits
        return state;
    }

    @Override
    public void close() throws Exception {
        LOG.debug("{} SnmpSourceReader: close() called.",
            Thread.currentThread().getName()
        );

        splits.clear();
        fetchedRecords.clear();
        currentSplit = null;
        currentAgentInfo = null;
    }

    @Override
    public void addSplits(List<SnmpSourceSplit> newSplits) {
        LOG.info("{} SnmpSourceReader: addSplits() called with {} new splits.",
            Thread.currentThread().getName(),
            newSplits.size()
        );

        splits.addAll(newSplits);
        // If no split is currently assigned, try to assign one now
        if (currentAgentInfo == null) {
            assignNextSplit();
        }

        availabilityFuture.complete(null);                  // Signal that new splits are available
        availabilityFuture = new CompletableFuture<>();           // Reset for next signal
    }

    private void assignNextSplit() {
        lock.lock();
        try {
            if (currentSplit == null && !splits.isEmpty()) {
                currentSplit = splits.poll();
                currentAgentInfo = currentSplit.getAgentInfo();
                lastPollTime = 0;                                 // Trigger immediate poll for the new split

                LOG.info("{} SnmpSourceReader: assignNextSplit() Assigned new split: {} with agent {}:{}.",
                    Thread.currentThread().getName(),
                    currentSplit.splitId(),
                    currentAgentInfo.getHost(),
                    currentAgentInfo.getPort()
                );

            } else if (currentSplit != null) {
                LOG.debug("{} SnmpSourceReader: assignNextSplit() Already have a current split {}. Not assigning a new one.",
                    Thread.currentThread().getName(),
                    currentSplit.splitId()
                );

            } else {
                LOG.debug("{} SnmpSourceReader: assignNextSplit() No splits available to assign.",
                    Thread.currentThread().getName()
                );
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {

        LOG.debug("{} SnmpSourceReader: handleSourceEvents() called with event: {}.",
            Thread.currentThread().getName(),
            sourceEvent
        );
        // No custom source events expected for now.
    }

    private void pollSnmpAgent(SnmpAgentInfo agentInfo, Collector<RowData> collector) {

        System.out.println("SnmpSourceReader: pollSnmpAgent(): Called" 
            + " Agent: "     + agentInfo.getHost() + ":" + agentInfo.getPort()
            + " for Thread " + Thread.currentThread().getName()
            + " (Direct System.out)"
        );

        Address targetAddress                   = GenericAddress.parse(agentInfo.getHost() + "/" + agentInfo.getPort());
        //USM usm                                 = null;
        TransportMapping<UdpAddress> transport  = null;
        Snmp snmp                               = null;
        try {

            transport                           = new DefaultUdpTransportMapping(new UdpAddress(0)); // Bind to any available port
            snmp                                = new Snmp(transport);

            if ("SNMPv1".equalsIgnoreCase(agentInfo.getSnmpVersion()) || ("SNMPv2C".equalsIgnoreCase(agentInfo.getSnmpVersion()))) {

                LOG.debug("{} SnmpSourceReader: pollSnmpAgent() Configuring SNMPv1/v2c (version: {}) for agent: {}:{}.",
                    Thread.currentThread().getName(),
                    agentInfo.getSnmpVersion(),
                    agentInfo.getHost(),
                    agentInfo.getPort()
                );
                
                System.out.println("SnmpSourceReader: pollSnmpAgent(): SNMPv1 Configuring" 
                    + " Agent: "     + agentInfo.getHost() + ":" + agentInfo.getPort()
                    + " for Thread " + Thread.currentThread().getName()
                    + " (Direct System.out)"
                );

                CommunityTarget<Address> target = new CommunityTarget<>();
                target.setCommunity(new OctetString(agentInfo.getCommunityString()));
                target.setAddress(targetAddress);
                target.setRetries(agentInfo.getRetries());
                target.setTimeout(agentInfo.getTimeoutSeconds() * 1000L); 

                if ("SNMPv1".equalsIgnoreCase(agentInfo.getSnmpVersion())) {
                    target.setVersion(SnmpConstants.version1);

                } else { // Default to SNMPv2c
                    target.setVersion(SnmpConstants.version2c);

                }

                LOG.debug("{} SnmpSourceReader: pollSnmpAgent() Set SNMPv1/v2c target for {}:{}.",
                    Thread.currentThread().getName(),
                    agentInfo.getHost(),
                    agentInfo.getPort()
                );

                sendSnmpRequest(snmp, target, agentInfo, collector);

            // } else if ("SNMPv3".equalsIgnoreCase(agentInfo.getSnmpVersion())) {

            //     LOG.debug("{} SnmpSourceReader: pollSnmpAgent() Configuring SNMPv3 for user: {}.",
            //         Thread.currentThread().getName(),
            //         agentInfo.getUserName()
            //     );

            //     SecurityProtocols.getInstance().addAuthenticationProtocol(new AuthMD5());
            //     SecurityProtocols.getInstance().addAuthenticationProtocol(new AuthSHA());
            //     SecurityProtocols.getInstance().addPrivacyProtocol(new PrivDES());
            //     SecurityProtocols.getInstance().addPrivacyProtocol(new PrivAES128());
            //     SecurityProtocols.getInstance().addPrivacyProtocol(new PrivAES192());
            //     SecurityProtocols.getInstance().addPrivacyProtocol(new PrivAES256());

            //     usm = new USM(SecurityProtocols.getInstance(), new OctetString(), new org.snmp4j.agent.DefaultMOServer());

            //     OctetString localEngineID = usm.getLocalEngineID(); 

            //     MPv3 mpv3 = new MPv3(usm); 

            //     snmp = new Snmp((TransportMapping)transport, new org.snmp4j.mp.MessageProcessingModel[] { mpv3 });

            //     snmp.listen();

            //     UsmUser user;
            //     OctetString userName        = new OctetString(agentInfo.getUserName());
            //     OctetString authPassphrase  = new OctetString(agentInfo.getPassword()); // Assuming password is authPassphrase
            //     OctetString privPassphrase  = new OctetString(agentInfo.getPassword()); // Assuming password is privPassphrase

            //     if (agentInfo.getPassword() != null && !agentInfo.getPassword().isEmpty()) {
            //         // Assuming SHA for auth and AES128 for priv as common secure choices
            //         user = new UsmUser(
            //             userName,
            //             AuthSHA.ID,
            //             authPassphrase,
            //             PrivAES128.ID,
            //             privPassphrase
            //         );
                    
            //         usm.addUser(user);
            //         LOG.debug("{} SnmpSourceReader: pollSnmpAgent() Added SNMPv3 user {} with AuthSHA and PrivAES128.",
            //             Thread.currentThread().getName(),
            //             agentInfo.getUserName()
            //         );

            //     } else {
            //         // No authentication or privacy
            //         user = new UsmUser(userName, null, null, null, null);
            //         usm.addUser(user);
            //         LOG.warn("{} SnmpSourceReader: pollSnmpAgent() SNMPv3 user {} configured without authentication or privacy.",
            //             Thread.currentThread().getName(),
            //             agentInfo.getUserName()
            //         );
            //     }

            //     UserTarget<Address> target = new UserTarget<>();

            //     target.setAddress(targetAddress);
            //     target.setVersion(SnmpConstants.version3);
            //     target.setSecurityName(userName);
            //     target.setSecurityLevel(SecurityLevel.AUTH_PRIV);           // Or NOAUTH_NOPRIV, AUTH_NOPRIV
            //     target.setRetries(agentInfo.getRetries());
            //     target.setTimeout(agentInfo.getTimeoutSeconds() * 1000L);   // Milliseconds

            //     LOG.debug("{} SnmpSourceReader: pollSnmpAgent() Set SNMPv3 target for {}:{}.",
            //         Thread.currentThread().getName(),
            //         agentInfo.getHost(),
            //         agentInfo.getPort()
            //     );

            //     sendSnmpRequest(snmp, target, agentInfo, collector);
            }
            

        } catch (UnknownHostException e) {
            this.errorMessage = e.getMessage();

            LOG.error("{} SnmpSourceReader: pollSnmpAgent() Unknown host for SNMP agent {}:{}. Error: {}. {}",
                Thread.currentThread().getName(),
                agentInfo.getHost(),
                agentInfo.getPort(),
                e.getMessage(),
                e
            );

            LOG.error("{} SnmpSourceReader: pollSnmpAgent() Failed to initialize SNMP session for split {}. Error: {}. Will attempt to assign next split. {}",
                Thread.currentThread().getName(),
                currentSplit.splitId(),
                e.getMessage(),
                e 
            );

            // In case of an initialization error, stop processing this split and try to get a new one.
            currentAgentInfo = null; // Mark current agent as failed/done
            assignNextSplit(); // Request a new split

        } catch (IOException e) {
            this.errorMessage = e.getMessage();

            LOG.error("{} SnmpSourceReader: pollSnmpAgent() IO error while polling SNMP agent {}:{}. Error: {}. {}",
                Thread.currentThread().getName(),
                agentInfo.getHost(),
                agentInfo.getPort(),
                e.getMessage(),
                e
            );

            LOG.error("{} SnmpSourceReader: pollSnmpAgent() Failed to open SNMP session for split {}. Error: {}. Will attempt to assign next split. {}",
                Thread.currentThread().getName(),
                currentSplit.splitId(),
                e.getMessage(),
                e 
            );

            currentAgentInfo = null; // Mark current agent as failed/done
            assignNextSplit(); // Request a new split

        } catch (Exception e) {
            this.errorMessage = e.getMessage();

            LOG.error("{} SnmpSourceReader: pollSnmpAgent() Unexpected error while polling SNMP agent {}:{}. Error: {}. {}",
                Thread.currentThread().getName(),
                agentInfo.getHost(),
                agentInfo.getPort(),
                e.getMessage(),
                e
            );

            LOG.error("{} SnmpSourceReader: pollSnmpAgent() Error while closing SNMP session for split {}: {}. {}",
                Thread.currentThread().getName(),
                (currentSplit != null ? currentSplit.splitId() : "N/A"),
                e.getMessage(),
                e 
            );        

        } finally {
            if (snmp != null) {
                try {
                    snmp.close();
                    LOG.debug("{} SnmpSourceReader: pollSnmpAgent() Closed SNMP session.",
                        Thread.currentThread().getName()
                    );

                } catch (IOException e) {
                    LOG.warn("{} SnmpSourceReader: pollSnmpAgent() Error closing SNMP session: {} {}.",
                        Thread.currentThread().getName(),
                        e.getMessage(),
                        e
                    );
                }
            }
            if (transport != null) {
                try {
                    transport.close();
                    LOG.debug("{} SnmpSourceReader: pollSnmpAgent() Closed transport mapping.",
                        Thread.currentThread().getName()
                    );

                } catch (IOException e) {
                    LOG.warn("{} SnmpSourceReader: pollSnmpAgent() Error closing transport mapping: {}. {}",
                        Thread.currentThread().getName(),
                        e.getMessage(),
                        e
                    );
                }
            }
        }
    }

    private <T extends org.snmp4j.Target<Address>> void sendSnmpRequest(Snmp snmp, T target, SnmpAgentInfo agentInfo, Collector<RowData> collector) throws IOException {

        LOG.error("{} SnmpSourceReader: sendSnmpRequest(): {} Called for Agent {}:{}",
            Thread.currentThread().getName(), 
            agentInfo.getPollMode(),
            agentInfo.getHost(),
            agentInfo.getPort()
        );

        System.out.println("SnmpSourceReader: sendSnmpRequest():"
            + " Poll Mode "         + agentInfo.getPollMode()
            + " Called for Agent: " + agentInfo.getHost() + ":" + agentInfo.getPort()
            + " for Thread "        + Thread.currentThread().getName()
            + " (Direct System.out)"
        );

        snmp.listen();
        PDU pdu = new PDU();
        List<OID> oidsToPoll = agentInfo.getOids().stream().map(s -> new OID(s)).collect(Collectors.toList());

        if ("GET".equalsIgnoreCase(agentInfo.getPollMode())) {

            LOG.debug("{} SnmpSourceReader: sendSnmpRequest() Performing SNMP GET for OIDs: {}.",
                Thread.currentThread().getName(),
                oidsToPoll
            );

            System.out.println("SnmpSourceReader: sendSnmpRequest()"
                + " Performing SNMP GET for OIDs: " + oidsToPoll
                + " for Thread "                    + Thread.currentThread().getName()
                + " (Direct System.out)"
            );

            for (OID oid : oidsToPoll) {
                pdu.add(new VariableBinding(oid));
            }

            pdu.setType(PDU.GET);
            ResponseEvent<Address> responseEvent = snmp.send(pdu, target);

            if (responseEvent != null && responseEvent.getResponse() != null) {

                LOG.error("{} SnmpSourceReader: sendSnmpRequest(): SNMP GET response receive for Agent {}:{} Response: {}",
                    Thread.currentThread().getName(), 
                    agentInfo.getHost(),
                    agentInfo.getPort(),
                    responseEvent
                );

                System.out.println("SnmpSourceReader: sendSnmpRequest(): Called" 
                    + " Agent: "            + agentInfo.getHost() + ":" + agentInfo.getPort()
                    + " Response receive"   + responseEvent
                    + " for Thread "        + Thread.currentThread().getName()
                    + " (Direct System.out)"
                 );

                
                for (VariableBinding vb : responseEvent.getResponse().getVariableBindings()) {
                    processVariableBinding(agentInfo.getHost(), agentInfo.getPort(), vb, collector);
                }

            } else if (responseEvent != null && responseEvent.getError() != null) {

                LOG.error("{} SnmpSourceReader: sendSnmpRequest() SNMP GET request to {}:{} failed with error: {}.",
                    Thread.currentThread().getName(),
                    agentInfo.getHost(),
                    agentInfo.getPort(),
                    responseEvent.getError().getMessage()
                );

            } else {
                LOG.warn("{} SnmpSourceReader: sendSnmpRequest() SNMP GET request to {}:{} timed out or no response.",
                    Thread.currentThread().getName(),
                    agentInfo.getHost(),
                    agentInfo.getPort()
                );
            }

        } else if ("WALK".equalsIgnoreCase(agentInfo.getPollMode())) {

            LOG.debug("{} SnmpSourceReader: sendSnmpRequest() Performing SNMP WALK for OIDs: {}.",
                Thread.currentThread().getName(),
                oidsToPoll
            );

            System.out.println("SnmpSourceReader: sendSnmpRequest()" 
                + " Performing SNMP WALK for OIDs: " + oidsToPoll
                + " for Thread "                     + Thread.currentThread().getName()
                + " (Direct System.out)"
            );

            // Using TableUtils for WALK operation
            TableUtils tableUtils = new TableUtils(snmp, new org.snmp4j.util.PDUFactory() {
                @Override
                public PDU createPDU(org.snmp4j.Target target) {
                    PDU pdu = new PDU();
                    pdu.setType(PDU.GETNEXT); // For WALK, typically GETNEXT is used
                    return pdu;
                }

                @Override
                public PDU createPDU(org.snmp4j.mp.MessageProcessingModel messageProcessingModel) {
                    PDU pdu = new PDU();
                    pdu.setType(PDU.GETNEXT);
                    return pdu;
                }
            });

            for (OID oid : oidsToPoll) {
                // The get );
                List<TableEvent> events = tableUtils.getTable(target, new OID[]{oid}, null, null);
                LOG.debug("{} SnmpSourceReader: sendSnmpRequest() SNMP WALK for OID {} returned {} events.",
                    Thread.currentThread().getName(),
                    oid,
                    events.size()
                );

                if (events.isEmpty()) {
                    LOG.warn("{} SnmpSourceReader: sendSnmpRequest() SNMP WALK for OID {} returned no data.",
                        Thread.currentThread().getName(),
                        oid
                    );
                }

                for (TableEvent event : events) {
                    if (getErrorMessage() != null && !getErrorMessage().isEmpty()) {
                        LOG.error("{} SnmpSourceReader: sendSnmpRequest() SNMP WALK error for OID {}: {}.",
                            Thread.currentThread().getName(),
                            oid,
                            event.getErrorMessage()
                        );
                        // Depending on the severity, you might want to fail the split or skip this OID.
                        continue;
                    }
                    if (event.getColumns() != null) {
                        for (VariableBinding vb : event.getColumns()) {
                            if (vb != null && vb.getOid() != null) { // Ensure OID is not null
                                processVariableBinding(agentInfo.getHost(), agentInfo.getPort(), vb, collector);
                            }
                        }
                    }
                }
            }
        } else {
            LOG.error("{} SnmpSourceReader: sendSnmpRequest() Unsupported poll mode: {}.",
                Thread.currentThread().getName(),
                agentInfo.getPollMode()
            );

            throw new IllegalArgumentException("Unsupported poll mode: " + agentInfo.getPollMode());
        }
    }

    private void processVariableBinding(String deviceId, int port, VariableBinding vb, Collector<RowData> collector) {
        String metricOid          = vb.getOid().toString();
        String metricValue        = vb.getVariable().toString();
        String dataType           = vb.getVariable().getSyntaxString(); // Get human-readable type
        String instanceIdentifier = ""; // For scalar, this is empty. For table, this is the index.

        // Extract instance identifier if it's a table OID
        if (vb.getOid().size() > metricOid.length()) { // Check if OID has an instance suffix
            // This is a simplistic way to get instance ID. More robust parsing might be needed
            // depending on how specific OIDs are structured.
            instanceIdentifier = vb.getOid().toString().substring(metricOid.length());
            if (instanceIdentifier.startsWith(".")) {
                instanceIdentifier = instanceIdentifier.substring(1);
            }
        }

        LocalDateTime ts = LocalDateTime.now(); // Current timestamp

        LOG.debug("{} SnmpSourceReader: sendSnmpRequest() Processing VB - Device: {}:{}, OID: {}, Value: {}, Type: {}.",
            Thread.currentThread().getName(),
            deviceId,
            port,
            metricOid,
            metricValue,
            dataType
        );

        // Create RowData based on the schema
        GenericRowData row = new GenericRowData(producedRowType.getFieldCount());

        int fieldIndex = 0;
        for (RowType.RowField field : producedRowType.getFields()) {
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();

            switch (fieldName) {
                case "device_id":
                    row.setField(fieldIndex, StringData.fromString(deviceId + ":" + port));
                    break;

                case "metric_oid":
                    row.setField(fieldIndex, StringData.fromString(metricOid));
                    break;
                case "metric_value":
                    row.setField(fieldIndex, StringData.fromString(metricValue));
                    break;

                case "data_type":
                    row.setField(fieldIndex, StringData.fromString(dataType));
                    break;

                case "instance_identifier":
                    row.setField(fieldIndex, StringData.fromString(instanceIdentifier));
                    break;

                case "ts":
                    // Convert LocalDateTime to a format compatible with Flink's TIMESTAMP(3)
                    // Flink's RowData expects internal representation for timestamps,
                    // which is typically milliseconds since epoch or similar.
                    // For simplicity, we convert to a StringData representation here,
                    // assuming a downstream sink can parse it.
                    // For true TIMESTAMP(3) type, you'd use TimestampData.fromLocalDateTime(ts)
                    // and ensure the schema truly expects TIMESTAMP(3) and not VARCHAR.
                    
                    row.setField(fieldIndex, StringData.fromString(ts.format(DATE_TIME_FORMATTER)));
                    break;

                case "PROC_TIME": 
                    // Process time field, can be ignored or set to current processing time
                    // Flink's SQL API typically handles PROCTIME() automatically.
                    // If you need to set it here, it's usually `null` for source unless
                    // it's a generated column from event time or a specific timestamp.
                    row.setField(fieldIndex, null); // Or TimestampData.fromLocalDateTime(LocalDateTime.now());
                    break;

                default:
                    row.setField(fieldIndex, null); // Set null for any unmapped fields
                    break;
            }
            fieldIndex++;
        }
        collector.collect(row);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {

        LOG.debug("{} isAvailable called:", 
            Thread.currentThread().getName() 
        );

        // If there are fetched records, we are immediately available
        if (fetchedRecords.peek() != null) {
            return CompletableFuture.completedFuture(null);

        }
        // If no more splits are expected and no current split, then we are not available (end of input)
        if (noMoreSplits.get() && currentAgentInfo == null) {
            return CompletableFuture.completedFuture(null);

        }
        // Otherwise, return the future which will be completed when new data or splits arrive
        return availabilityFuture;
    }

 
    public void onSplitFinished(Collection<String> splitIds) {

        LOG.debug("{} SnmpSourceReader: onSplitFinished() called for splits: {}.",
            Thread.currentThread().getName(),
            splitIds
        );
        // Implement your logic here based on the finished split IDs.
    }

    // Removed @Override as this is not a standard Flink SourceReader override
    public void onRawSplitChanges(List<SnmpSourceSplit> splits) {

        LOG.debug("{} SnmpSourceReader: onRawSplitChanges() called with {} splits.",
            Thread.currentThread().getName(),
            splits.size()
        );
        // This method might be used for advanced scenarios where splits are dynamically added/removed
        // within a running task. For now, simply add them.
        addSplits(splits); // Assuming addSplits is a valid internal method to handle new splits
    }

    // Removed @Override as this is not a standard Flink SourceReader override
    public void onRawStateRestore(List<SnmpSourceSplit> splits) {

        LOG.info("{} SnmpSourceReader: onRawSplitChanges() Restoring state with {} splits. Total pending splits after restore: {}. Attempting to re-assign if necessary.",
            Thread.currentThread().getName(),
            splits.size(),
            this.splits.size()
        );
        // Add restored splits to the current list of splits
        this.splits.clear(); // Clear existing splits, as this is a restore
        this.splits.addAll(splits);

        if (!this.splits.isEmpty() && currentAgentInfo == null) {
            
            LOG.debug("{} SnmpSourceReader: onRawSplitChanges() Called, Assigning a split immediately after state restore as currentAgentInfo is null.",
                Thread.currentThread().getName()
            );
            assignNextSplit();
            lastPollTime = 0; // Trigger immediate poll after restore
        }
    }

    @Override
    public void notifyNoMoreSplits() {

        LOG.debug("{} SnmpSourceReader: notifyNoMoreSplits() called. No more splits expected.",
            Thread.currentThread().getName()
        );


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