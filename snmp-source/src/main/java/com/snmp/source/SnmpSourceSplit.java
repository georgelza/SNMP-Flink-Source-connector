/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpSourceSplit.java
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

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable; 
import java.util.Objects;

/**
 * A {@link SourceSplit} for the SNMP source. Each split represents a single SNMP agent
 * that a source reader will be responsible for polling.
 */
public class SnmpSourceSplit implements SourceSplit, Serializable { // Implement Serializable

    private static final long serialVersionUID = 1L;

    // Removed 'final' keyword
    private String splitId;
    private SnmpAgentInfo agentInfo;

    // No-argument constructor for serialization
    public SnmpSourceSplit() {
    }

    /**
     * Constructs a new SnmpSourceSplit.
     *
     * @param splitId   A unique identifier for this split.
     * @param agentInfo The SNMP agent information associated with this split.
     */
    public SnmpSourceSplit(String splitId, SnmpAgentInfo agentInfo) {

        this.splitId   = Objects.requireNonNull(splitId,   "Split ID cannot be null");
        this.agentInfo = Objects.requireNonNull(agentInfo, "AgentInfo cannot be null");
    }

    @Override
    public String splitId() {
        
        return splitId;
    }

    public SnmpAgentInfo getAgentInfo() {
        
        return agentInfo;
    }

    // Custom serialization method
    private void writeObject(ObjectOutputStream out) throws IOException {

        out.writeUTF(splitId);
        out.writeObject(agentInfo); // This will trigger SnmpAgentInfo's custom writeObject
    }

    // Custom deserialization method
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {

        splitId = in.readUTF();
        agentInfo = (SnmpAgentInfo) in.readObject(); // This will trigger SnmpAgentInfo's custom readObject
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnmpSourceSplit that = (SnmpSourceSplit) o;

        return splitId.equals(that.splitId) && agentInfo.equals(that.agentInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, agentInfo);
    }

    @Override
    public String toString() {
        
        return "SnmpSourceSplit{"   + '\'' +
                "   splitId=     '" + splitId + '\'' +
                "  ,agentInfo=    " + agentInfo + '\'' +
                "}";
    }
}