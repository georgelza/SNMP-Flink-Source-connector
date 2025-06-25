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
/       GIT Repo        :   
/
/       Blog            :   
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

/**
 * A {@link SourceSplit} for the SNMP source. Each split represents a single SNMP agent
 * that a source reader will be responsible for polling.
 */
public class SnmpSourceSplit implements SourceSplit {

    private final String splitId;
    private final SnmpAgentInfo agentInfo;

    /**
     * Constructs a new SnmpSourceSplit.
     *
     * @param splitId A unique identifier for this split.
     * @param agentInfo The SNMP agent information associated with this split.
     */
    public SnmpSourceSplit(String splitId, SnmpAgentInfo agentInfo) {
        this.splitId = splitId;
        this.agentInfo = agentInfo;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public SnmpAgentInfo getAgentInfo() {
        return agentInfo;
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
        return "SnmpSourceSplit{"   +
                    "splitId='"     + splitId + '\'' +
                    ", agentInfo="  + agentInfo +
                '}';
    }
}
