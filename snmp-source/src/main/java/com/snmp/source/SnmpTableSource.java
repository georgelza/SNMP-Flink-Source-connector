/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpTableSource.java
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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Flink Table API {@link DynamicTableSource} for SNMP data.
 * This class describes how to read data from the SNMP agents.
 */
public class SnmpTableSource implements ScanTableSource {

    private final DataType      producedDataType;
    private final List<String>  targetAgents;
    private final String        snmpVersion;
    private final String        communityString;
    private final String        username;
    private final String        password;
    private final String        pollMode;
    private final List<String>  oids;
    private final int           intervalSeconds;
    private final int           timeoutSeconds;
    private final int           retries;

    /**
     * Constructor for SnmpTableSource.
     *
     * @param producedDataType  The data type of the rows produced by this source.
     * @param targetAgents      List of SNMP agent addresses (IP:Port).
     * @param snmpVersion       SNMP version (SNMPv1, SNMPv2c, SNMPv3).
     * @param communityString   Community string for SNMPv1/v2c.
     * @param username          SNMPv3 username.
     * @param password          SNMPv3 password.
     * @param pollMode          SNMP poll mode (GET or WALK).
     * @param oids              List of OIDs for GET, or a single root OID (for WALK).
     * @param intervalSeconds   Polling interval in seconds.
     * @param timeoutSeconds    SNMP request timeout in seconds.
     * @param retries           Number of SNMP request retries.
     */
    public SnmpTableSource(
            DataType     producedDataType,
            List<String> targetAgents,
            String       snmpVersion,
            String       communityString,
            String       username,
            String       password,
            String       pollMode,
            List<String> oids,
            int          intervalSeconds,
            int          timeoutSeconds,
            int          retries) {
        this.producedDataType   = producedDataType;
        this.targetAgents       = targetAgents;
        this.snmpVersion        = snmpVersion;
        this.communityString    = communityString;
        this.username           = username;
        this.password           = password;
        this.pollMode           = pollMode;
        this.oids               = oids;
        this.intervalSeconds    = intervalSeconds;
        this.timeoutSeconds     = timeoutSeconds;
        this.retries            = retries;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        List<SnmpAgentInfo> agentInfos = targetAgents.stream().map(target -> {
            String[] parts = target.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 161;
            return new SnmpAgentInfo(
                    host,
                    port,
                    snmpVersion,
                    communityString,
                    username,
                    password,
                    pollMode,
                    oids,
                    intervalSeconds,
                    timeoutSeconds,
                    retries);
        }).collect(Collectors.toList());

        return SourceProvider.of(new SnmpSource(agentInfos));
    }

    @Override
    public DynamicTableSource copy() {
        return new SnmpTableSource(
                producedDataType,
                targetAgents,
                snmpVersion,
                communityString,
                username,
                password,
                pollMode,
                oids,
                intervalSeconds,
                timeoutSeconds,
                retries);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnmpTableSource that = (SnmpTableSource) o;
        return intervalSeconds                  == that.intervalSeconds &&
                timeoutSeconds                  == that.timeoutSeconds &&
                retries                         == that.retries &&
                producedDataType.equals(        that.producedDataType) &&
                targetAgents.equals(            that.targetAgents) &&
                snmpVersion.equals(             that.snmpVersion) &&
                Objects.equals(communityString, that.communityString) &&
                Objects.equals(username,        that.username) &&
                Objects.equals(password,        that.password) &&
                pollMode.equals(that.pollMode) &&
                oids.equals(that.oids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                targetAgents,
                snmpVersion,
                communityString,
                username,
                password,
                pollMode,
                oids,
                intervalSeconds,
                timeoutSeconds,
                retries);
    }

    @Override
    public String asSummaryString() {
        return "SNMP Table Source";
    } 
}
