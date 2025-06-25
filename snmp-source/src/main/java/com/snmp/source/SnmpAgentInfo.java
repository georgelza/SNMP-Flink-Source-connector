/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpAgentInfo.java
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

import java.io.Serializable;
import java.util.Objects;
import java.util.List;

/**
 * A serializable class to hold information about an SNMP agent.
 * This class will be used in {@link SnmpSourceSplit} to define
 * the target for a specific polling task.
 */
public class SnmpAgentInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String    host;
    private final int       port;
    private final String    snmpVersion;
    private final String    communityString;
    private final String    username; // For SNMPv3
    private final String    password; // For SNMPv3
    private final String    pollMode; // GET or WALK
    private final int       intervalSeconds;
    private final int       timeoutSeconds;
    private final int       retries;
    private final List<String> oids; // List of OIDs for GET, or single root OID for WALK

    /**
     * Constructor for SNMP agent information.
     *
     * @param host              The IP address or hostname of the SNMP agent.
     * @param port              The port of the SNMP agent (default 161).
     * @param snmpVersion       The SNMP version (e.g., "SNMPv1", "SNMPv2c", "SNMPv3").
     * @param communityString   The community string for SNMPv1/v2c.
     * @param username          The username for SNMPv3.
     * @param password          The password for SNMPv3.
     * @param pollMode          The polling mode ("GET" or "WALK").
     * @param oids              A list of OIDs (for GET) or a single root OID (for WALK).
     * @param intervalSeconds   Polling interval in seconds.
     * @param timeoutSeconds    SNMP request timeout in seconds.
     * @param retries           Number of SNMP request retries.
     */
    public SnmpAgentInfo(
            String          host,
            int             port,
            String          snmpVersion,
            String          communityString,
            String          username,
            String          password,
            String          pollMode,
            List<String>    oids,
            int             intervalSeconds,
            int             timeoutSeconds,
            int             retries
        ) {
        this.host            = host;
        this.port            = port;
        this.snmpVersion     = snmpVersion;
        this.communityString = communityString;
        this.username        = username;
        this.password        = password;
        this.pollMode        = pollMode;
        this.oids            = oids;
        this.intervalSeconds = intervalSeconds;
        this.timeoutSeconds  = timeoutSeconds;
        this.retries         = retries;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getSnmpVersion() {
        return snmpVersion;
    }

    public String getCommunityString() {
        return communityString;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getPollMode() {
        return pollMode;
    }

    public List<String> getOids() {
        return oids;
    }

    public int getIntervalSeconds() {
        return intervalSeconds;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public int getRetries() {
        return retries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnmpAgentInfo that = (SnmpAgentInfo) o;
        return port                 == that.port &&
                intervalSeconds     == that.intervalSeconds &&
                timeoutSeconds      == that.timeoutSeconds &&
                retries             == that.retries &&
                host.equals(that.host) &&
                snmpVersion.equals(that.snmpVersion) &&
                Objects.equals(communityString, that.communityString) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password) &&
                pollMode.equals(that.pollMode) &&
                oids.equals(that.oids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
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
    }

    @Override
    public String toString() {
        return "SnmpAgentInfo{"     +
                "host='"            + host + '\'' +
                ", port="           + port +
                ", snmpVersion='"   + snmpVersion + '\'' +
                ", pollMode='"      + pollMode + '\'' +
                ", oids="           + oids +
                '}';
    }
}
