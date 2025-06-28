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
/       GIT Repo        :   https://github.com/georgelza/SNMP-Flink-Source-connector
/
/       Blog            :
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.source;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Represents the configuration information for a single SNMP agent
 * that the Flink SNMP Source connector will poll.
 * This class is {@link Serializable} to allow it to be checkpointed by Flink.
 */
public class SnmpAgentInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String       host;
    private int          port;
    private String       snmpVersion;
    private String       communityString;
    private String       userName;
    private String       password;
    private String       pollMode;
    private List<String> oids;
    private int          intervalSeconds;
    private int          timeoutSeconds;
    private int          retries;

    /**
     * Constructor for SnmpAgentInfo.
     *
     * @param host              The hostname or IP address of the SNMP agent.
     * @param port              The UDP port of the SNMP agent (usually 161).
     * @param snmpVersion       The SNMP version to use (e.g., "SNMPv1", "SNMPv2c", "SNMPv3").
     * @param communityString   The community string for SNMPv1/v2c agents.
     * @param userName          The username for SNMPv3 agents.
     * @param password          The password for SNMPv3 agents.
     * @param pollMode          The polling mode ("GET" or "WALK").
     * @param oids              A list of OIDs to poll or walk.
     * @param intervalSeconds   The polling interval in seconds.
     * @param timeoutSeconds    The timeout for SNMP requests in seconds.
     * @param retries           The number of retries for SNMP requests.
     */
    public SnmpAgentInfo(
            String       host,
            int          port,
            String       snmpVersion,
            String       communityString,
            String       userName,
            String       password,
            String       pollMode,
            List<String> oids,
            int          intervalSeconds,
            int          timeoutSeconds,
            int          retries) {
                
        this.host            = Objects.requireNonNull(host, "Host cannot be null.");
        this.port            = port;
        this.snmpVersion     = Objects.requireNonNull(snmpVersion, "SNMP Version cannot be null.");
        this.communityString = communityString;
        this.userName        = userName;
        this.password        = password;
        this.pollMode        = Objects.requireNonNull(pollMode, "Poll mode cannot be null.");
        this.oids            = Objects.requireNonNull(oids, "OIDs list cannot be null.");
        this.intervalSeconds = intervalSeconds;
        this.timeoutSeconds  = timeoutSeconds;
        this.retries         = retries;
    }

    // Getters for all fields
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

    public String getUserName() {
        return userName;
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
        
        return port == that.port &&
                intervalSeconds == that.intervalSeconds &&
                timeoutSeconds == that.timeoutSeconds &&
                retries == that.retries &&
                host.equals(that.host) &&
                snmpVersion.equals(that.snmpVersion) &&
                communityString.equals(that.communityString) &&
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
            userName,
            password,
            pollMode, 
            oids, 
            intervalSeconds, 
            timeoutSeconds, 
            retries);
    }

    @Override
    public String toString() {
        return "SnmpAgentInfo{" +
                "    host=            '" + host + '\'' +
                "   ,port=             " + port +
                "   ,snmpVersion=     '" + snmpVersion + '\'' +
                "   ,communityString= '" + "*********" + '\'' +
                "   ,userName=        '" + userName + '\'' +    
                "   ,password=        '" + "*********" + '\'' +
                "   ,pollMode=        '" + pollMode + '\'' +
                "   ,oids=             " + oids +
                "   ,intervalSeconds=  " + intervalSeconds +
                "   ,timeoutSeconds=   " + timeoutSeconds +
                "   ,retries=          " + retries +
                '}';
    }
}
