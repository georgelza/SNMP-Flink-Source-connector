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
    private String       authProtocol;      // for SNMPv3 authentication protocol
    private String       privProtocol;      // for SNMPv3 privacy protocol
    private String       pollMode;
    private List<String> oids;
    private int          intervalSeconds;
    private int          timeoutSeconds;
    private int          retries;
    private String       deviceId;          // Automatically generated unique ID

    /**
     * Constructor for SnmpAgentInfo.
     *
     * @param host            The hostname or IP address of the SNMP agent.
     * @param port            The port number of the SNMP agent (default 161).
     * @param snmpVersion     The SNMP version (e.g., "SNMPv2c", "SNMPv1", "SNMPv3").
     * @param communityString The community string for SNMPv1/v2c.
     * @param userName        The username for SNMPv3 authentication.
     * @param password        The password for SNMPv3 authentication.
     * @param authProtocol    The password for SNMPv3 authentication.
     * @param privProtocol    The SNMPv3 privacy protocol (e.g., "DES", "AES", "NONE").
     * @param pollMode        The polling mode ("GET" or "WALK").
     * @param oids            A list of OIDs to poll.
     * @param intervalSeconds The polling interval in seconds.
     * @param timeoutSeconds  The SNMP request timeout in seconds.
     * @param retries         The number of retries for SNMP requests.
     */
    public SnmpAgentInfo(
        String host,
        int port,
        String snmpVersion,
        String communityString,
        String userName, 
        String password, 
        String authProtocol, 
        String privProtocol, 
        String pollMode,
        List<String> oids,
        int intervalSeconds,
        int timeoutSeconds,
        int retries)
    {
        this.host               = Objects.requireNonNull(host, "Host cannot be null");
        this.port               = port;
        this.snmpVersion        = Objects.requireNonNull(snmpVersion, "SNMP Version cannot be null");
        this.communityString    = communityString;
        this.userName           = userName; 
        this.password           = password; 
        this.authProtocol       = authProtocol; 
        this.authProtocol       = privProtocol; 
        this.pollMode           = Objects.requireNonNull(pollMode, "Poll Mode cannot be null");
        this.oids               = Objects.requireNonNull(oids, "OIDs cannot be null");
        this.intervalSeconds    = intervalSeconds;
        this.timeoutSeconds     = timeoutSeconds;
        this.retries            = retries;
        this.deviceId           = generateDeviceId(); // Generate unique ID based on agent info

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

    public String getAuthProtocol() { // New getter
        return authProtocol;
    }

    public String getPrivProtocol() { // New getter
        return privProtocol;
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

    public String getDeviceId() {
        return deviceId;
    }

    // Setters (if needed for checkpointing, but constructor is preferred for immutability)
    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setSnmpVersion(String snmpVersion) {
        this.snmpVersion = snmpVersion;
    }

    public void setCommunityString(String communityString) {
        this.communityString = communityString;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setAuthProtocol(String authProtocol) { // New setter
        this.authProtocol = authProtocol;
    }

    public void setPrivProtocol(String privProtocol) { // New setter
        this.privProtocol = privProtocol;
    }

    public void setPollMode(String pollMode) {
        this.pollMode = pollMode;
    }

    public void setOids(List<String> oids) {
        this.oids = oids;
    }

    public void setIntervalSeconds(int intervalSeconds) {
        this.intervalSeconds = intervalSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }


    private String generateDeviceId() {
        // A simple way to generate a unique ID for the device based on its connection parameters.
        // This ensures the deviceId is consistent across restarts for the same agent.
        return String.format("snmp-%s-%d-%s", host, port, snmpVersion);
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
                Objects.equals(userName, that.userName) && 
                Objects.equals(password, that.password) && 
                Objects.equals(authProtocol, that.authProtocol) && 
                Objects.equals(privProtocol, that.privProtocol) && 
                pollMode.equals(that.pollMode) &&
                oids.equals(that.oids) &&
                deviceId.equals(that.deviceId);
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
            authProtocol, 
            privProtocol,
            pollMode, 
            oids, 
            intervalSeconds, 
            timeoutSeconds, 
            retries,
            deviceId);
    }

    @Override
    public String toString() {
        return "SnmpAgentInfo{" +
                " host='"               + host + '\'' +
                ",port="                + port +
                ",snmpVersion='"        + snmpVersion + '\'' +
                ",communityString='"    + "*********" + '\'' +
                ",userName='"           + userName + '\'' +    
                ",password='"           + "*********" + '\'' + 
                ",authProtocol='"       + authProtocol + '\'' + 
                ",privProtocol='"       + privProtocol + '\'' +
                ",pollMode='"           + pollMode + '\'' +
                ",oids="                + oids +
                ",intervalSeconds= "    + intervalSeconds +
                ",timeoutSeconds="      + timeoutSeconds +
                ",retries="             + retries +
                ",deviceId='"           + deviceId + '\'' +
                '}';
    }
}