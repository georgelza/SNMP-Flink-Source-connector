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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.ArrayList; // Import ArrayList
import java.util.List;

/**
 * A serializable class to hold information about an SNMP agent.
 * This class will be used in {@link SnmpSourceSplit} to define
 * the target for a specific polling task.
 */
public class SnmpAgentInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    // Removed 'final' keyword from all fields
    private String    host;
    private int       port;
    private String    snmpVersion;
    private String    communityString;
    private String    username; // For SNMPv3
    private String    password; // For SNMPv3
    private String    pollMode; // GET or WALK
    private int       intervalSeconds;
    private int       timeoutSeconds;
    private int       retries;
    private List<String> oids; // List of OIDs for polling

    // No-argument constructor for serialization
    public SnmpAgentInfo() {
        // Initialize list to avoid NullPointerException during deserialization if no oids are set
        this.oids = new ArrayList<>();
    }

    /**
     * Constructs a new SnmpAgentInfo object.
     *
     * @param host            The hostname or IP address of the SNMP agent.
     * @param port            The port number of the SNMP agent (default is 161).
     * @param snmpVersion     The SNMP version to use (e.g., "SNMPv1", "SNMPv2c", "SNMPv3").
     * @param communityString The SNMP community string for SNMPv1/v2c.
     * @param username        The username for SNMPv3 authentication.
     * @param password        The password for SNMPv3 authentication.
     * @param pollMode        The polling mode ("GET" or "WALK").
     * @param oids            A list of OIDs to poll.
     * @param intervalSeconds The polling interval in seconds.
     * @param timeoutSeconds  The timeout for SNMP requests in seconds.
     * @param retries         The number of retries for SNMP requests.
     */
    public SnmpAgentInfo(
            String host,
            int port,
            String snmpVersion,
            String communityString,
            String username,
            String password,
            String pollMode,
            List<String> oids,
            int intervalSeconds,
            int timeoutSeconds,
            int retries) {
        this.host               = Objects.requireNonNull(host, "Host cannot be null");
        this.port               = port;
        this.snmpVersion        = Objects.requireNonNull(snmpVersion, "SNMP version cannot be null");
        this.communityString    = communityString;
        this.username           = username;
        this.password           = password;
        this.pollMode           = Objects.requireNonNull(pollMode, "Poll mode cannot be null");
        this.oids               = new ArrayList<>(Objects.requireNonNull(oids, "OIDs list cannot be null")); // Initialize with ArrayList
        this.intervalSeconds    = intervalSeconds;
        this.timeoutSeconds     = timeoutSeconds;
        this.retries            = retries;
    }

    // Getters
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
        return new ArrayList<>(oids); // Return a copy to prevent external modification
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

    // Custom serialization
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeUTF(host);
        out.writeInt(port);
        out.writeUTF(snmpVersion);
        out.writeUTF(communityString);
        out.writeUTF(username);
        out.writeUTF(password);
        out.writeUTF(pollMode);
        out.writeInt(intervalSeconds);
        out.writeInt(timeoutSeconds);
        out.writeInt(retries);
        out.writeObject(new ArrayList<>(oids)); // Write a serializable copy of the list
    }

    // Custom deserialization
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        host            = in.readUTF();
        port            = in.readInt();
        snmpVersion     = in.readUTF();
        communityString = in.readUTF();
        username        = in.readUTF();
        password        = in.readUTF();
        pollMode        = in.readUTF();
        intervalSeconds = in.readInt();
        timeoutSeconds  = in.readInt();
        retries         = in.readInt();
        oids            = (List<String>) in.readObject(); // Read the list
    }


    @Override
    public boolean equals(Object o) {
        
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnmpAgentInfo that = (SnmpAgentInfo) o;
        // Compare all fields for equality
        return port             == that.port &&
                intervalSeconds == that.intervalSeconds &&
                timeoutSeconds  == that.timeoutSeconds &&
                retries         == that.retries &&
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
                  "host='"          + host + '\'' +
                ", port="           + port +
                ", snmpVersion='"   + snmpVersion + '\'' +
                ", pollMode='"      + pollMode + '\'' +
                ", oids="           + oids +
                '}';
    }
}