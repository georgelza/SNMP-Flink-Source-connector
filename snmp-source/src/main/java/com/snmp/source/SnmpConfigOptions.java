/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpConfigOptions.java
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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.util.List;
import java.util.stream.Collectors;
import java.util.Arrays;

/**
 * Configuration options for the SNMP Flink source connector.
 * These options are used to configure the SNMP polling behavior.
 */
public class SnmpConfigOptions {

    public static final ConfigOption<String> TARGET_AGENTS = ConfigOptions
            .key("target")
            .stringType()
            .noDefaultValue()
            .withDescription("Comma-separated list of SNMP agent addresses (IP:Port).");

    public static final ConfigOption<String> SNMP_VERSION = ConfigOptions
            .key("snmp.version")
            .stringType()
            .defaultValue("SNMPv2c")
            .withDescription("SNMP version to use (SNMPv1, SNMPv2c, SNMPv3).");

    public static final ConfigOption<String> SNMP_COMMUNITY_STRING = ConfigOptions
            .key("snmp.community-string")
            .stringType()
            .defaultValue("public")
            .withDescription("SNMP community string for SNMPv1/v2c agents.");

    public static final ConfigOption<String> SNMP_USERNAME = ConfigOptions
            .key("snmp.username")
            .stringType()
            .noDefaultValue()
            .withDescription("SNMPv3 username.");

    public static final ConfigOption<String> SNMP_PASSWORD = ConfigOptions
            .key("snmp.password")
            .stringType()
            .noDefaultValue()
            .withDescription("SNMPv3 authentication password.");

    public static final ConfigOption<String> SNMP_POLL_MODE = ConfigOptions
            .key("snmp.poll_mode")
            .stringType()
            .defaultValue("GET")
            .withDescription("SNMP poll mode (GET or WALK).");

    public static final ConfigOption<String> OIDS = ConfigOptions
            .key("oids")
            .stringType()
            .noDefaultValue()
            .withDescription("Comma-separated list of OIDs for GET mode, or a single root OID for WALK mode.");

    public static final ConfigOption<Integer> INTERVAL = ConfigOptions
            .key("interval_seconds")
            .intType()
            .defaultValue(10)
            .withDescription("Polling interval in seconds.");

    public static final ConfigOption<Integer> TIMEOUT = ConfigOptions
            .key("timeout_seconds")
            .intType()
            .defaultValue(5)
            .withDescription("SNMP request timeout in seconds.");

    public static final ConfigOption<Integer> RETRIES = ConfigOptions
            .key("retries")
            .intType()
            .defaultValue(2)
            .withDescription("Number of SNMP request retries.");

    /**
     * Parses the target agents string into a list of "ip:port" strings.
     * @param config The Flink configuration.
     * @return A list of target agent strings.
     * @throws IllegalArgumentException if TARGET_AGENTS is not set.
     */
    public static List<String> getTargetAgents(ReadableConfig config) {
        String targets = config.getOptional(TARGET_AGENTS)
                .orElseThrow(() -> new IllegalArgumentException("SNMP 'target' property must be specified."));
        return Arrays.stream(targets.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    /**
     * Parses the OIDs string into a list of OID strings.
     * @param config The Flink configuration.
     * @return A list of OID strings.
     * @throws IllegalArgumentException if OIDS is not set.
     */
    public static List<String> getOids(ReadableConfig config) {
        String oids = config.getOptional(OIDS)
                .orElseThrow(() -> new IllegalArgumentException("SNMP 'oids' property must be specified."));
        return Arrays.stream(oids.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}
