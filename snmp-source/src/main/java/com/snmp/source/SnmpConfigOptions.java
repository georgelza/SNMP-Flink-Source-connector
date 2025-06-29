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

import java.util.List;

/**
 * Defines the configuration options for the Flink SNMP Source Connector.
 * These options correspond to the properties that can be set in the 'WITH' clause
 * of a Flink SQL CREATE TABLE statement.
 * This class consolidates all connector-specific configuration keys.
 */
public class SnmpConfigOptions {

// Prevent instantiation
private SnmpConfigOptions() {}

/**
 * Prefix for connector-specific properties in flink-conf.yaml if you were to use
 * them outside of a WITH clause for a table. Not directly used in WITH clause parsing.
 */
public static final String PROPERTIES_PREFIX = "snmp.";

/**
 * The target SNMP agent's address and port (e.g., "192.168.1.10:161").
 * This is a required option.
 */
public static final ConfigOption<String> TARGET = ConfigOptions
        .key("target")
        .stringType()
        .noDefaultValue()
        .withDescription("The target SNMP agent's host and port (e.g., '192.168.1.10:161').");

/**
 * The SNMP version to use (e.g., "SNMPv1", "SNMPv2c", "SNMPv3").
 * This is a required option.
 */
public static final ConfigOption<String> SNMP_VERSION = ConfigOptions
        .key("snmp.version")
        .stringType()
        .defaultValue("SNMPv2c") // Common default
        .withDescription("The SNMP version to use (e.g., 'SNMPv1', 'SNMPv2c', 'SNMPv3'). Defaults to SNMPv2c.");

/**
 * The community string for SNMPv1/v2c agents.
 * This is a required option.
 */
public static final ConfigOption<String> SNMP_COMMUNITY_STRING = ConfigOptions
        .key("snmp.community-string")
        .stringType()
        .noDefaultValue()
        .withDescription("The community string for SNMPv1/v2c agents (e.g., 'public').");

/**
 * The SNMPv3 user name for authentication.
 * This is a required option for SNMPv3.
 */
public static final ConfigOption<String> SNMP_USERNAME = ConfigOptions
        .key("snmp.username")
        .stringType()
        .noDefaultValue()
        .withDescription("The SNMPv3 user name for authentication. Required for SNMPv3 agents.");

public static final ConfigOption<String> SNMP_PASSWORD = ConfigOptions
        .key("snmp.password")
        .stringType()
        .noDefaultValue()
        .withDescription("The SNMPv3 password for authentication. Required for SNMPv3 agents.");

/**
 * The polling mode: "GET" for specific OIDs or "WALK" for subtree traversal.
 * This is a required option.
 */
public static final ConfigOption<String> SNMP_POLL_MODE = ConfigOptions
        .key("snmp.poll_mode")
        .stringType()
        .defaultValue("GET")    // Common default
        .withDescription("The polling mode: 'GET' for specific OIDs or 'WALK' for subtree traversal. Defaults to GET.");

/**
 * A comma-separated list of OIDs (Object Identifiers) to poll.
 * Required.
 */
public static final ConfigOption<String> OIDS = ConfigOptions // MODIFIED: Changed from List<String> to String
        .key("oids")
        .stringType()
        .noDefaultValue() // MODIFIED: Removed .asList()
        .withDescription("A comma-separated list of OIDs (Object Identifiers) to poll (e.g., '.1.3.6.1.2.1.1.1.0, .1.3.6.1.2.1.1.5.0').");

/**
 * The interval (in seconds) between successive polls to the SNMP agent.
 * Required.
 */
public static final ConfigOption<Integer> SNMP_INTERVAL_SECONDS = ConfigOptions
        .key("snmp.interval-seconds")
        .intType()
        .defaultValue(5)        // Default to 5 seconds
        .withDescription("The interval (in seconds) between successive polls to the SNMP agent. Defaults to 5.");

/**
 * The timeout (in seconds) for SNMP requests.
 * Required.
 */
public static final ConfigOption<Integer> SNMP_TIMEOUT_SECONDS = ConfigOptions
        .key("snmp.timeout-seconds")
        .intType()
        .defaultValue(2)        // Default to 2 seconds
        .withDescription("The timeout (in seconds) for SNMP requests. Defaults to 2.");

/**
 * The number of retries for SNMP requests if the first attempt fails.
 * Required.
 */
public static final ConfigOption<Integer> SNMP_RETRIES = ConfigOptions
        .key("snmp.retries")
        .intType()
        .defaultValue(1)        // Default to 1 retry
        .withDescription("The number of retries for SNMP requests if the first attempt fails. Defaults to 1.");
}