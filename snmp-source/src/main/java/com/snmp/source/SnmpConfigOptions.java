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
public static final ConfigOption<String> TARGET = ConfigOptions //
        .key("target") //
        .stringType() //
        .noDefaultValue() //
        .withDescription("A comma-separated list of target SNMP agents' addresses and ports (e.g., \"192.168.1.10:161, 192.168.1.11:162\")."); //

/**
 * The SNMP version to use (e.g., "SNMPv2c", "SNMPv1", "SNMPv3").
 * Required.
 */
public static final ConfigOption<String> SNMP_VERSION = ConfigOptions
        .key("snmp.version")
        .stringType()
        .defaultValue("SNMPv2c") // Default to SNMPv2c
        .withDescription("The SNMP version to use (e.g., 'SNMPv2c', 'SNMPv1', 'SNMPv3'). Defaults to 'SNMPv2c'.");

/**
 * The community string for SNMPv1/v2c (e.g., "public").
 * Required for SNMPv1/v2c.
 */
public static final ConfigOption<String> SNMP_COMMUNITY_STRING = ConfigOptions //
        .key("snmp.community-string") //
        .stringType() //
        .defaultValue("public") // Default to "public" //
        .withDescription("The community string for SNMPv1/v2c. Defaults to 'public'."); //

/**
 * The username for SNMPv3 authentication.
 * Optional for SNMPv3.
 */
public static final ConfigOption<String> SNMP_USERNAME = ConfigOptions
        .key("snmp.username")
        .stringType()
        .noDefaultValue()
        .withDescription("The username for SNMPv3 authentication. Optional.");

/**
 * The password for SNMPv3 authentication.
 * Optional for SNMPv3.
 */
public static final ConfigOption<String> SNMP_PASSWORD = ConfigOptions
        .key("snmp.password")
        .stringType()
        .noDefaultValue()
        .withDescription("The password for SNMPv3 authentication. Optional.");

/**
 * The polling mode ("GET" or "WALK").
 * Required.
 */
public static final ConfigOption<String> SNMP_POLL_MODE = ConfigOptions
        .key("snmp.poll_mode") //
        .stringType() //
        .defaultValue("GET") // Default to "GET" //
        .withDescription("The SNMP polling mode ('GET' or 'WALK'). Defaults to 'GET'."); //

/**
 * A comma-separated list of OIDs (Object Identifiers) to poll.
 * Required.
 */
public static final ConfigOption<String> OIDS = ConfigOptions //
        .key("oids") //
        .stringType() //
        .noDefaultValue() //
        .withDescription("A comma-separated list of OIDs (Object Identifiers) to poll (e.g., '.1.3.6.1.2.1.1.1.0, .1.3.6.1.2.1.1.5.0')."); //

/**
 * The interval (in seconds) between successive polls to the SNMP agent.
 * Required.
 */
public static final ConfigOption<Integer> SNMP_INTERVAL_SECONDS = ConfigOptions
        .key("snmp.interval-seconds")
        .intType()
        .defaultValue(5) // Default to 5 seconds
        .withDescription("The interval (in seconds) between successive polls to the SNMP agent. Defaults to 5.");

/**
 * The timeout (in seconds) for SNMP requests.
 * Required.
 */
public static final ConfigOption<Integer> SNMP_TIMEOUT_SECONDS = ConfigOptions
        .key("snmp.timeout-seconds")
        .intType()
        .defaultValue(2) // Default to 2 seconds
        .withDescription("The timeout (in seconds) for SNMP requests. Defaults to 2.");

/**
 * The number of retries for SNMP requests if the first attempt fails.
 * Required.
 */
public static final ConfigOption<Integer> SNMP_RETRIES = ConfigOptions
        .key("snmp.retries")
        .intType()
        .defaultValue(1) // Default to 1 retry
        .withDescription("The number of retries for SNMP requests if the first attempt fails. Defaults to 1.");
}