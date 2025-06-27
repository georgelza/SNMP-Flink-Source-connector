/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpTableSourceFactory.java
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
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * A {@link DynamicTableSourceFactory} for the SNMP source connector.
 * This factory is responsible for creating a {@link SnmpTableSource} from table properties.
 */
public class SnmpTableSourceFactory implements DynamicTableSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpTableSourceFactory.class);

    public static final String IDENTIFIER = "snmp";

    @Override
    public String factoryIdentifier() {
        LOG.debug("{}: factoryIdentifier called, identifier={}.", 
            Thread.currentThread().getName(),
            IDENTIFIER
        );
        // Return the identifier for this factory
        // This identifier is used to register the factory in the Flink environment
        return IDENTIFIER;
    }

    // Must Have's
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SnmpConfigOptions.TARGET_AGENTS);
        options.add(SnmpConfigOptions.SNMP_POLL_MODE);
        options.add(SnmpConfigOptions.OIDS);

        return options;
    }

    //Optionals
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SnmpConfigOptions.SNMP_VERSION);
        options.add(SnmpConfigOptions.SNMP_COMMUNITY_STRING);
        options.add(SnmpConfigOptions.SNMP_USERNAME);
        options.add(SnmpConfigOptions.SNMP_PASSWORD);
        options.add(SnmpConfigOptions.INTERVAL);
        options.add(SnmpConfigOptions.TIMEOUT);
        options.add(SnmpConfigOptions.RETRIES);
        
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        final java.util.List<String> targetAgents       = SnmpConfigOptions.getTargetAgents(helper.getOptions());
        final String                 snmpVersion        = helper.getOptions().get(SnmpConfigOptions.SNMP_VERSION);
        final String                 communityString    = helper.getOptions().get(SnmpConfigOptions.SNMP_COMMUNITY_STRING);
        final String                 username           = helper.getOptions().getOptional(SnmpConfigOptions.SNMP_USERNAME).orElse(null);
        final String                 password           = helper.getOptions().getOptional(SnmpConfigOptions.SNMP_PASSWORD).orElse(null);
        final String                 pollMode           = helper.getOptions().get(SnmpConfigOptions.SNMP_POLL_MODE);
        final java.util.List<String> oids               = SnmpConfigOptions.getOids(helper.getOptions());
        final int                    intervalSeconds    = helper.getOptions().get(SnmpConfigOptions.INTERVAL);
        final int                    timeoutSeconds     = helper.getOptions().get(SnmpConfigOptions.TIMEOUT);
        final int                    retries            = helper.getOptions().get(SnmpConfigOptions.RETRIES);
        final DataType               producedDataType   = context.getPhysicalRowDataType();

        // Log the input parameters
        LOG.debug("{} Called, Creating SNMP Table Source with parameters:", 
            Thread.currentThread().getName());
        LOG.debug("    Target Agents:                {}", targetAgents);
        LOG.debug("    SNMP Version:                 {}", snmpVersion);
        LOG.debug("    Community String:             {}", "******"); // Mask sensitive information
        LOG.debug("    Username:                     {}", username);
        LOG.debug("    Password:                     {}", "******"); // Mask sensitive information
        LOG.debug("    Poll Mode:                    {}", pollMode);
        LOG.debug("    OIDs:                         {}", oids);
        LOG.debug("    Interval:                     {} Seconds", intervalSeconds);
        LOG.debug("    Timeout:                      {} Seconds", timeoutSeconds);
        LOG.debug("    Retries:                      {}", retries);

        return new SnmpTableSource(
                targetAgents,
                snmpVersion,
                communityString,
                username,
                password,
                pollMode,
                oids,
                intervalSeconds,
                timeoutSeconds,
                retries,
                producedDataType);
    }
}
