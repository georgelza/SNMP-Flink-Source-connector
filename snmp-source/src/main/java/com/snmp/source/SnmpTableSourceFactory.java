/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpTableSourceFactory.java
/
/       Description     :   SNMP Source connector
/                       :   This will be executed via the Create Table as per CreFlinkFlows / snmp_poll_data examples
/                       :   This will create a table with a connector=snmp defined.
/                       :   Table variables extracted via the SnmpConfigOptions.java
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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 * A {@link DynamicTableSourceFactory} for the SNMP source connector.
 * This factory is responsible for creating a {@link SnmpTableSource} from table properties.
 */
public class SnmpTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "snmp"; // The connector identifier used in 'connector'='snmp'

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SnmpConfigOptions.TARGET_AGENTS);
        options.add(SnmpConfigOptions.SNMP_POLL_MODE);
        options.add(SnmpConfigOptions.OIDS);
        return options;
    }

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

        // Validate all options are properly used and not conflicting.
        helper.validate();

        // Get the produced data type
        final DataType producedDataType = context.getPhysicalRowDataType();

        // Extract configuration values
        final String snmpVersion        = helper.getOptions().get(SnmpConfigOptions.SNMP_VERSION);
        final String pollMode           = helper.getOptions().get(SnmpConfigOptions.SNMP_POLL_MODE);
        final String communityString    = helper.getOptions().get(SnmpConfigOptions.SNMP_COMMUNITY_STRING);
        final String username           = helper.getOptions().getOptional(SnmpConfigOptions.SNMP_USERNAME).orElse(null);
        final String password           = helper.getOptions().getOptional(SnmpConfigOptions.SNMP_PASSWORD).orElse(null);
        final int   intervalSeconds     = helper.getOptions().get(SnmpConfigOptions.INTERVAL);
        final int   timeoutSeconds      = helper.getOptions().get(SnmpConfigOptions.TIMEOUT);
        final int   retries             = helper.getOptions().get(SnmpConfigOptions.RETRIES);

        // Get target agents and OIDs using the helper methods in SnmpConfigOptions
        final java.util.List<String> targetAgents   = SnmpConfigOptions.getTargetAgents(helper.getOptions());
        final java.util.List<String> oids           = SnmpConfigOptions.getOids(helper.getOptions());

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
                producedDataType
        );
    }
}
