/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/ 
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpDynamicTableSourceFactory.java
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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Arrays; // ADDED: Import for Arrays.asList()
import java.util.stream.Collectors;

import static com.snmp.source.SnmpConfigOptions.*; // Import all static options from the consolidated file

/**
 * A {@link DynamicTableSourceFactory} for the SNMP connector.
 * This factory is responsible for creating a {@link SnmpDynamicTableSource}
 * based on the properties defined in the Flink SQL CREATE TABLE statement's WITH clause.
 */
public class SnmpDynamicTableSourceFactory implements DynamicTableSourceFactory {

    // Static initializer block: This code runs once when the class is loaded.
    // Useful for very early debugging to see if the factory class is being discovered and loaded by Flink.
    static {
        System.out.println("SnmpDynamicTableSourceFactory: Static initializer called. (Direct System.out)");
    }

    /**
     * Constructor for the SNMP Dynamic Table Source Factory.
     * This is called by Flink's plugin mechanism when it needs to create an instance of the factory.
     */
    public SnmpDynamicTableSourceFactory() {
        System.out.println("SnmpDynamicTableSourceFactory: Constructor called. (Direct System.out)");
    }

    /**
     * Returns the identifier of this factory.
     * This identifier is used in the 'connector' property of the CREATE TABLE statement.
     * For example, 'connector' = 'snmp'.
     *
     * @return The string identifier for this factory.
     */
    @Override
    public String factoryIdentifier() {
        System.out.println("SnmpDynamicTableSourceFactory: factoryIdentifier() called. Returning 'snmp'. (Direct System.out)");
        return "snmp"; // This must match the 'connector' property in CREATE TABLE
    }

    /**
     * Defines the set of required configuration options for this connector.
     * Flink will validate that all these options are present in the CREATE TABLE WITH clause.
     *
     * @return A set of {@link ConfigOption}s that are mandatory.
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        System.out.println("SnmpDynamicTableSourceFactory: requiredOptions() called. (Direct System.out)");
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TARGET);    
        options.add(OIDS);
        options.add(SNMP_POLL_MODE);
        // interval_seconds, timeout_seconds, retries, snmp.version, snmp.community-string
        // are optional due to default values or conditional requirements
        System.out.println("SnmpDynamicTableSourceFactory: Required options: " + options + " (Direct System.out)");
        return options;
    }

    /**
     * Defines the set of optional configuration options for this connector.
     * These options can be provided in the CREATE TABLE WITH clause but are not mandatory.
     *
     * @return A set of {@link ConfigOption}s that are optional.
     */

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        System.out.println("SnmpDynamicTableSourceFactory: optionalOptions() called. (Direct System.out)");
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SNMP_INTERVAL_SECONDS);
        options.add(SNMP_VERSION);
        options.add(SNMP_COMMUNITY_STRING);
        options.add(SNMP_USERNAME);
        options.add(SNMP_PASSWORD);
        options.add(SNMP_TIMEOUT_SECONDS);
        options.add(SNMP_RETRIES);
        return options;
    }
    

    /**
     * Creates a {@link DynamicTableSource} based on the given context.
     * This is the main method where the connector's logic is instantiated.
     *
     * @param context The factory context containing resolved table information and options.
     * @return A new instance of {@link SnmpDynamicTableSource}.
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        System.out.println("SnmpDynamicTableSourceFactory: createDynamicTableSource() called. (Direct System.out)");

        // The ObjectIdentifier represents the full path of the table (catalog.database.table)
        ObjectIdentifier tableIdentifier = context.getObjectIdentifier();
        System.out.println("SnmpDynamicTableSourceFactory: Table identifier: " + tableIdentifier + " (Direct System.out)");

        // The table's schema (columns, types, watermarks)
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        // Corrected: Safely retrieve and log the produced data type
        DataType producedDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        System.out.println("SnmpDynamicTableSourceFactory: Produced data type: " + producedDataType.toString() + " (Direct System.out)");


        // Retrieve and validate options from the WITH clause
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // Validate all options are properly configured
        helper.validateExcept(PROPERTIES_PREFIX); // Validate all options, ignoring those with a specific prefix if any

        // Corrected: Correctly get options from the helper
        final ReadableConfig options = helper.getOptions();
        System.out.println("SnmpDynamicTableSourceFactory: Retrieved ReadableConfig options. (Direct System.out)");

        // Extract connection and polling parameters from options
        String target           = options.get(TARGET);
        String snmpVersion      = options.get(SNMP_VERSION);
        String communityString  = options.get(SNMP_COMMUNITY_STRING);
        String userName         = options.get(SNMP_USERNAME);        // SNMPv3 user name, if applicable
        String password         = options.get(SNMP_PASSWORD);        // SNMPv3 password, if applicable
        String pollMode         = options.get(SNMP_POLL_MODE);
        
        // MODIFIED: Read OIDS as a single string and then manually split it
        String oidsString = options.get(OIDS);
        List<String> oids = Arrays.asList(oidsString.split(",")); // MODIFIED LINE

        Integer intervalSeconds = options.get(SNMP_INTERVAL_SECONDS);
        Integer timeoutSeconds  = options.get(SNMP_TIMEOUT_SECONDS);
        Integer retries         = options.get(SNMP_RETRIES);

        System.out.println("SnmpDynamicTableSourceFactory: Extracted Options - " + 
                "  Target:    " + target +
                ", Version:   " + snmpVersion +
                ", Community: " + communityString +
                ", Mode:      " + pollMode +
                ", OIDs:      " + (oids != null ? oids.stream().collect(Collectors.joining(",")) : "null") + // Added null check for oids
                ", Interval:  " + intervalSeconds + "s" +
                ", Timeout:   " + timeoutSeconds + "s" +
                ", Retries:   " + retries + " (Direct System.out)");

        // Create SnmpAgentInfo based on the extracted options
        // Ensure this matches the SnmpAgentInfo constructor exactly (9 arguments)
        SnmpAgentInfo snmpAgentInfo = new SnmpAgentInfo(
                target.split(":")[0], // Host
                Integer.parseInt(target.split(":")[1]), // Port
                snmpVersion,
                communityString,
                userName,
                password,
                pollMode,
                oids,
                intervalSeconds,
                timeoutSeconds,
                retries
        );
        System.out.println("SnmpDynamicTableSourceFactory: Created SnmpAgentInfo: " + 
            snmpAgentInfo.getHost() + ":" + 
            snmpAgentInfo.getPort() + " (Direct System.out)"
        );


        // Return the DynamicTableSource implementation
        return new SnmpDynamicTableSource(producedDataType, snmpAgentInfo);
    }
}