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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.snmp.source.SnmpConfigOptions.*;

public class SnmpDynamicTableSourceFactory implements DynamicTableSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpDynamicTableSourceFactory.class);

    static {

        LOG.debug("{} SnmpDynamicTableSourceFactory: Static initializer called.",
            Thread.currentThread().getName()
        );
        
        System.out.println("\n SnmpDynamicTableSourceFactory: Static initializer called." 
            + " for Thread " + Thread.currentThread().getName()
            + " (Direct System.out)"
        );
    }

    public SnmpDynamicTableSourceFactory() {

        LOG.debug("{} SnmpDynamicTableSourceFactory: Constructor called.",
            Thread.currentThread().getName()
        );

        System.out.println("\n SnmpDynamicTableSourceFactory: Constructor called." 
            + " for Thread " + Thread.currentThread().getName()
            + " (Direct System.out)"
        );
    }

    @Override
    public String factoryIdentifier() {
    
        LOG.debug("{} SnmpDynamicTableSourceFactory: factoryIdentifier() called.",
            Thread.currentThread().getName()
        );

        System.out.println("\n SnmpDynamicTableSourceFactory: factoryIdentifier() called." 
            + " for Thread " + Thread.currentThread().getName()
            + " (Direct System.out)"
        );

        return "snmp";
    
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {

        LOG.debug("{} SnmpDynamicTableSourceFactory: requiredOptions() called.",
            Thread.currentThread().getName()
        );
        
        System.out.println("\n SnmpDynamicTableSourceFactory: requiredOptions() called." 
            + " for Thread " + Thread.currentThread().getName()
            + " (Direct System.out)"
        );

        final Set<ConfigOption<?>> options = new HashSet<>();
        
        options.add(TARGET);
        options.add(OIDS);
        options.add(SNMP_POLL_MODE);
        
        LOG.debug("{} SnmpDynamicTableSourceFactory: requiredOptions() Completed.",
            Thread.currentThread().getName()
        );
        
        System.out.println("\n SnmpDynamicTableSourceFactory: requiredOptions() Completed" 
            + " for Thread " + Thread.currentThread().getName()
            + " (Direct System.out)"
        );

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        
        LOG.debug("{} SnmpDynamicTableSourceFactory: optionalOptions() called.",
            Thread.currentThread().getName()
        );
        
        System.out.println("\n SnmpDynamicTableSourceFactory: optionalOptions() called." 
            + " for Thread " + Thread.currentThread().getName()
            + " (Direct System.out)"
        );

        final Set<ConfigOption<?>> options = new HashSet<>();
        
        options.add(SNMP_VERSION);
        options.add(SNMP_COMMUNITY_STRING);
        options.add(SNMP_USERNAME);
        options.add(SNMP_PASSWORD);
        options.add(SNMP_INTERVAL_SECONDS);
        options.add(SNMP_TIMEOUT_SECONDS);
        options.add(SNMP_RETRIES);
        options.add(SNMPV3_AUTH_PROTOCOL);
        options.add(SNMPV3_PRIV_PROTOCOL);
        
        LOG.debug("{} SnmpDynamicTableSourceFactory: optionalOptions() Completed.",
            Thread.currentThread().getName()
        );
        
        System.out.println("\n SnmpDynamicTableSourceFactory: optionalOptions() Completed" 
            + " for Thread " + Thread.currentThread().getName()
            + " (Direct System.out)"
        );

        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        LOG.debug("\n {} SnmpDynamicTableSourceFactory: createDynamicTableSource() called.",
            Thread.currentThread().getName()
        );

        System.out.println("\n SnmpDynamicTableSourceFactory: createDynamicTableSource() called" 
            + " for Thread " + Thread.currentThread().getName()
            + " (Direct System.out)"
        );

        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // Corrected method call: getOptions() instead of getValidatedConfigurations()
        final ReadableConfig config = helper.getOptions();
        final DataType producedDataType = context.getPhysicalRowDataType();

        // Extract common SNMP configuration options
        final String snmpVersion        = config.getOptional(SNMP_VERSION).orElse("SNMPv2c");
        final String communityString    = config.getOptional(SNMP_COMMUNITY_STRING).orElse("public");
        final String userName           = config.getOptional(SNMP_USERNAME).orElse(null);
        final String password           = config.getOptional(SNMP_PASSWORD).orElse(null);
        final String authProtocol       = config.getOptional(SNMPV3_AUTH_PROTOCOL).orElse(null);
        final String privProtocol       = config.getOptional(SNMPV3_PRIV_PROTOCOL).orElse(null);
        final String pollMode           = config.get(SNMP_POLL_MODE);
        final String oidsString         = config.get(OIDS);
        final List<String> oids         = Arrays.asList(oidsString.split(",")).stream()
                                            .map(String::trim)
                                            .filter(s -> !s.isEmpty())
                                            .collect(Collectors.toList());
        final int intervalSeconds       = config.get(SNMP_INTERVAL_SECONDS);
        final int timeoutSeconds        = config.get(SNMP_TIMEOUT_SECONDS);
        final int retries               = config.get(SNMP_RETRIES);

        // Parse multiple targets
        final String targets = config.get(TARGET);
        List<SnmpAgentInfo> snmpAgentInfoList = new ArrayList<>();
        for (String target : targets.split(",")) {
            target = target.trim();
            if (target.isEmpty()) {
                continue;
            }

            String[] parts = target.split(":");
            if (parts.length != 2) {

                LOG.error("{} SnmpDynamicTableSourceFactory: createDynamicTableSource(): Invalid target format. Expected 'host:port', but got {}",
                    Thread.currentThread().getName(),
                    target
                );

                System.out.println("\n SnmpDynamicTableSourceFactory: createDynamicTableSource()" 
                    + " Invalid target format. Expected 'host:port', but got. " + target
                    + " for Thread "                                            + Thread.currentThread().getName()
                    + " (Direct System.out)"
                );

                throw new IllegalArgumentException("Invalid target format. Expected 'host:port', but got: " + target);
            }
            
            String host = parts[0];
            int port    = Integer.parseInt(parts[1]);

            SnmpAgentInfo snmpAgentInfo = new SnmpAgentInfo( 
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
                    retries
            );
            snmpAgentInfoList.add(snmpAgentInfo);
            
            LOG.debug("{} SnmpDynamicTableSourceFactory: Created SnmpAgentInfo for {}:{}",
                Thread.currentThread().getName(),
                snmpAgentInfo.getHost(),
                snmpAgentInfo.getPort()
            );

            System.out.println("\n SnmpDynamicTableSourceFactory: createDynamicTableSource()" 
                + " Created SnmpAgentInfo for: " + snmpAgentInfo.getHost() + ":" + snmpAgentInfo.getPort()
                + " for Thread "                 + Thread.currentThread().getName()
                + " (Direct System.out)"
            );
        }

        if (snmpAgentInfoList.isEmpty()) {

            LOG.error("{} SnmpDynamicTableSourceFactory: createDynamicTableSource(): No valid SNMP targets agent provided.",
                Thread.currentThread().getName()
            );

            System.out.println("\n SnmpDynamicTableSourceFactory: createDynamicTableSource()" 
                + " No valid SNMP targets agent provided."
                + " for Thread " + Thread.currentThread().getName()
                + " (Direct System.out)"
            );

            throw new IllegalArgumentException("No valid SNMP targets agent provided.");
        }

        // Return the DynamicTableSource implementation
        // This line needs to be updated in SnmpDynamicTableSource.java to accept a List<SnmpAgentInfo>
        return new SnmpDynamicTableSource(producedDataType, snmpAgentInfoList);
    }
}