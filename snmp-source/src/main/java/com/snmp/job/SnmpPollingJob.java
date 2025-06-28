/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpPollingJob.java
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

package com.snmp.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 *
 */
public class SnmpPollingJob {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpPollingJob.class);

    public static void main(String[] args) throws Exception {

        System.out.println(
            Thread.currentThread().getName() + " Method called."
        );
        

        // Parse command-line arguments for catalog, database, and the table to select from
        String catalogName;
        String databaseName;
        String tableToSelectFromName;
        String fullTableName;

        if (args.length >= 3) {
            catalogName           = args[0];
            databaseName          = args[1];
            tableToSelectFromName = args[2];
            fullTableName         = String.format("%s.%s.%s", catalogName, databaseName, tableToSelectFromName);
           
            System.out.println(
                 Thread.currentThread().getName() + "{} Arguments received: Catalog='{}', Database='{}', Table to Select From='{}'" + catalogName + databaseName + tableToSelectFromName
            );

            LOG.debug("{}: Arguments received: Catalog='{}', Database='{}', Table to Select From='{}'",
                Thread.currentThread().getName(),
                catalogName,
                databaseName,
                tableToSelectFromName
            );

        } else {
            LOG.error("{}: ERROR: Insufficient arguments provided. Please provide Catalog Name, Database Name, and the Table Name to Select From.",
                Thread.currentThread().getName()
            );

            return; // Exit if arguments are incorrect
        }

        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // Enable checkpointing every 5 seconds for fault tolerance

        // Set up the table environment
        EnvironmentSettings settings    = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // This job relies on Flink's configuration to find the specified catalog.
        // If it's a Hive Catalog, ensure 'flink-connector-hive_<scala-version>.jar' and
        // Hive client JARs (e.g., hive-exec, hadoop-common etc.) are placed in your
        // Flink cluster's 'lib/' directory. This job does NOT include them directly.
        try {
            tableEnv.useCatalog(catalogName);
            tableEnv.useDatabase(databaseName);
            LOG.debug("{}: Flink TableEnvironment configured to use Catalog: '{}' and Database: '{}'.", 
                Thread.currentThread().getName(),
                catalogName, 
                databaseName
            );
        } catch (Exception e) {
            LOG.error("{}: Failed to use catalog '{}' or database '{}'. Ensure it's correctly configured in Flink and necessary connector JARs are in Flink's lib directory. Error: {} {}",
                Thread.currentThread().getName(),
                catalogName, 
                databaseName, 
                e.getMessage(), 
                e
            );
            throw e; // Re-throw to fail the job
        }
        
        Table resultTable = tableEnv.from(tableToSelectFromName);

        LOG.debug("{}: Starting Flink job to continuously read from {}...", 
            Thread.currentThread().getName(),   
            fullTableName
        );
        
        tableEnv.toDataStream(resultTable, Row.class).print();

        // Execute the Flink job. This will start the continuous reading and printing.
        env.execute("SNMP Data Viewer Job for " + fullTableName);
    }
}
