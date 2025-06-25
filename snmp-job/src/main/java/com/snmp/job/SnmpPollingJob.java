/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector / Job
/
/       File            :   SnmpPollingJob.java
/
/       Description     :   SNMP Source Job, this will be instantiated by either selecting from the table or by submitting
/                       :   the job by calling /opt/flink/bin/flink run command
/       
/       Created     	:   June 2025
/
/       copyright       :   Copyright 2025, - G Leonard, georgelza@gmail.com
/                       
/       GIT Repo        :   
/
/       Blog            :   
/
/                       :   SELECT * FROM hive_catalog.snmp.snmp_poll_data
/                       :   or
/                       :   SELECT * FROM hive_catalog.snmp_poll_data WHERE metric_oid = '.1.3.6.1.2.1.1.1.0';
/                       :   or
/                           /opt/flink/bin/flink run \
/                               --detached \
/                               -c com.snmp.job.SnmpPollingJob \
/                               /opt/flink/lib/flink/snmp-job-1.0-SNAPSHOT.jar \
/                               hive_catalog \
/                               snmp \
/                               snmp_poll_data
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
 * A sample Flink job that demonstrates how to read data from a pre-existing Flink table
 * and print its contents to the console.
 * This job assumes the target table has been created and is potentially populated
 * by another Flink job (e.g., an INSERT INTO from an SNMP source).
 *
 * This Java application is submitted to Flink using `flink run -c ...`.
 * Its purpose is to demonstrate reading from a table that is already being populated
 * by another Flink job (e.g., one started via Flink SQL client's `INSERT INTO`).
 */
public class SnmpPollingJob {

    private static final Logger LOG = LoggerFactory.getLogger(SnmpPollingJob.class);

    public static void main(String[] args) throws Exception {

        LOG.info("{}: Called'");

        // Parse command-line arguments for catalog, database, and the table to select from
        String catalogName;
        String databaseName;
        String tableToInsertToName; // This will be the name of the table to read from
        String fullTableName;

        if (args.length >= 3) {
            catalogName           = args[0];
            databaseName          = args[1];
            tableToInsertToName   = args[2];
            fullTableName         = String.format("%s.%s.%s", catalogName, databaseName, tableToInsertToName);
           
            LOG.info("{}: Arguments received: Catalog='{}', Database='{}', Table to Select From='{}'",
                    SnmpPollingJob.class.getSimpleName(),
                    catalogName, 
                    databaseName, 
                    tableToInsertToName
            );

        } else {
            LOG.error("{}: ERROR: Insufficient arguments provided. Please provide Catalog Name, Database Name, and the Table Name to Insert T.",
                    SnmpPollingJob.class.getSimpleName()

            );
            LOG.error("{}: Usage: flink run -c com.snmp.job.SnmpPollingJob /path/to/snmp-job-1.0-SNAPSHOT.jar <catalog_name> <database_name> <table_to_select_from>",
                    SnmpPollingJob.class.getSimpleName()

            );
            return; // Exit if arguments are incorrect
        }

        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // Enable checkpointing every 5 seconds for fault tolerance

        // Set up the table environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Set the current catalog and database using the provided arguments
        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase(databaseName);

        LOG.info("{}: Flink TableEnvironment configured to use Catalog: '{}' and Database: '{}'.", 
            SnmpPollingJob.class.getSimpleName(),   
            catalogName, 
            databaseName
        );

        // Select all data from the specified table.
        // This assumes the table exists and its connector is properly configured to provide data.
        LOG.info("{}: Attempting to read data from Flink table: '{}' (Full name: {}).", 
            SnmpPollingJob.class.getSimpleName(),   
            tableToInsertToName, 
            fullTableName
        );
        
        Table resultTable = tableEnv.from(tableToInsertToName);

        LOG.info("{}: Starting Flink job to continuously read from {} and print to console...", 
            SnmpPollingJob.class.getSimpleName(),   
            fullTableName
        );
        
        // Convert Table to DataStream of Row and print to stdout
        tableEnv.toDataStream(resultTable, Row.class).print();

        // Execute the Flink job. This will start the continuous reading and printing.
        // For DML statements (like INSERT INTO) executeSql submits the job.
        // For SELECT to DataStream/print, env.execute() is still required to start the Flink job graph.
        env.execute("SNMP Data Viewer Job for " + fullTableName);
    }
}
