/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Java SNMP MIB Loader into Flink based table.
/                       :   Part of SNMP Source Connector Project.
/
/       File            :   MibToMetadataLoaderJob.java
/
/       Description     :   Java file Loader parse MIB files and load OID metadata into a database table.
/
/       Created     	:   June 2025
/
/       copyright       :   Copyright 2025, - G Leonard, georgelza@gmail.com
/                       
/       GIT Repo        :   
/
/       Blog            :   
/
/       Example Flink run command:
/
/       flink run -c com.snmp.loader.MibToMetadataLoaderJob \
/              /opt/flink/lib/snmp-mib-loader-1.0-SNAPSHOT.jar 
/               --mib-files \
/               /mib_files/SNMPv2-SMI.mib,\
/               /mib_files/SNMPv2-TC.mib, \
/               /mib_files/SNMPv2-MIB.mib,\
/               /mib_files/RFC1213-MIB.mib \
/               --catalog fluss_catalog \
/               --database snmp \
/               --table snmp_oid_metadata
/
/
/       Build using Mibble Package
/
/               https://github.com/cederberg/mibble
/               https://www.mibble.org/
/               https://www.mibble.org/doc/introduction.html
/
/
/       The Repo includes the primary 4 mib fils.
/
/       For Standard (RFC) MIBs
/
/       These MIBs are defined in RFC (Request for Comments) documents by the IETF (Internet Engineering Task Force). 
/       You can usually find them on the IETF website or reputable MIB repositories.
/
/       SNMPv2-SMI.mib (SNMPv2-Structure of Management Information):
/
/       Source RFC: RFC 2578 (SMIv2) - https://www.rfc-editor.org/rfc/rfc2578.html
/
/       - https://mibbrowser.online/mibdb_search.php?mib=SNMPv2-SMI
/        
/       The MIB definitions are typically embedded within the RFC text, which you'd then save as a .mib file.
/       You can also often find direct .mib files on MIB repository sites like MIB Depot (search for SNMPv2-SMI).
/       SNMPv2-TC.mib (SNMPv2-Textual Conventions):
/
/       Source RFC: RFC 2579 (Textual Conventions for SMIv2) - https://www.rfc-editor.org/rfc/rfc2579.html
/
/
/       - https://mibbrowser.online/mibdb_search.php?mib=SNMPv2-TC
/
/       MIB definitions are within the RFC.
/       Direct .mib file often on MIB Depot (search for SNMPv2-TC).
/       SNMPv2-MIB.mib (SNMPv2-Management Information Base):
/
/
/       - https://mibbrowser.online/mibdb_search.php?mib=SNMPv2-MIB
/
/
/       Source RFC: RFC 3418 (SNMPv2 MIB) - https://www.rfc-editor.org/rfc/rfc3418.html
/       MIB definitions are within the RFC.
/       Direct .mib file often on MIB Depot (search for SNMPv2-MIB).
/       RFC1213-MIB.mib (MIB-II):
/
/
/       - https://mibbrowser.online/mibdb_search.php?mib=RFC1213-MIB
/
/       Source RFC: RFC 1213 (Management Information Base for Network Management of TCP/IP-based Internets) - https://www.rfc-editor.org/rfc/rfc1213.html
/       This is a very common and fundamental MIB (often referred to as MIB-II).
/       MIB definitions are within the RFC.
/       Direct .mib file often on MIB Depot (search for RFC1213-MIB).
/
/
/       W.R.T.: CISCO-CME-MIB.mib see
/       https://www.cisco.com/c/en/us/td/docs/voice_ip_comm/cucme/mib/reference/guide/ccme_mib.html
/
/
/      Recommendation for Standard MIBs:
/
/      While you can copy-paste from RFCs, it's often more convenient and less error-prone to download them directly from a well-known MIB repository like 
/      [MIB Depot](https://www.google.com/search?q=https://www.mibdepot.com/). - replaced by https://mibbrowser.online after a domain high jack.
/
/      Many SNMP tools (like the snmp4j-tools you are using) also bundle these common MIBs within their installation or JAR resources, which are 
/      typically used by default when you call mibLoader.load("SNMPv2-MIB") without a direct file path. You can often find them within the snmp4j-tools JAR 
/      itself, in paths like /org/snmp4j/tools/mibs/.
/
/      General Cisco MIB Download Page: Cisco MIBs Support - https://www.google.com/search?q=https://www.cisco.com/c/en/us/support/docs/ip/simple-network-management-protocol-snmp/24716-mibs-download-faq.html
/
*///////////////////////////////////////////////////////////////////////////////////////////////////////

package com.snmp.loader;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MibToMetadataLoaderJob {

    private static final Logger LOG = LoggerFactory.getLogger(MibToMetadataLoaderJob.class);

    public static void main(String[] args) throws Exception {
        // 1. Parse command line arguments
        ParameterTool params = ParameterTool.fromArgs(args);
        String mibFilesArg = params.getRequired("mib-files");
        String catalogName = params.get("catalog", "default_catalog");
        String databaseName = params.get("database", "default_database");
        String tableName = params.getRequired("table");

        List<File> mibFiles = Arrays.stream(mibFilesArg.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(File::new)
                .collect(Collectors.toList());

        if (mibFiles.isEmpty()) {
            throw new IllegalArgumentException("No MIB files provided. Use --mib-files /path/to/mib1.mib,/path/to/mib2.mib");
        }

        // 2. Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining(); // Can be useful for debugging or specific optimizations

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Set the current catalog and database
        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase(databaseName);

        // 3. Create a DataStream from the MIB files and parse them
        // In a real scenario, you might want to parallelize this more or use a custom source
        // For simplicity, this example processes all files in a single flatMap.
        DataStream<GenericRowData> mibMetadataStream = env.fromCollection(mibFiles)
                .flatMap(new MibParserFunction());

        // 4. Define the schema for the data produced by MibParserFunction
        // This schema must match the column types of your 'snmp_oid_metadata' table
        Schema schema = Schema.newBuilder()
                .column("oid_full", org.apache.flink.table.api.DataTypes.STRING())
                .column("oid_name", org.apache.flink.table.api.DataTypes.STRING())
                .column("description", org.apache.flink.table.api.DataTypes.STRING())
                .column("syntax_type", org.apache.flink.table.api.DataTypes.STRING())
                .column("unit", org.apache.flink.table.api.DataTypes.STRING())
                .column("oid_type", org.apache.flink.table.api.DataTypes.STRING())
                .column("mib_module", org.apache.flink.table.api.DataTypes.STRING())
                .build();

        // 5. Convert the DataStream<GenericRowData> into a Flink Table
        Table mibTable = tableEnv.fromDataStream(
                mibMetadataStream,
                schema
        );

        // 6. Insert the data from the Flink Table into the pre-created 'snmp_oid_metadata' table
        // This assumes 'snmp_oid_metadata' is already registered in Flink's catalog.
        // The table name is now dynamically taken from the arguments.
        mibTable.insertInto(tableName).await(); // .await() makes the insertion blocking for job completion

        // Execute the Flink job with the specified name.
        env.execute(String.format("MIB Loader: Loading (Files: %s) into %s.%s.%s", mibFilesArg, catalogName, databaseName, tableName));
    }
}
 