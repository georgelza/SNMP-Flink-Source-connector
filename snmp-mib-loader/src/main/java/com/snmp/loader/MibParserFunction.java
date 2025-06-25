/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Java SNMP MIB Loader into Flink based table.
/                       :   Part of SNMP Source Connector Project.
/
/       File            :   MibParserFunction.java
/
/       Description     :   SNMP Source connector
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

package com.snmp.loader;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.percederberg.mibble.*;
import net.percederberg.mibble.loader.*;
import net.percederberg.mibble.value.*;

import net.percederberg.mibble.Mib;
import net.percederberg.mibble.MibLoader;
import net.percederberg.mibble.MibSymbol;
import net.percederberg.mibble.mib.MibNode;
import net.percederberg.mibble.value.ObjectIdentifierValue;
import net.percederberg.mibble.type.ObjectType;
import net.percederberg.mibble.type.TextualConvention;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A Flink RichFlatMapFunction that parses SNMP MIB files using the Mibble library
 * and extracts OID metadata, then emits it as RowData.
 */
public class MibParserFunction extends RichFlatMapFunction<File, GenericRowData> {

    private static final Logger LOG = LoggerFactory.getLogger(MibParserFunction.class);

    private transient MibLoader mibLoader;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mibLoader = new MibLoader();
        // Add common MIB directories if necessary, e.g., from a configuration
        // For simplicity, we assume MIBs might be in the same directory as the input file or standard locations.
        // In a real deployment, these paths would likely be configured.
    }

    @Override
    public void flatMap(File mibFile, Collector<GenericRowData> out) throws Exception {
        LOG.info("Processing MIB file: {}", mibFile.getAbsolutePath());
        try {
            // Add the parent directory of the current MIB file to the loader's search path
            // This helps in resolving IMPORTS from other MIBs in the same directory.
            mibLoader.addDir(mibFile.getParentFile());
            Mib mib = mibLoader.load(mibFile);
            LOG.info("Successfully loaded MIB: {} from file: {}", mib.getName(), mibFile.getName());

            for (MibSymbol symbol : mib.getAllSymbols()) {
                if (symbol instanceof MibNode) {
                    MibNode node = (MibNode) symbol;
                    String oidFull = "";
                    String oidName = node.getName();
                    String description = Objects.toString(node.getDescription(), "");
                    String syntaxType = "Unknown";
                    String unit = ""; // Mibble does not provide a direct unit field easily. This might need regex on description.
                    String oidType = getMibbleOidType(node);
                    String mibModule = node.getMib().getName();

                    if (node.getValue() instanceof ObjectIdentifierValue) {
                        oidFull = ((ObjectIdentifierValue) node.getValue()).toString();
                    } else if (node instanceof ObjectType) { // For OBJECT-TYPEs which are common for metrics
                        ObjectType objectType = (ObjectType) node;
                        oidFull = Objects.toString(objectType.getOID(), "");
                        syntaxType = Objects.toString(objectType.getSyntax().getName(), "");
                        // Attempt to extract UNIT from description if available
                        String descLower = description.toLowerCase();
                        if (descLower.contains("units:") || descLower.contains("unit is")) {
                            int unitIndex = descLower.indexOf("units:");
                            if (unitIndex == -1) unitIndex = descLower.indexOf("unit is");
                            if (unitIndex != -1) {
                                String sub = description.substring(unitIndex);
                                int newLineIndex = sub.indexOf('\n');
                                int dotIndex = sub.indexOf('.');
                                int endIndex = description.length();

                                if (newLineIndex != -1) endIndex = Math.min(endIndex, unitIndex + newLineIndex);
                                if (dotIndex != -1) endIndex = Math.min(endIndex, unitIndex + dotIndex);

                                unit = description.substring(unitIndex, endIndex).trim();
                                unit = unit.replaceAll("(?i)units:|unit is", "").trim();
                                if (unit.endsWith(".")) unit = unit.substring(0, unit.length() - 1);
                            }
                        }
                    } else if (symbol.getType() instanceof TextualConvention) {
                        syntaxType = ((TextualConvention) symbol.getType()).getSyntax().getName();
                    } else if (symbol.getType() != null) {
                        syntaxType = symbol.getType().getName();
                    }

                    // Create a new RowData and set the fields
                    GenericRowData row = new GenericRowData(RowKind.INSERT, 7);
                    row.setField(0, StringData.fromString(oidFull));
                    row.setField(1, StringData.fromString(oidName));
                    row.setField(2, StringData.fromString(description));
                    row.setField(3, StringData.fromString(syntaxType));
                    row.setField(4, StringData.fromString(unit));
                    row.setField(5, StringData.fromString(oidType));
                    row.setField(6, StringData.fromString(mibModule));

                    out.collect(row);
                }
            }
        } catch (IOException e) {
            LOG.error("Error loading MIB file {}: {}", mibFile.getAbsolutePath(), e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Unexpected error processing MIB file {}: {}", mibFile.getAbsolutePath(), e.getMessage(), e);
        }
    }

    /**
     * Determines the OID type based on the MibNode.
     * This is a simplified mapping based on common MIB constructs.
     * Mibble's API might offer more direct ways to categorize these.
     */
    private String getMibbleOidType(MibNode node) {
        if (node instanceof ObjectType) {
            ObjectType objType = (ObjectType) node;
            if (objType.isScalar()) {
                return "Scalar";
            } else if (objType.isTable()) {
                return "Table";
            } else if (objType.isColumn()) {
                return "Column"; // Mibble has a specific check for columns
            }
        }
        // Fallback for other MibNode types or if not an ObjectType
        if (node.getClass().getSimpleName().contains("Notification")) { // Heuristic
            return "Notification";
        } else if (node.getClass().getSimpleName().contains("Group")) { // Heuristic
            return "Group";
        }
        return "Unknown";
    }

    @Override
    public void close() throws Exception {
        super.close();
        // MibLoader itself doesn't require explicit closing based on Mibble docs.
        // Resource cleanup for any other transient objects can happen here.
    }
}