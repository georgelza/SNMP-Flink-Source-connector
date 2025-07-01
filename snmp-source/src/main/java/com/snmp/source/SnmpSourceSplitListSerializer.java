/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/ 
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpSourceSplitListSerializer.java
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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class SnmpSourceSplitListSerializer implements SimpleVersionedSerializer<List<SnmpSourceSplit>> {

    private final SnmpSourceSplitSerializer splitSerializer = new SnmpSourceSplitSerializer(); //

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(List<SnmpSourceSplit> splits) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeInt(splits.size()); //
            for (SnmpSourceSplit split : splits) { //
                byte[] serializedSplit = splitSerializer.serialize(split); //
                oos.writeInt(serializedSplit.length); //
                oos.write(serializedSplit); //
            }
            return baos.toByteArray();
        }
    }

    @Override
    public List<SnmpSourceSplit> deserialize(int version, byte[] serialized) throws IOException {
        if (version != getVersion()) {
            throw new IOException("Cannot deserialize split list with version " + version + ". Current version is " + getVersion() + ".");
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            int size = ois.readInt(); //
            List<SnmpSourceSplit> splits = new ArrayList<>(size); //
            for (int i = 0; i < size; i++) { //
                int length = ois.readInt(); //
                byte[] splitBytes = new byte[length]; //
                ois.readFully(splitBytes); //
                // This call might throw a RuntimeException if ClassNotFoundException occurs internally
                splits.add(splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes)); //
            }
            return splits;
        } catch (RuntimeException e) { // Catch RuntimeException instead of ClassNotFoundException //
            // This will catch the RuntimeException propagated from SnmpSourceSplitSerializer.deserialize
            throw new IOException("Failed to deserialize list of SnmpSourceSplit. Cause: " + e.getMessage(), e);
        }
    }
}