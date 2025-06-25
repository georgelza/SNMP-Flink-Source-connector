/* //////////////////////////////////////////////////////////////////////////////////////////////////////
/
/
/       Project         :   Apache Flink SNMP Source connector
/
/       File            :   SnmpSourceSplitSerializer.java
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

package com.snmp.source;

import org.apache.flink.core.io.SimpleVersionedSerializer; //
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream; //
import java.io.ObjectOutputStream;
import java.io.IOException;

public class SnmpSourceSplitSerializer implements SimpleVersionedSerializer<SnmpSourceSplit> { //

    @Override
    public int getVersion() { //
        return 1;
    }

    @Override
    public byte[] serialize(SnmpSourceSplit split) throws IOException { //
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(split);
            return baos.toByteArray();
        }
    }

    @Override
    public SnmpSourceSplit deserialize(int version, byte[] serialized) throws IOException { //
        if (version != getVersion()) {
            throw new IOException("Cannot deserialize split with version " + version + ". Current version is " + getVersion() + ".");
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (SnmpSourceSplit) ois.readObject(); //
        } catch (ClassNotFoundException e) { // Catch the ClassNotFoundException
            // Wrap in a RuntimeException as SimpleVersionedSerializer.deserialize doesn't declare ClassNotFoundException
            throw new RuntimeException("Failed to deserialize SnmpSourceSplit due to ClassNotFoundException.", e);
        }
    }
}