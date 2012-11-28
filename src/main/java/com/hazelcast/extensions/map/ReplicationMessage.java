package com.hazelcast.extensions.map;

import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @mdogan 11/28/12
 */
abstract class ReplicationMessage<K> implements DataSerializable {

    private K key;

    public ReplicationMessage() {
    }

    public ReplicationMessage(final K key) {
        this.key = key;
    }

    public void writeData(final DataOutput out) throws IOException {
        SerializationHelper.writeObject(out, key);
    }

    public void readData(final DataInput in) throws IOException {
        key = (K) SerializationHelper.readObject(in);
    }

    public K getKey() {
        return key;
    }
}
