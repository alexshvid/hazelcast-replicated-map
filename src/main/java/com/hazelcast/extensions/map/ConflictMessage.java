package com.hazelcast.extensions.map;

/**
 * @mdogan 11/28/12
 */
public class ConflictMessage<K> extends ReplicationMessage<K> {

    public ConflictMessage() {
    }

    public ConflictMessage(final K key) {
        super(key);
    }
}
