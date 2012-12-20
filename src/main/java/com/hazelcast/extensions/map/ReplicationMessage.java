/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
