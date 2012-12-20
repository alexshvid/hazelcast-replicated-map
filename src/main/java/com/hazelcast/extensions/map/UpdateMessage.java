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

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UpdateMessage<K, V> extends ReplicationMessage<K> {

    V value;
    Vector vector;
    Member origin;

    public UpdateMessage() {
    }

    public UpdateMessage(K key, V v, Vector vector, Member origin) {
        super(key);
        this.value = v;
        this.vector = vector;
        this.origin = origin;
    }

    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        SerializationHelper.writeObject(out, value);
        vector.writeData(out);
        origin.writeData(out);

    }

    public void readData(DataInput in) throws IOException {
        super.readData(in);
        value = (V) SerializationHelper.readObject(in);
        vector = new Vector();
        vector.readData(in);
        origin = new MemberImpl();
        origin.readData(in);
    }

    public boolean isRemove() {
        return value == null;
    }

    @Override
    public String toString() {
        return "UpdateMessage{" +
               "key=" + getKey() +
               ", value=" + value +
               ", vector=" + vector +
               ", origin=" + origin +
               '}';
    }
}
