/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class ReplicationMessage<K, V> implements DataSerializable {

	private static final long serialVersionUID = -4815192795649067789L;

	K key;
    V value;
    Vector vector;
    Member origin;
    int updateHash;

    public ReplicationMessage() {
    }

    public ReplicationMessage(K key, V v, Vector vector, Member origin, int hash) {
        this.key = key;
        this.value = v;
        this.vector = vector;
        this.origin = origin;
        this.updateHash = hash;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
    	out.writeObject(key);
    	out.writeObject(value);
        vector.writeData(out);
        origin.writeData(out);
        out.writeInt(updateHash);
    }

    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        value = in.readObject();
        vector = new Vector();
        vector.readData(in);
        origin = new MemberImpl();
        origin.readData(in);
        updateHash = in.readInt();
    }

    public boolean isRemove() {
        return value == null;
    }

    @Override
    public String toString() {
        return "ReplicationMessage{" +
               "key=" + key +
               ", value=" + value +
               ", vector=" + vector +
               ", origin=" + getUpdateHash() +
               '}';
    }

    public int getUpdateHash() {
        return updateHash;
    }
}
