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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class ReplicationMessage implements DataSerializable {

	private static final long serialVersionUID = -4815192795649067789L;

	private String replicatedMap;
	private Object key;
    private Object value;
    private Vector vector;
    private String memberId;
    private int updateHash;

    public ReplicationMessage() {
    }

    public ReplicationMessage(String replicatedMap, Object key, Object v, Vector vector, String memberId, int hash) {
        this.replicatedMap = replicatedMap;
    	this.key = key;
        this.value = v;
        this.vector = vector;
        this.memberId = memberId;
        this.updateHash = hash;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
    	out.writeUTF(replicatedMap);
    	out.writeObject(key);
    	out.writeObject(value);
        vector.writeData(out);
        out.writeUTF(memberId);
        out.writeInt(updateHash);
    }

    public void readData(ObjectDataInput in) throws IOException {
    	replicatedMap = in.readUTF();
        key = in.readObject();
        value = in.readObject();
        vector = new Vector();
        vector.readData(in);
        memberId = in.readUTF();
        updateHash = in.readInt();
    }

    public boolean isRemove() {
        return value == null;
    }

    @Override
    public String toString() {
        return "ReplicationMessage{" +
        		"replicatedMap=" + replicatedMap +
               ", key=" + key +
               ", value=" + value +
               ", vector=" + vector +
               ", updateHash=" + updateHash +
               '}';
    }

	String getReplicatedMap() {
		return replicatedMap;
	}

	Object getKey() {
		return key;
	}

	Vector getVector() {
		return vector;
	}

	String getMemberId() {
		return memberId;
	}

	int getUpdateHash() {
		return updateHash;
	}

	Object getValue() {
		return value;
	}

}
