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

public class ValueHolder<V> {

    private V value;
    private Vector vector;
    private int latestUpdateHash = 0;
    private long updateTime = System.currentTimeMillis();

    public ValueHolder() {
    }

    public ValueHolder(V value, Vector vector, int hash) {
        this.value = value;
        this.vector = vector;
        this.latestUpdateHash = hash;
    }

    public V getValue() {
        return value;
    }

    public Vector getVector() {
        return vector;
    }

    public void setValue(V value, int hash) {
        this.value = value;
        this.latestUpdateHash = hash;
        this.updateTime = System.currentTimeMillis();
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public int getLatestUpdateHash() {
        return latestUpdateHash;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ValueHolder{");
        sb.append("value=").append(value);
        sb.append(", vector=").append(vector);
        sb.append(", latestUpdateHash=").append(latestUpdateHash);
        sb.append('}');
        return sb.toString();
    }
}


