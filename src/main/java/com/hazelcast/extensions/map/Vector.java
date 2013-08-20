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

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Vector implements DataSerializable {

    final Map<Member, AtomicInteger> clocks;

    public Vector() {
        clocks = new ConcurrentHashMap<Member, AtomicInteger>();
    }

    public void writeData(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(clocks.size());
        for (Entry<Member, AtomicInteger> entry : clocks.entrySet()) {
            entry.getKey().writeData(dataOutput);
            dataOutput.writeInt(entry.getValue().get());
        }
    }

    public void readData(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            Member m = new MemberImpl();
            m.readData(dataInput);
            int clock = dataInput.readInt();
            clocks.put(m, new AtomicInteger(clock));
        }
    }

    static boolean happenedBefore(Vector x, Vector y) {
        Set<Member> members = new HashSet<Member>(x.clocks.keySet());
        members.addAll(y.clocks.keySet());

        boolean hasLesser = false;
        for (Member m : members) {
            int xi = x.clocks.get(m) != null ? x.clocks.get(m).get() : 0;
            int yi = y.clocks.get(m) != null ? y.clocks.get(m).get() : 0;
            if (xi > yi) {
                return false;
            }
            if (xi < yi) {
                hasLesser = true;
            }
        }
        return hasLesser;
    }

    @Override
    public String toString() {
        return "Vector{" +
                "clocks=" + clocks +
                '}';
    }
}
