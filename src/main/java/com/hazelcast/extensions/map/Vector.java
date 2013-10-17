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
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class Vector implements DataSerializable {

	private static final long serialVersionUID = -8566139603253326256L;

	final Map<String, AtomicInteger> clocks;

    public Vector() {
        clocks = new ConcurrentHashMap<String, AtomicInteger>();
    }

    public void writeData(ObjectDataOutput dataOutput) throws IOException {
        dataOutput.writeInt(clocks.size());
        for (Entry<String, AtomicInteger> entry : clocks.entrySet()) {
        	dataOutput.writeUTF(entry.getKey());
            dataOutput.writeInt(entry.getValue().get());
        }
    }

    public void readData(ObjectDataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            String memberId = dataInput.readUTF();
            int clock = dataInput.readInt();
            clocks.put(memberId, new AtomicInteger(clock));
        }
    }

    public boolean happenedBefore(Vector that) {
        Set<String> members = new HashSet<String>(clocks.keySet());
        members.addAll(that.clocks.keySet());

        boolean hasLesser = false;
        for (String m : members) {
            int i = clocks.get(m) != null ? clocks.get(m).get() : 0;
            int iThat = that.clocks.get(m) != null ? that.clocks.get(m).get() : 0;
            if (i > iThat) {
                return false;
            }
            if (i < iThat) {
                hasLesser = true;
            }
        }
        return hasLesser;
    }
    
    public void applyVector(Vector update) {
        for (String m : update.clocks.keySet()) {
            AtomicInteger currentClock = clocks.get(m);
            AtomicInteger updateClock = update.clocks.get(m);
            if (currentClock == null) {
            	clocks.put(m, new AtomicInteger(updateClock.get()));
            }
            else {
            	while(!currentClock.compareAndSet(currentClock.get(), Math.max(currentClock.get(), updateClock.get())));
            }
        }
    }

    @Override
    public String toString() {
        return "Vector{" +
                "clocks=" + clocks +
                '}';
    }
}
