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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ReplicatedMap<K, V> implements Map<K, V> {

	private static final long CLEANUP_TTL = TimeUnit.SECONDS.toMillis(10);
	
	private final String name;
    private final ConcurrentHashMap<K, ValueHolder<V>> map = new ConcurrentHashMap<K, ValueHolder<V>>();
    private final Object[] mutexes = new Object[32];
    private final ReplicationService replicationService;

    ReplicatedMap(String name, ReplicationService replicationService) {
    	this.name = name;
        for (int i = 0; i < mutexes.length; i++) {
            mutexes[i] = new Object();
        }
        this.replicationService = replicationService;
    }

    public String getName() {
    	return name;
    }
    
    public V get(Object key) {
        final ValueHolder<V> vh = map.get(key);
        return vh == null ? null : vh.getValue();
    }

    public ValueHolder<V> getValueHolder(Object key) {
        return map.get(key);
    }

    public V put(K key, V value) {
        V oldValue = null;
        synchronized (getMutex(key)) {
            final ValueHolder<V> current = map.get(key);
            final Vector vector;
            int hash = replicationService.getLocalMemberHash();
            if (current == null) {
                vector = new Vector();
                map.put(key, new ValueHolder<V>(value, vector, hash));
            } else {
                oldValue = current.getValue();
                vector = current.getVector();
                current.setValue(value, hash);
            }
            vector.incrementClock(replicationService.getLocalMemberId());
            replicationService.propagate(new ReplicationMessage(name, key, value, vector, replicationService.getLocalMemberId(), hash));
        }
        return oldValue;
    }

    public V remove(Object key) {
        V old;
        synchronized (getMutex(key)) {
            final ValueHolder<V> current = map.get(key);
            final Vector vector;
            if (current == null) {
                old = null;
            } else {
                vector = current.getVector();
                old = current.getValue();
                current.setValue(null, 0);
                vector.incrementClock(replicationService.getLocalMemberId());
                replicationService.propagate(new ReplicationMessage(name, key, null, vector, replicationService.getLocalMemberId(), replicationService.getLocalMemberHash()));
            }
        }
        return old;
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    public Set<K> keySet() {
        return map.keySet();
    }

    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        map.clear();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public int size() {
        return map.size();
    }

    @Override
    public boolean equals(Object o) {
        return map.equals(o);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " -> " + map.toString();
    }

    private Object getMutex(final Object key) {
        return mutexes[key.hashCode() != Integer.MIN_VALUE ? Math.abs(key.hashCode()) % mutexes.length : 0];
    }

    
    void processUpdateMessage(final ReplicationMessage update) {
        if (replicationService.getLocalMemberId().equals(update.getMemberId())) {
            return;
        }
        synchronized (getMutex(update.getKey())) {
            final ValueHolder<V> localEntry = map.get(update.getKey());
            if (localEntry == null) {
                if (!update.isRemove()) {
                    map.put((K) update.getKey(), new ValueHolder<V>((V) update.getValue(), update.getVector(), update.getUpdateHash()));
                }
            } else {
                final Vector currentVector = localEntry.getVector();
                final Vector updateVector = update.getVector();
                if (updateVector.happenedBefore(currentVector)) {
                    // ignore the update. This is an old update
                } else if (currentVector.happenedBefore(updateVector)) {
                    // A new update happened
                    applyTheUpdate(update, localEntry);
                } else {
                    // no preceding among the clocks. Lower hash wins..
                    if (localEntry.getLatestUpdateHash() >= update.getUpdateHash()) {
                        applyTheUpdate(update, localEntry);
                    } else {
                    	currentVector.applyVector(updateVector);
                        replicationService.propagate(new ReplicationMessage(name, update.getKey(), localEntry.getValue(),
                                currentVector, replicationService.getLocalMemberId(), localEntry.getLatestUpdateHash()));
                    }
                }
            }
        }
    }
    
    private void applyTheUpdate(ReplicationMessage update, ValueHolder<V> localEntry) {
        Vector localVector = localEntry.getVector();
        Vector remoteVector = update.getVector();
        localEntry.setValue((V) update.getValue(), update.getUpdateHash());
        localVector.applyVector(remoteVector);
    }

    
    void cleanup() {
        final Iterator<ValueHolder<V>> iter = map.values().iterator();
        final long now = System.currentTimeMillis();
        while (iter.hasNext()) {
            final ValueHolder<V> v = iter.next();
            if (v.getValue() == null && (v.getUpdateTime() + CLEANUP_TTL) < now) {
                iter.remove();
            }
        }
    }

}
