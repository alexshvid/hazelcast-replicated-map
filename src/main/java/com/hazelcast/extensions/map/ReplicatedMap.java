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

import com.hazelcast.core.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicatedMap<K, V> implements Map<K, V> {

    private final ConcurrentHashMap<K, ValueHolder<V>> map = new ConcurrentHashMap<K, ValueHolder<V>>();
    private final ReplicationListener listener = new ReplicationListener();
    private final String topicId;
    private final Object[] mutexes = new Object[32];
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final ITopic<ReplicationMessage<K, V>> topic;
    private final Member localMember;

    public ReplicatedMap(HazelcastInstance hazelcast, String mapName) {
        final String name = "rm:" + mapName;
        for (int i = 0; i < mutexes.length; i++) {
            mutexes[i] = new Object();
        }
        final String threadName = hazelcast.getName() + "." + name;
        executor = Executors.newSingleThreadExecutor(new Factory(threadName + ".replicator"));
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new Factory(threadName + ".cleaner"));
        localMember = hazelcast.getCluster().getLocalMember();
        topic = hazelcast.getTopic(name);
        topicId = topic.addMessageListener(listener);
        scheduledExecutor.scheduleWithFixedDelay(new Cleaner(), 5, 5, TimeUnit.SECONDS);
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
            int hash = localMember.getUuid().hashCode();
            if (current == null) {
                vector = new Vector();
                map.put(key, new ValueHolder<V>(value, vector, hash));
            } else {
                oldValue = current.getValue();
                vector = current.getVector();
                current.setValue(value, hash);
            }
            incrementClock(vector);
            topic.publish(new ReplicationMessage<K, V>(key, value, vector, localMember, hash));
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
                incrementClock(vector);
                topic.publish(new ReplicationMessage(key, null, vector, localMember, localMember.getUuid().hashCode()));
            }
        }
        return old;
    }

    private void incrementClock(final Vector vector) {
        final AtomicInteger clock = vector.clocks.get(localMember);
        if (clock != null) {
            clock.incrementAndGet();
        } else {
            vector.clocks.put(localMember, new AtomicInteger(1));
        }
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

    public void destroy() {
    	topic.removeMessageListener(topicId);
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        map.clear();
    }

    private class ReplicationListener implements MessageListener<ReplicationMessage<K, V>> {

        public void onMessage(final Message<ReplicationMessage<K, V>> message) {
            executor.submit(new Runnable() {
                public void run() {
                    processUpdateMessage(message.getMessageObject());
                }
            });
        }

        private void processUpdateMessage(final ReplicationMessage<K, V> update) {
            if (localMember.equals(update.origin)) {
                return;
            }
            synchronized (getMutex(update.key)) {
                final ValueHolder<V> localEntry = map.get(update.key);
                if (localEntry == null) {
                    if (!update.isRemove()) {
                        map.put(update.key, new ValueHolder<V>(update.value, update.vector, update.getUpdateHash()));
                    }
                } else {
                    final Vector currentVector = localEntry.getVector();
                    final Vector updateVector = update.vector;
                    if (Vector.happenedBefore(updateVector, currentVector)) {
                        // ignore the update. This is an old update
                    } else if (Vector.happenedBefore(currentVector, updateVector)) {
                        // A new update happened
                        applyTheUpdate(update, localEntry);
                    } else {
                        // no preceding among the clocks. Lower hash wins..
                        if (localEntry.getLatestUpdateHash() >= update.getUpdateHash()) {
                            applyTheUpdate(update, localEntry);
                        } else {
                            applyVector(updateVector, currentVector);
                            topic.publish(new ReplicationMessage<K, V>(update.key, localEntry.getValue(),
                                    currentVector, localMember, localEntry.getLatestUpdateHash()));
                        }
                    }
                }
            }
        }


        private void applyTheUpdate(ReplicationMessage<K, V> update, ValueHolder<V> localEntry) {
            Vector localVector = localEntry.getVector();
            Vector remoteVector = update.vector;
            localEntry.setValue(update.value, update.getUpdateHash());
            applyVector(remoteVector, localVector);
        }

        private void applyVector(Vector update, Vector current) {
            for (Member m : update.clocks.keySet()) {
                final AtomicInteger currentClock = current.clocks.get(m);
                final AtomicInteger updateClock = update.clocks.get(m);
                if (smaller(currentClock, updateClock)) {
                    current.clocks.put(m, new AtomicInteger(updateClock.get()));
                }
            }
        }

        private boolean smaller(AtomicInteger int1, AtomicInteger int2) {
            int i1 = int1 == null ? 0 : int1.get();
            int i2 = int2 == null ? 0 : int2.get();
            return i1 < i2;
        }
    }

    private class Cleaner implements Runnable {

        private final long ttl = TimeUnit.SECONDS.toMillis(10);

        public void run() {
            final Iterator<ValueHolder<V>> iter = map.values().iterator();
            final long now = System.currentTimeMillis();
            while (iter.hasNext()) {
                final ValueHolder<V> v = iter.next();
                if (v.getValue() == null && (v.getUpdateTime() + ttl) < now) {
                    iter.remove();
                }
            }
        }
    }

    private class Factory implements ThreadFactory {

        private final String name;

        private Factory(final String name) {
            this.name = name;
        }

        public Thread newThread(final Runnable r) {
            final Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        }
    }

}
