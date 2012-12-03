/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
    private final Object[] mutexes = new Object[32];
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final HazelcastInstance hazelcast;
    private final ITopic<ReplicationMessage<K>> topic;
    private final Member localMember;

    public ReplicatedMap(HazelcastInstance hazelcast, String mapName) {
        final String name = "rm:" + mapName;
        this.hazelcast = hazelcast;
        for (int i = 0; i < mutexes.length; i++) {
            mutexes[i] = new Object();
        }
        final String threadName = hazelcast.getName() + "." + name;
        executor = Executors.newSingleThreadExecutor(new Factory(threadName + ".replicator"));
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new Factory(threadName + ".cleaner"));
        localMember = hazelcast.getCluster().getLocalMember();
        topic = hazelcast.getTopic(name);
        topic.addMessageListener(listener);
        scheduledExecutor.scheduleWithFixedDelay(new Cleaner(), 5, 5, TimeUnit.SECONDS);
    }

    public V get(Object key) {
        final ValueHolder<V> vh = map.get(key);
        return vh == null ? null : vh.getValue();
    }

    ValueHolder<V> getV(Object key) {
        return map.get(key);
    }

    public V put(K key, V value) {
        V oldValue = null;
        synchronized (getMutex(key)) {
            final ValueHolder<V> old = map.get(key);
            final Vector vector;
            if (old == null) {
                vector = new Vector();
                map.put(key, new ValueHolder<V>(value, vector));
            } else {
                oldValue = old.getValue();
                vector = old.getVector();
                map.get(key).setValue(value);
            }
            incrementClock(vector);
            topic.publish(new UpdateMessage(key, value, vector, localMember));
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
                current.setValue(null);
                incrementClock(vector);
                topic.publish(new UpdateMessage(key, null, vector, localMember));
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
        return mutexes[Math.abs(key.hashCode()) % mutexes.length];
    }

    public void destroy() {
        topic.removeMessageListener(listener);
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        map.clear();
    }

    private class ReplicationListener implements MessageListener<ReplicationMessage<K>> {

        public void onMessage(final Message<ReplicationMessage<K>> message) {
            executor.submit(new Runnable() {
                public void run() {
                    if (message.getMessageObject() instanceof UpdateMessage) {
                        processUpdateMessage((UpdateMessage<K, V>) message.getMessageObject());
                    } else {
                        processConflictMessage((ConflictMessage<K>) message.getMessageObject());
                    }
                }
            });
        }

        private void processConflictMessage(final ConflictMessage<K> conflict) {
            if (isMaster()) {
                synchronized (getMutex(conflict.getKey())) {
                    ValueHolder<V> valueHolder = map.get(conflict.getKey());
                    put(conflict.getKey(), valueHolder != null ? valueHolder.getValue() : null);
                }
            }
        }

        private void processUpdateMessage(final UpdateMessage<K, V> update) {
            if (localMember.equals(update.origin)) {
                return;
            }
            synchronized (getMutex(update.getKey())) {
                final ValueHolder<V> localEntry = map.get(update.getKey());
                if (localEntry == null) {
                    if (!update.isRemove()) {
                        map.put(update.getKey(), new ValueHolder<V>(update.value, update.vector));
                    }
                } else {
                    final Vector localVector = localEntry.getVector();
                    final Vector remoteVector = update.vector;
                    if (localVector.descends(remoteVector)) {
                        // ignore the update. This is an old update
                    } else if (remoteVector.descends(localVector)) {
                        // A new update happened
                        applyTheUpdate(update, localEntry);
                    } else {
                        // no preceding among the clocks. The older wins
                        if (localMember.hashCode() >= update.origin.hashCode()) {
                            applyTheUpdate(update, localEntry);
                        } else {
                            applyVector(remoteVector, localVector);
                        }
                        if (!isMaster()) {
                            topic.publish(new ConflictMessage<K>(update.getKey()));
                        }
                    }
                }
            }
        }

        private void applyTheUpdate(UpdateMessage<K, V> updateMessage, ValueHolder<V> localEntry) {
            Vector localVector = localEntry.getVector();
            Vector remoteVector = updateMessage.vector;
            localEntry.setValue(updateMessage.value);
            applyVector(remoteVector, localVector);
        }

        private void applyVector(Vector remote, Vector local) {
            for (Member m : remote.clocks.keySet()) {
                final AtomicInteger localClock = local.clocks.get(m);
                final AtomicInteger remoteClock = remote.clocks.get(m);
                if (smaller(localClock, remoteClock)) {
                    local.clocks.put(m, new AtomicInteger(remoteClock.get()));
                }
            }
        }

        private boolean smaller(AtomicInteger int1, AtomicInteger int2) {
            int i1 = int1 == null ? 0 : int1.get();
            int i2 = int2 == null ? 0 : int2.get();
            return i1 < i2;
        }
    }

    private boolean isMaster() {
        return hazelcast.getCluster().getMembers().iterator().next().localMember();
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
