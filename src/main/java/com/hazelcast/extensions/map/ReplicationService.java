package com.hazelcast.extensions.map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;


public class ReplicationService<K, V> {

	private final ConcurrentMap<String, ReplicatedMap<K, V>> replicatedMaps = new ConcurrentHashMap<String, ReplicatedMap<K, V>>();
    private final ReplicationListener listener = new ReplicationListener();
    private final String topicId;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final ITopic<ReplicationMessage<K, V>> topic;
    private final String localMemberId;
    private final int localMemberHash;

	public ReplicationService(HazelcastInstance hazelcast) {
        final String name = "replicationService";
        final String threadName = hazelcast.getName() + "." + name;
        executor = Executors.newSingleThreadExecutor(new Factory(threadName + ".replicator"));
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new Factory(threadName + ".cleaner"));
        Member localMember = hazelcast.getCluster().getLocalMember();
        localMemberId = localMember.getUuid();
        localMemberHash = localMember.getUuid().hashCode();
        topic = hazelcast.getTopic(name);
        topicId = topic.addMessageListener(listener);
        scheduledExecutor.scheduleWithFixedDelay(new Cleaner(), 5, 5, TimeUnit.SECONDS);
	}
	
	public ReplicatedMap<K, V> getMap(String name) {
		ReplicatedMap<K, V> replicatedMap = replicatedMaps.get(name);
		if (replicatedMap == null) {
			replicatedMaps.putIfAbsent(name, new ReplicatedMap<K, V>(name, this));
			replicatedMap = replicatedMaps.get(name);
		}
		return replicatedMap;
	}
	
    public void destroy() {
    	topic.removeMessageListener(topicId);
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        for (ReplicatedMap<K, V> replicatedMap : replicatedMaps.values()) {
        	replicatedMap.clear();
        }
        replicatedMaps.clear();
    }
	
	int getLocalMemberHash() {
		return localMemberHash;
	}
	
	String getLocalMemberId() {
		return localMemberId;
	}
	
	void publish(ReplicationMessage<K, V> message) {
		topic.publish(message);
	}
	
    private class ReplicationListener implements MessageListener<ReplicationMessage<K, V>> {

        public void onMessage(final Message<ReplicationMessage<K, V>> message) {
            executor.submit(new Runnable() {
                public void run() {
                	getMap(message.getMessageObject().getReplicatedMap()).processUpdateMessage(message.getMessageObject());
                }
            });
        }

    }

    private class Cleaner implements Runnable {

        public void run() {
        	for (ReplicatedMap<K, V> replicatedMap : replicatedMaps.values()) {
        		replicatedMap.cleanup();
        	}
        }
    }
	
    private class Factory implements ThreadFactory {

        private final String threadName;

        private Factory(final String threadName) {
            this.threadName = threadName;
        }

        public Thread newThread(final Runnable r) {
            final Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            return t;
        }
    }

    
}
