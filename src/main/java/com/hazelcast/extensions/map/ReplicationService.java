package com.hazelcast.extensions.map;

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

	private final ReplicatedMap<K, V> replicatedMap;
    private final ReplicationListener listener = new ReplicationListener();
    private final String topicId;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final ITopic<ReplicationMessage<K, V>> topic;
    private final String localMemberId;
    private final int localMemberHash;

	public ReplicationService(HazelcastInstance hazelcast, String mapName) {
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
        replicatedMap = new ReplicatedMap<K,V>(mapName, this);
	}
	
	public ReplicatedMap<K, V> getMap() {
		return replicatedMap;
	}
	
    public void destroy() {
    	topic.removeMessageListener(topicId);
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        replicatedMap.clear();
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
                	replicatedMap.processUpdateMessage(message.getMessageObject());
                }
            });
        }

    }

    private class Cleaner implements Runnable {

        public void run() {
        	replicatedMap.cleanup();
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
