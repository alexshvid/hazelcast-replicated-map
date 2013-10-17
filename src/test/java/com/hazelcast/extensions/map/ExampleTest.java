package com.hazelcast.extensions.map;

import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import static org.junit.Assert.assertEquals;

public class ExampleTest extends SystemProperties {

    @Test
	public void testExample() throws InterruptedException {
    	try {
			HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
			ReplicationService rs = new ReplicationService(hz);
			ReplicatedMap<Integer, String> map = rs.getMap("test");
	
			HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
			ReplicationService rs2 = new ReplicationService(hz2);
			ReplicatedMap<Integer, String> map2 = rs2.getMap("test");
	
			for (int i = 0; i < 10000; i++) {
			   map.put(i, "test" + i);
			}
	
			Thread.sleep(2000);
			assertEquals(map.size(), map2.size()); 
		
    	}
    	finally {
    		Hazelcast.shutdownAll();
    	}
	}
	
}
