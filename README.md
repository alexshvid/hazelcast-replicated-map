Replicated map implementation using Hazelcast.

###Usage:

````java
HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
ReplicationService rs = new ReplicationService(hz);
ReplicatedMap<Integer, String> map = rs.getMap("test");

HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
ReplicationService rs2 = new ReplicationService(hz2);
ReplicatedMap<Integer, String> map2 = rs2.getMap("test");

for (int i = 0; i < 10000; i++) {
   map.put(i, "test" + i);
}

Thread.sleep(2000); // wait for async replication to complete
System.out.println(map2.size()); // should be equal to 10000
	
Hazelcast.shutdownAll();