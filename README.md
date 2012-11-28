Replicated map implementation using Hazelcast.

###Usage:

````java
final HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
ReplicatedMap<Integer, String> map = new ReplicatedMap<Integer, String>(hz, "test");

final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
ReplicatedMap<Integer, String> map2 = new ReplicatedMap<Integer, String>(hz2, "test");

for (int i = 0; i < 10000; i++) {
   map.put(i, "test" + i);
}

Thread.sleep(2000); // wait for async replication to complete
System.out.println(map2.size()); // should be equal to 10000
