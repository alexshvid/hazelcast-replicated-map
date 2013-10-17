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

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class ReplicatedMapTest {

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.multicast.group", "224.3.3.3");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("hazelcast.wait.seconds.before.join", "0");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    @Test
    public void test() throws InterruptedException {
        Config config = new Config();
        final int k = 4;
        final HazelcastInstance[] hz = new HazelcastInstance[k];
        final ReplicatedMap<Integer, Integer> maps[] = new ReplicatedMap[k];
        for (int i = 0; i < k; i++) {
            hz[i] = Hazelcast.newHazelcastInstance(config);
            maps[i] = new ReplicationService<Integer, Integer>(hz[i], "test").getMap();
        }
        hz[k - 1].getPartitionService().getPartition(1).getOwner();
        Thread.sleep(5000);

        final int threadCount = 10 * k;
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final int entryCount = 1000;

        for (int i = 0; i < threadCount; i++) {
            final int id = i;
            executorService.submit(new Runnable() {
                public void run() {
                    final ReplicatedMap<Integer, Integer> map = maps[id % (k)];
                    Random random = new Random();
                    try {
                        for (int j = 0; j < entryCount; j++) {
                            map.put(random.nextInt(entryCount), (id + 1) * 10 + random.nextInt(9));
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                        fail(t.getMessage());
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        assertTrue(latch.await(threadCount * entryCount * 20, TimeUnit.MILLISECONDS));
        executorService.shutdown();
        Thread.sleep(entryCount * 100);

        Throwable t = null;
        try {
            for (int i = 1; i < k; i++) {
                assertEquals(maps[0].size(), maps[i].size());
                for (Integer key : maps[0].keySet()) {
                    try {
                        assertEquals(maps[0].get(key), maps[i].get(key));
                    } catch (AssertionError e) {
                        System.err.println(e);
                        System.err.println("key=" + key);
                        System.err.println("0 = " + maps[0].getValueHolder(key).getVector());
                        System.err.println(i + " = " + maps[i].getValueHolder(key).getVector());
                        System.err.println();
                        t = e;
                    }
                }
            }
        } finally {
            Hazelcast.shutdownAll();
        }

        assertNull(t);
    }
}
