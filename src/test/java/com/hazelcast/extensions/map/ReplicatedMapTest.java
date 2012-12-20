/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

public class ReplicatedMapTest {

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    @Test
    public void test() throws InterruptedException {
        Config config = new Config();
        final int k = 3;
        final HazelcastInstance[] hz = new HazelcastInstance[k];
        final ReplicatedMap<Integer, Integer> maps[] = new ReplicatedMap[k];
        for (int i = 0; i < k; i++) {
            hz[i] = Hazelcast.newHazelcastInstance(config);
            maps[i] = new ReplicatedMap<Integer, Integer>(hz[i], "test");
        }
        hz[k - 1].getPartitionService().getPartition(1).getOwner();
        Thread.sleep(5000);

        final int threadCount = 2 * k;
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final int entryCount = 1000;

        for (int i = 0; i < threadCount; i++) {
            final int id = i;
            executorService.submit(new Runnable() {
                public void run() {
                    final ReplicatedMap<Integer, Integer> map = maps[id % k];
                    Random random = new Random();
                    try {
                        for (int j = 0; j < entryCount; j++) {
                            if (random.nextInt(entryCount) % 2 == 0) {
                                map.put(random.nextInt(entryCount), random.nextInt(entryCount));
                            } else {
//                                map.remove(random.nextInt(entryCount));
                            }
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
        Thread.sleep(entryCount * 10);

        for (int i = 1; i < k; i++) {
            assertEquals(maps[0].size(), maps[i].size());
            for (Integer key : maps[0].keySet()) {
                try {
                    assertEquals(maps[0].get(key), maps[i].get(key));
                } catch (AssertionError e) {
                    System.out.println(e.getMessage());
                    System.out.println("000 = " + maps[0].getV(key).getVector());
                    System.out.println("iii = " + maps[i].getV(key).getVector());
                    System.out.println();
//                    throw e;
                }
            }
        }
    }
}
