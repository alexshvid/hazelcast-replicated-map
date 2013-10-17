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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class VectorTest {

    @Test
    public void testEqual() throws Exception {

        Vector a = new Vector();
        Vector b = new Vector();
        a.clocks.put("m1", new AtomicInteger(1));
        a.clocks.put("m2", new AtomicInteger(1));
        a.clocks.put("m3", new AtomicInteger(1));

        b.clocks.put("m1", new AtomicInteger(1));
        b.clocks.put("m2", new AtomicInteger(1));
        b.clocks.put("m3", new AtomicInteger(1));

        assertFalse(a.happenedBefore(b));
        assertFalse(b.happenedBefore(a));
    }

    @Test
    public void testDesc() throws Exception {

        Vector a = new Vector();
        Vector b = new Vector();
        a.clocks.put("m1", new AtomicInteger(0));
        a.clocks.put("m2", new AtomicInteger(1));
        a.clocks.put("m3", new AtomicInteger(0));

        b.clocks.put("m1", new AtomicInteger(0));
        b.clocks.put("m2", new AtomicInteger(0));
        b.clocks.put("m3", new AtomicInteger(0));

        assertTrue(b.happenedBefore(a));
    }

    @Test
    public void testNotDesc() throws Exception {

        Vector a = new Vector();
        Vector b = new Vector();
        a.clocks.put("m1", new AtomicInteger(0));
        a.clocks.put("m2", new AtomicInteger(1));
        a.clocks.put("m3", new AtomicInteger(0));

        b.clocks.put("m1", new AtomicInteger(0));
        b.clocks.put("m2", new AtomicInteger(1));
        b.clocks.put("m3", new AtomicInteger(1));

        assertFalse(b.happenedBefore(a));
    }

}
