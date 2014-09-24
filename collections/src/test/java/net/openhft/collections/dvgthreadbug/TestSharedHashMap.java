/*
 * Copyright 2014 Higher Frequency Trading
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections.dvgthreadbug;

import net.openhft.collections.Builder;
import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.lang.model.DataValueClasses;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class TestSharedHashMap {
    SharedHashMap<String, TestDataValue> shm;
    TestDataValue data1 = DataValueClasses.newDirectReference(TestDataValue.class);
    TestDataValue data2 = DataValueClasses.newDirectReference(TestDataValue.class);
    TestDataValue data3 = DataValueClasses.newDirectReference(TestDataValue.class);
    TestDataValue data4 = DataValueClasses.newDirectReference(TestDataValue.class);
    TestDataValue data5 = DataValueClasses.newDirectReference(TestDataValue.class);
    TestDataValue data6 = DataValueClasses.newDirectReference(TestDataValue.class);
    TestDataValue data7 = DataValueClasses.newDirectReference(TestDataValue.class);
    TestDataValue data8 = DataValueClasses.newDirectReference(TestDataValue.class);

    @Before
    public void setUp() throws Exception {
        try {
            shm = SharedHashMapBuilder.of(String.class, TestDataValue.class)
                    .generatedValueType(true)
                    .actualSegments(1) // only used for testing purposes.
                    .entries(2048)
                    .entrySize(256)
                    .file(Builder.getPersistenceFile() )
                    .create();

            //Map entries are loaded into various segments in mappeddatastore
            shm.acquireUsing("1111.2222.A0", data1);

            shm.acquireUsing("1111.2222.B0", data2);

            shm.acquireUsing("1111.2222.A1", data3);

            shm.acquireUsing("1111.2222.B1", data4);

            shm.acquireUsing("1111.2222.A2", data5);

            shm.acquireUsing("1111.2222.B2", data6);

            //data7 typically points to data in same segment as data1 points to i.e. segmentnum=0
            //To see segments i added line
            //LOG.info("key="+key+" segmentnum="+segmentNum);
            //in method : V lookupUsing(K key, V value, boolean create) in class VanillaSharedHashMap

            shm.acquireUsing("1111.2222.A3", data7);

            shm.acquireUsing("1111.2222.B3", data8);

            System.gc();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        try {
            shm.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        long startMem = memoryUsed();
        //Simple case just to prove all is OK
        data1.setLongString("AAAAAAAAAAAAAAAAAAAA");
        assertEquals("AAAAAAAAAAAAAAAAAAAA", data1.getLongString());
        data7.setLongString("BBBBBBBBBBBBBBBBBBBB");
        assertEquals("BBBBBBBBBBBBBBBBBBBB", data7.getLongString());

        System.out.println("Start threads to test concurrent read/write in same segment .. this used to fail");
        //..change data7 to another e.g. data2 and should work
        ExecutorService threads = Executors.newFixedThreadPool(2);

        Future<?> a = threads.submit(new TestTask(data1, "AAAAAAAAAAAAAAAAAAAA"));
        Future<?> b = threads.submit(new TestTask(data7, "BBBBBBBBBBBBBBBBBBBB"));
        a.get();
        b.get();
        threads.shutdown();
        long memUsed = memoryUsed() - startMem;
        System.out.printf("Used %,d KB memory%n", memUsed / 1024);
        assertEquals(1 << 20, memUsed, 1 << 20);
    }

    static long memoryUsed() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    class TestTask implements Runnable {
        TestDataValue data;
        String msg;

        TestTask(TestDataValue data, String msg) {
            this.data = data;
            this.msg = msg;
        }

        public void run() {
            for (int i = 0; i < 1000000; i++) {
                data.setLongString(msg);
                String longString = data.getLongString();
                try {
                    assertEquals(msg, longString);
                } catch (AssertionError e) {
                    System.out.println("test " + i + " " + longString);
                    String longString2 = data.getLongString();
                    System.out.println("++ test " + i + " " + longString2);
                    e.printStackTrace();
                    String longString3 = data.getLongString();
                    System.out.println("++++ test " + i + " " + longString3);
                    break;
                }
            }
        }

    }
}