/*
 * Copyright 2013 Peter Lawrey
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

package net.openhft.collections;

import net.openhft.lang.values.LongValue;
import net.openhft.lang.values.LongValue£native;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SharedHashMapTest {
    @Test
    public void testAcquire() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test");
        file.delete();
        file.deleteOnExit();
        int entries = /*100 **/ 1000 * 1000;
        SharedHashMap<CharSequence, LongValue> map =
                new SharedHashMapBuilder()
                        .entries(entries)
                        .segments(128)
                        .entrySize(24) // TODO not enough protection from over sized entries.
                        .create(file, CharSequence.class, LongValue.class);
//        DataValueGenerator dvg = new DataValueGenerator();
//        dvg.setDumpCode(true);
//        LongValue value = (LongValue) dvg.acquireNativeClass(LongValue.class).newInstance();
        LongValue value = new LongValue£native();
        LongValue value2 = new LongValue£native();
        LongValue value3 = new LongValue£native();
        StringBuilder sb = new StringBuilder();
        for (int j = 1; j <= 3; j++) {
            for (int i = 0; i < entries; i++) {
                sb.setLength(0);
                sb.append("user:");
                sb.append(i);
//                System.out.println(sb);
//                if (i == 10)
//                    Thread.yield();
                if (j > 1)
                    assertNotNull(map.getUsing(sb, value));
                else
                    map.acquireUsing(sb, value);
                long n = value.addAtomicValue(1);
                if (n != j)
                    assertEquals(j, n);
                map.acquireUsing(sb, value2);
                long n2 = value2.getValue();
                if (n2 != j)
                    assertEquals(j, n2);
                assertEquals(value3, map.getUsing(sb, value3));
                long n3 = value3.getValue();
                if (n3 != 1)
                    assertEquals(j, n3);
            }
        }

        map.close();
    }

    @Test
    @Ignore
    public void testAcquirePerf() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test");
        file.delete();
        file.deleteOnExit();
        final int entries = 10 * 1000 * 1000;
        final SharedHashMap<CharSequence, LongValue> map =
                new SharedHashMapBuilder()
                        .entries(entries)
                        .segments(128)
                        .entrySize(24) // TODO not enough protection from over sized entries.
                        .create(file, CharSequence.class, LongValue.class);
//        DataValueGenerator dvg = new DataValueGenerator();
//        dvg.setDumpCode(true);
//        LongValue value = (LongValue) dvg.acquireNativeClass(LongValue.class).newInstance();
        int threads = Runtime.getRuntime().availableProcessors();
        long start = System.currentTimeMillis();
        ExecutorService es = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            es.submit(new Runnable() {
                @Override
                public void run() {
                    LongValue value = new LongValue£native();
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < entries; i++) {
                        sb.setLength(0);
                        sb.append("user:");
                        sb.append(i);
                        map.acquireUsing(sb, value);
                        long n = value.addAtomicValue(1);
                    }
                }
            });
        }
        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
        long time = System.currentTimeMillis() - start;
        System.out.printf("Throughput %.1f M ops/sec%n", threads * entries / 1000.0 / time);
        map.close();
    }
}
