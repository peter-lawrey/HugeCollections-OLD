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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    // i7-3970X CPU @ 3.50GHz, hex core: -verbose:gc -Xmx64m
    // to tmpfs file system
    // 10M users, updated 12 times. Throughput 19.3 M ops/sec, no GC!
    // 50M users, updated 12 times. Throughput 19.8 M ops/sec, no GC!
    // 100M users, updated 12 times. Throughput 19.0M ops/sec, no GC!
    // 200M users, updated 12 times. Throughput 18.4 M ops/sec, no GC!
    // 400M users, updated 12 times. Throughput 18.4 M ops/sec, no GC!

    // to ext4 file system.
    // 10M users, updated 12 times. Throughput 17.7 M ops/sec, no GC!
    // 50M users, updated 12 times. Throughput 16.5 M ops/sec, no GC!
    // 100M users, updated 12 times. Throughput 15.9 M ops/sec, no GC!
    // 200M users, updated 12 times. Throughput 15.4 M ops/sec, no GC!
    // 400M users, updated 12 times. Throughput 7.8 M ops/sec, no GC!
    // 600M users, updated 12 times. Throughput 5.8 M ops/sec, no GC!

    @Test
    @Ignore
    public void testAcquirePerf() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test");
        file.delete();
        file.deleteOnExit();
        final long entries = 200 * 1000 * 1000L;
        final SharedHashMap<CharSequence, LongValue> map =
                new SharedHashMapBuilder()
                        .entries(entries)
                        .segments(1024)
                        .entrySize(24) // TODO not enough protection from over sized entries.
                        .create(file, CharSequence.class, LongValue.class);
//        DataValueGenerator dvg = new DataValueGenerator();
//        dvg.setDumpCode(true);
//        LongValue value = (LongValue) dvg.acquireNativeClass(LongValue.class).newInstance();
        int threads = Runtime.getRuntime().availableProcessors();
        long start = System.currentTimeMillis();
        ExecutorService es = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            final int t = i;
            es.submit(new Runnable() {
                @Override
                public void run() {
                    LongValue value = new LongValue£native();
                    StringBuilder sb = new StringBuilder();
                    int next = 50 * 1000 * 1000;
                    for (int i = 0; i < entries; i++) {
                        sb.setLength(0);
                        sb.append("user:");
                        sb.append(i);
                        map.acquireUsing(sb, value);
                        long n = value.addAtomicValue(1);
                        if (t == 0 && i == next) {
                            System.out.println(i);
                            next += 50 * 1000 * 1000;
                        }
                    }
                }
            });
        }
        es.shutdown();
        es.awaitTermination(30, TimeUnit.MINUTES);
        long time = System.currentTimeMillis() - start;
        System.out.printf("Throughput %.1f M ops/sec%n", threads * entries / 1000.0 / time);
        map.close();
        printStatus();
    }

    private void printStatus() {
        try {
            BufferedReader br = new BufferedReader(new FileReader("/proc/self/status"));
            for (String line; (line = br.readLine()) != null; )
                System.out.println(line);
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //  i7-3970X CPU @ 3.50GHz, hex core: -Xmx30g -verbose:gc
    // 10M users, updated 12 times. Throughput 16.2 M ops/sec, longest [Full GC 853669K->852546K(3239936K), 0.8255960 secs]
    // 50M users, updated 12 times. Throughput 13.3 M ops/sec,  longest [Full GC 5516214K->5511353K(13084544K), 3.5752970 secs]
    // 100M users, updated 12 times. Throughput 11.8 M ops/sec, longest [Full GC 11240703K->11233711K(19170432K), 5.8783010 secs]
    // 200M users, updated 12 times. Throughput 4.2 M ops/sec, longest [Full GC 25974721K->22897189K(27962048K), 21.7962600 secs]

    @Test
    @Ignore
    public void testCHMAcquirePerf() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException {
        final long entries = 200 * 1000 * 1000L;
        final ConcurrentMap<String, AtomicInteger> map = new ConcurrentHashMap<String, AtomicInteger>((int) (entries * 5 / 4), 1.0f, 1024);

        int threads = Runtime.getRuntime().availableProcessors();
        long start = System.currentTimeMillis();
        ExecutorService es = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            es.submit(new Runnable() {
                @Override
                public void run() {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < entries; i++) {
                        sb.setLength(0);
                        sb.append("user:");
                        sb.append(i);
                        String key = sb.toString();
                        AtomicInteger count = map.get(key);
                        if (count == null) {
                            map.put(key, new AtomicInteger());
                            count = map.get(key);
                        }
                        count.getAndIncrement();
                    }
                }
            });
        }
        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
        long time = System.currentTimeMillis() - start;
        System.out.printf("Throughput %.1f M ops/sec%n", threads * entries / 1000.0 / time);
        printStatus();
    }
}
