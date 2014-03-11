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
import net.openhft.lang.values.LongValueNative;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class SharedHashMapTest {

    private StringBuilder sb = new StringBuilder();


    @Test
    public void testRemoveWithKey() throws Exception {

        final SharedHashMap<CharSequence, CharSequence> map = new SharedHashMapBuilder()
                .minSegments(2)
                .create(getPersistenceFile(), CharSequence.class, CharSequence.class);

        assertFalse(map.containsKey("key3"));
        map.put("key1", "one");
        map.put("key2", "two");
        assertEquals(2, map.size());

        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertFalse(map.containsKey("key3"));

        assertEquals("one", map.get("key1"));
        assertEquals("two", map.get("key2"));

        final CharSequence result = map.remove("key1");

        assertEquals(1, map.size());

        assertEquals("one", result);
        assertFalse(map.containsKey("key1"));

        assertEquals(null, map.get("key1"));
        assertEquals("two", map.get("key2"));
        assertFalse(map.containsKey("key3"));

        // lets add one more item for luck !
        map.put("key3", "three");
        assertEquals("three", map.get("key3"));
        assertTrue(map.containsKey("key3"));
        assertEquals(2, map.size());

        // and just for kicks we'll overwrite what we have
        map.put("key3", "overwritten");
        assertEquals("overwritten", map.get("key3"));
        assertTrue(map.containsKey("key3"));
        assertEquals(2, map.size());

        map.close();
    }


    @Test
    public void testSize() throws Exception {

        final SharedHashMap<CharSequence, CharSequence> map = new SharedHashMapBuilder()
                .minSegments(1024)
                .removeReturnsNull(true)
                .create(getPersistenceFile(), CharSequence.class, CharSequence.class);


        for (int i = 1; i < 1024; i++) {
            map.put("key" + i, "value");
            assertEquals(i, map.size());
        }

        for (int i = 1023; i >= 1; ) {
            map.remove("key" + i);
            i--;
            assertEquals(i, map.size());
        }
        map.close();
    }

    @Test
    public void testRemoveInteger() throws IOException {

        final SharedHashMap<Object, Object> map = new SharedHashMapBuilder()
                .create(getPersistenceFile(), Object.class, Object.class);


        int count = 2345;
        for (int i = 1; i < count; i++) {
            map.put(i, i);
            assertEquals(i, map.size());
        }

        for (int i = count - 1; i >= 1; ) {
            Integer j = (Integer) map.put(i, i);
            assertEquals(i, j.intValue());
            Integer j2 = (Integer) map.remove(i);
            assertEquals(i, j2.intValue());
            i--;
            assertEquals(i, map.size());
        }
        map.close();
    }

    @Test
    public void testRemoveWithKeyAndRemoveReturnsNull() throws Exception {

        final SharedHashMap<CharSequence, CharSequence> map = new SharedHashMapBuilder()
                .minSegments(2)
                .removeReturnsNull(true)
                .create(getPersistenceFile(), CharSequence.class, CharSequence.class);

        assertFalse(map.containsKey("key3"));
        map.put("key1", "one");
        map.put("key2", "two");
        assertEquals(2, map.size());

        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertFalse(map.containsKey("key3"));

        assertEquals("one", map.get("key1"));
        assertEquals("two", map.get("key2"));

        final CharSequence result = map.remove("key1");
        assertEquals(null, result);

        assertEquals(1, map.size());

        assertFalse(map.containsKey("key1"));

        assertEquals(null, map.get("key1"));
        assertEquals("two", map.get("key2"));
        assertFalse(map.containsKey("key3"));

        // lets add one more item for luck !
        map.put("key3", "three");
        assertEquals("three", map.get("key3"));
        assertTrue(map.containsKey("key3"));
        assertEquals(2, map.size());

        // and just for kicks we'll overwrite what we have
        map.put("key3", "overwritten");
        assertEquals("overwritten", map.get("key3"));
        assertTrue(map.containsKey("key3"));
        assertEquals(2, map.size());

        map.close();
    }


    @Test
    public void testReplaceWithKey() throws Exception {

        final SharedHashMap<CharSequence, CharSequence> map = new SharedHashMapBuilder()
                .minSegments(2)
                .create(getPersistenceFile(), CharSequence.class, CharSequence.class);


        map.put("key1", "one");
        map.put("key2", "two");
        assertEquals(2, map.size());

        assertEquals("one", map.get("key1"));
        assertEquals("two", map.get("key2"));

        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));

        final CharSequence result = map.replace("key1", "newValue");

        assertEquals("one", result);
        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertEquals(2, map.size());

        assertEquals("newValue", map.get("key1"));
        assertEquals("two", map.get("key2"));

        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertFalse(map.containsKey("key3"));

        assertEquals(2, map.size());

        // let and one more item for luck !
        map.put("key3", "three");
        assertEquals(3, map.size());

        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertTrue(map.containsKey("key3"));
        assertEquals("three", map.get("key3"));

        // and just for kicks we'll overwrite what we have
        map.put("key3", "overwritten");
        assertEquals("overwritten", map.get("key3"));

        assertTrue(map.containsKey("key1"));
        assertTrue(map.containsKey("key2"));
        assertTrue(map.containsKey("key3"));

        final CharSequence result2 = map.replace("key2", "newValue");

        assertEquals("two", result2);
        assertEquals("newValue", map.get("key2"));

        final CharSequence result3 = map.replace("rubbish", "newValue");
        assertEquals(null, result3);

        assertFalse(map.containsKey("rubbish"));
        assertEquals(3, map.size());

        map.close();
    }


    @Test
    public void testReplaceWithKeyAnd2Params() throws Exception {

        final SharedHashMap<CharSequence, CharSequence> map = new SharedHashMapBuilder()
                .minSegments(2)
                .create(getPersistenceFile(), CharSequence.class, CharSequence.class);

        map.put("key1", "one");
        map.put("key2", "two");

        assertEquals("one", map.get("key1"));
        assertEquals("two", map.get("key2"));

        final boolean result = map.replace("key1", "one", "newValue");

        assertEquals(true, result);

        assertEquals("newValue", map.get("key1"));
        assertEquals("two", map.get("key2"));

        // let and one more item for luck !
        map.put("key3", "three");
        assertEquals("three", map.get("key3"));

        // and just for kicks we'll overwrite what we have
        map.put("key3", "overwritten");
        assertEquals("overwritten", map.get("key3"));

        final boolean result2 = map.replace("key2", "two", "newValue2");

        assertEquals(true, result2);
        assertEquals("newValue2", map.get("key2"));

        final boolean result3 = map.replace("newKey", "", "newValue");
        assertEquals(false, result3);

        final boolean result4 = map.replace("key2", "newValue2", "newValue2");
        assertEquals(true, result4);

        map.close();
    }

    @Test
    public void testRemoveWithKeyAndValue() throws Exception {

        final SharedHashMap<CharSequence, CharSequence> map = new SharedHashMapBuilder()
                .minSegments(2)
                .create(getPersistenceFile(), CharSequence.class, CharSequence.class);


        map.put("key1", "one");
        map.put("key2", "two");

        assertEquals("one", map.get("key1"));
        assertEquals("two", map.get("key2"));


        // a false remove
        final boolean wasRemoved1 = map.remove("key1", "three");

        assertFalse(wasRemoved1);


        assertEquals(null, map.get("key1"), "one");
        assertEquals("two", map.get("key2"), "two");

        map.put("key1", "one");


        final boolean wasRemoved2 = map.remove("key1", "three");
        assertFalse(wasRemoved2);

        // lets add one more item for luck !
        map.put("key3", "three");
        assertEquals("three", map.get("key3"));

        // and just for kicks we'll overwrite what we have
        map.put("key3", "overwritten");
        assertEquals("overwritten", map.get("key3"));

        map.close();
    }


    @Test
    public void testAcquireWithNullKey() throws Exception {
        SharedHashMap<CharSequence, LongValue> map = getSharedMap(10 * 1000, 128, 24);
        assertNull(map.acquireUsing(null, new LongValueNative()));

        map.close();
    }

    @Test
    public void testGetWithNullKey() throws Exception {
        SharedHashMap<CharSequence, LongValue> map = getSharedMap(10 * 1000, 128, 24);
        assertNull(map.getUsing(null, new LongValueNative()));

        map.close();
    }

    @Test
    public void testAcquireWithNullContainer() throws Exception {
        SharedHashMap<CharSequence, LongValue> map = getSharedMap(10 * 1000, 128, 24);
        map.acquireUsing("key", new LongValueNative());
        assertEquals(0, map.acquireUsing("key", null).getValue());

        map.close();
    }

    @Test
    public void testGetWithNullContainer() throws Exception {
        SharedHashMap<CharSequence, LongValue> map = getSharedMap(10 * 1000, 128, 24);
        map.acquireUsing("key", new LongValueNative());
        assertEquals(0, map.getUsing("key", null).getValue());

        map.close();
    }

    @Test
    public void testGetWithoutAcquireFirst() throws Exception {
        SharedHashMap<CharSequence, LongValue> map = getSharedMap(10 * 1000, 128, 24);
        assertNull(map.getUsing("key", new LongValueNative()));

        map.close();
    }

    @Test
    public void testAcquireAndGet() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        int entries = 1000 * 1000;
        SharedHashMap<CharSequence, LongValue> map = getSharedMap(entries, 128, 24);

        LongValue value = new LongValueNative();
        LongValue value2 = new LongValueNative();
        LongValue value3 = new LongValueNative();

        for (int j = 1; j <= 3; j++) {
            for (int i = 0; i < entries; i++) {
                CharSequence userCS = getUserCharSequence(i);

                if (j > 1) {
                    assertNotNull(map.getUsing(userCS, value));
                } else {
                    map.acquireUsing(userCS, value);
                }
                assertEquals(j - 1, value.getValue());

                value.addAtomicValue(1);

                assertEquals(value2, map.acquireUsing(userCS, value2));
                assertEquals(j, value2.getValue());

                assertEquals(value3, map.getUsing(userCS, value3));
                assertEquals(j, value3.getValue());
            }
        }

        map.close();
    }

    @Test
    public void testAcquireFromMultipleThreads() throws Exception {
        SharedHashMap<CharSequence, LongValue> map = getSharedMap(1000 * 1000, 128, 24);

        CharSequence key = getUserCharSequence(0);
        map.acquireUsing(key, new LongValueNative());

        int iterations = 1000;
        int noOfThreads = 10;
        CyclicBarrier barrier = new CyclicBarrier(noOfThreads);

        Thread[] threads = new Thread[noOfThreads];
        for (int t = 0; t < noOfThreads; t++) {
            threads[t] = new Thread(new IncrementRunnable(map, key, iterations, barrier));
            threads[t].start();
        }
        for (int t = 0; t < noOfThreads; t++) {
            threads[t].join();
        }

        assertEquals(noOfThreads * iterations, map.acquireUsing(key, new LongValueNative()).getValue());

        map.close();
    }

    private static final class IncrementRunnable implements Runnable {

        private final SharedHashMap<CharSequence, LongValue> map;

        private final CharSequence key;

        private final int iterations;

        private final CyclicBarrier barrier;

        private IncrementRunnable(SharedHashMap<CharSequence, LongValue> map, CharSequence key, int iterations, CyclicBarrier barrier) {
            this.map = map;
            this.key = key;
            this.iterations = iterations;
            this.barrier = barrier;
        }

        @Override
        public void run() {
            try {
                LongValue value = new LongValueNative();
                barrier.await();
                for (int i = 0; i < iterations; i++) {
                    map.acquireUsing(key, value);
                    value.addAtomicValue(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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

    // dual E5-2650v2 @ 2.6 GHz, 128 GB: -verbose:gc -Xmx32m
    // to tmpfs
    // TODO small GC on startup should be tidied up, [GC 9216K->1886K(31744K), 0.0036750 secs]
    // 10M users, updated 16 times. Throughput 33.0M ops/sec, VmPeak: 5373848 kB, VmRSS: 544252 kB
    // 50M users, updated 16 times. Throughput 31.2 M ops/sec, VmPeak: 9091804 kB, VmRSS: 3324732 kB
    // 250M users, updated 16 times. Throughput 30.0 M ops/sec, VmPeak:	24807836 kB, VmRSS: 14329112 kB
    // 1000M users, updated 16 times, Throughput 24.1 M ops/sec, VmPeak: 85312732 kB, VmRSS: 57165952 kB
    // 2500M users, updated 16 times, Throughput 23.5 M ops/sec, VmPeak: 189545308 kB, VmRSS: 126055868 kB

    // to ext4
    // 10M users, updated 16 times. Throughput 28.4 M ops/sec, VmPeak: 5438652 kB, VmRSS: 544624 kB
    // 50M users, updated 16 times. Throughput 28.2 M ops/sec, VmPeak: 9091804 kB, VmRSS: 9091804 kB
    // 250M users, updated 16 times. Throughput 26.1 M ops/sec, VmPeak:	24807836 kB, VmRSS: 24807836 kB
    // 1000M users, updated 16 times, Throughput 1.3 M ops/sec, TODO FIX this

    @Test
    @Ignore
    public void testAcquirePerf() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException {
//        int runs = Integer.getInteger("runs", 10);
        for (int runs : new int[]{10, 50, 250, 1000, 2500}) {
            final long entries = runs * 1000 * 1000L;
            final SharedHashMap<CharSequence, LongValue> map = getSharedMap(entries * 4 / 3, 1024, 24);

            int procs = Runtime.getRuntime().availableProcessors();
            int threads = procs * 2;
            int count = runs > 500 ? runs > 1200 ? 1 : 2 : 3;
            final int independence = Math.min(procs, runs > 500 ? 8 : 4);
            for (int j = 0; j < count; j++) {
                long start = System.currentTimeMillis();
                ExecutorService es = Executors.newFixedThreadPool(procs);
                for (int i = 0; i < threads; i++) {
                    final int t = i;
                    es.submit(new Runnable() {
                        @Override
                        public void run() {
                            LongValue value = nativeLongValue();
                            StringBuilder sb = new StringBuilder();
                            int next = 50 * 1000 * 1000;
                            // use a factor to give up to 10 digit numbers.
                            int factor = Math.max(1, (int) ((10 * 1000 * 1000 * 1000L - 1) / entries));
                            for (long i = t % independence; i < entries; i += independence) {
                                sb.setLength(0);
                                sb.append("u:");
                                sb.append(i * factor);
                                map.acquireUsing(sb, value);
                                long n = value.addAtomicValue(1);
                                assert n >= 0 && n < 1000 : "Counter corrupted " + n;
                                if (t == 0 && i == next) {
                                    System.out.println(i);
                                    next += 50 * 1000 * 1000;
                                }
                            }
                        }
                    });
                }
                es.shutdown();
                es.awaitTermination(runs / 10 + 1, TimeUnit.MINUTES);
                long time = System.currentTimeMillis() - start;
                System.out.printf("Throughput %.1f M ops/sec%n", threads * entries / independence / 1000.0 / time);
            }
            printStatus();
            map.close();
        }
    }

    //  i7-3970X CPU @ 3.50GHz, hex core: -Xmx30g -verbose:gc
    // 10M users, updated 12 times. Throughput 16.2 M ops/sec, longest [Full GC 853669K->852546K(3239936K), 0.8255960 secs]
    // 50M users, updated 12 times. Throughput 13.3 M ops/sec,  longest [Full GC 5516214K->5511353K(13084544K), 3.5752970 secs]
    // 100M users, updated 12 times. Throughput 11.8 M ops/sec, longest [Full GC 11240703K->11233711K(19170432K), 5.8783010 secs]
    // 200M users, updated 12 times. Throughput 4.2 M ops/sec, longest [Full GC 25974721K->22897189K(27962048K), 21.7962600 secs]

    // dual E5-2650v2 @ 2.6 GHz, 128 GB: -verbose:gc -Xmx100g
    // 10M users, updated 16 times. Throughput 155.3 M ops/sec, VmPeak: 113291428 kB, VmRSS: 9272176 kB, [Full GC 1624336K->1616457K(7299072K), 2.5381610 secs]
    // 50M users, updated 16 times. Throughput 120.4 M ops/sec, VmPeak: 113291428 kB, VmRSS: 28436248 kB [Full GC 6545332K->6529639K(18179584K), 6.9053810 secs]
    // 250M users, updated 16 times. Throughput 114.1 M ops/sec, VmPeak: 113291428 kB, VmRSS: 76441464 kB  [Full GC 41349527K->41304543K(75585024K), 17.3217490 secs]
    // 1000M users, OutOfMemoryError.

    @Test
    @Ignore
    public void testCHMAcquirePerf() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException {
        for (int runs : new int[]{10, 50, 250, 1000, 2500}) {
            System.out.println("Testing " + runs + " million entries");
            final long entries = runs * 1000 * 1000L;
            final ConcurrentMap<String, AtomicInteger> map = new ConcurrentHashMap<String, AtomicInteger>((int) (entries * 5 / 4), 1.0f, 1024);

            int procs = Runtime.getRuntime().availableProcessors();
            int threads = procs * 2;
            int count = runs > 500 ? runs > 1200 ? 1 : 2 : 3;
            final int independence = Math.min(procs, runs > 500 ? 8 : 4);
            for (int j = 0; j < count; j++) {
                long start = System.currentTimeMillis();
                ExecutorService es = Executors.newFixedThreadPool(procs);
                for (int i = 0; i < threads; i++) {
                    final int t = i;
                    es.submit(new Runnable() {
                        @Override
                        public void run() {
                            StringBuilder sb = new StringBuilder();
                            int next = 50 * 1000 * 1000;
                            // use a factor to give up to 10 digit numbers.
                            int factor = Math.max(1, (int) ((10 * 1000 * 1000 * 1000L - 1) / entries));
                            for (long i = t % independence; i < entries; i += independence) {
                                sb.setLength(0);
                                sb.append("u:");
                                sb.append(i * factor);
                                String key = sb.toString();
                                AtomicInteger count = map.get(key);
                                if (count == null) {
                                    map.put(key, new AtomicInteger());
                                    count = map.get(key);
                                }
                                count.getAndIncrement();
                                if (t == 0 && i == next) {
                                    System.out.println(i);
                                    next += 50 * 1000 * 1000;
                                }
                            }
                        }
                    });
                }
                es.shutdown();
                es.awaitTermination(10, TimeUnit.MINUTES);
                printStatus();
                long time = System.currentTimeMillis() - start;
                System.out.printf("Throughput %.1f M ops/sec%n", threads * entries / 1000.0 / time);
            }
        }
    }

/*
    // generates garbage
    public static final Class<LongValue> nativeLongValueClass;

    static {
        DataValueGenerator dvg = new DataValueGenerator();
//        dvg.setDumpCode(true);
        try {
            nativeLongValueClass = dvg.acquireNativeClass(LongValue.class);
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }*/

    public static LongValue nativeLongValue() {
/*
        try {
            return nativeLongValueClass.newInstance();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
*/
        return new LongValueNative();
    }

    private CharSequence getUserCharSequence(int i) {
        sb.setLength(0);
        sb.append("u:");
        sb.append(i * 9876); // test 10 digit user numbers.
        return sb;
    }

    private static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime());
        file.delete();
        file.deleteOnExit();
        return file;
    }

    private static SharedHashMap<CharSequence, LongValue> getSharedMap(long entries, int segments, int entrySize) throws IOException {
        return new SharedHashMapBuilder()
                .entries(entries)
                .minSegments(segments)
                .entrySize(entrySize)
                .generatedValueType(true)
                .create(getPersistenceFile(), CharSequence.class, LongValue.class);
    }

    private static void printStatus() {
        if (!new File("/proc/self/status").exists()) return;
        try {
            BufferedReader br = new BufferedReader(new FileReader("/proc/self/status"));
            for (String line; (line = br.readLine()) != null; )
                if (line.startsWith("Vm"))
                    System.out.print(line.replaceAll("  +", " ") + ", ");
            System.out.println();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPutAndRemove() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-remove-test");
        file.delete();
        file.deleteOnExit();
        int entries = 100 * 1000;
        SharedHashMap<CharSequence, CharSequence> map =
                new SharedHashMapBuilder()
                        .entries(entries)
                        .minSegments(16)
                        .entrySize(32)
                        .putReturnsNull(true)
                        .removeReturnsNull(true)
                        .create(file, CharSequence.class, CharSequence.class);
        StringBuilder key = new StringBuilder();
        StringBuilder value = new StringBuilder();
        StringBuilder value2 = new StringBuilder();
        for (int j = 1; j <= 3; j++) {
            for (int i = 0; i < entries; i++) {
                key.setLength(0);
                key.append("user:").append(i);
                value.setLength(0);
                value.append("value:").append(i);
//                System.out.println(key);
                assertNull(map.getUsing(key, value));
                assertNull(map.put(key, value));
                assertNotNull(map.getUsing(key, value2));
                assertEquals(value.toString(), value2.toString());
                assertNull(map.remove(key));
                assertNull(map.getUsing(key, value));
            }
        }

        map.close();
    }
}
