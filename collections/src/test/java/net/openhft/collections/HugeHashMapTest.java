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

import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;

/**
 * User: plawrey Date: 07/12/13 Time: 11:48
 */
public class HugeHashMapTest {
    static final int N_THREADS = 32;
    // 32M needs 5 GB of memory
    // 64M needs 10 GB of memory
    // 128M needs 20 GB of memory
    // 256M needs 40 GB of memory
    static final int COUNT = 4 * 1000000;
    static final long stride;

    static {
        long _stride = Long.MAX_VALUE / COUNT;
        while (_stride % 2 == 0)
            _stride--;
        stride = _stride;
    }

    static void assertEquals(long a, long b) {
        if (a != b)
            org.junit.Assert.assertEquals(a, b);
    }

    static void assertEquals(double a, double b, double err) {
        if (a != b)
            org.junit.Assert.assertEquals(a, b, err);
    }

    static void assertKeySet(Set<Integer> keySet, int[] expectedKeys) {
        assertEquals(expectedKeys.length, keySet.size());

        Iterator<Integer> keyIterator = keySet.iterator();
        for (int i = 0; i < expectedKeys.length; i++) {
            org.junit.Assert.assertEquals("On position " + i, new Integer(expectedKeys[i]), keyIterator.next());
        }
    }

    static void assertValues(Collection<String> valueSet, String[] expectedValues) {
        assertEquals(expectedValues.length, valueSet.size());

        Iterator<String> valueIterator = valueSet.iterator();
        for (int i = 0; i < expectedValues.length; i++) {
            org.junit.Assert.assertEquals("On position " + i, expectedValues[i], valueIterator.next());
        }
    }

    static void assertEntrySet(Set<Map.Entry<Integer, String>> entrySet, int[] expectedKeys, String[] expectedValues) {
        assertEquals(expectedKeys.length, entrySet.size());

        Iterator<Map.Entry<Integer, String>> entryIterator = entrySet.iterator();
        for (int i = 0; i < expectedKeys.length; i++) {
            Map.Entry<Integer, String> entry = entryIterator.next();
            org.junit.Assert.assertEquals("On position " + i, new Integer(expectedKeys[i]), entry.getKey());
            org.junit.Assert.assertEquals("On position " + i, expectedValues[i], entry.getValue());
        }
    }

    static void assertMap(Map<Integer, String> map, int[] expectedKeys, String[] expectedValues) {
        assertEquals(expectedKeys.length, map.size());
        for (int i = 0; i < expectedKeys.length; i++) {
            org.junit.Assert.assertEquals("On position " + i, expectedValues[i], map.get(expectedKeys[i]));
        }
    }

    /*
    Put/getUsing 1,339 K operations per second
     */
    @Test
    public void testPut() throws ExecutionException, InterruptedException {
        int count = 4000000;
        HugeConfig config = HugeConfig.DEFAULT.clone()
                .setSegments(256)
                .setSmallEntrySize(72) // TODO 64 corrupts the values !!
                .setCapacity(count);

        final HugeHashMap<CharSequence, SampleValues> map =
                new HugeHashMap<CharSequence, SampleValues>(
                        config, CharSequence.class, SampleValues.class);
        long start = System.nanoTime();
        final SampleValues value = new SampleValues();
        StringBuilder user = new StringBuilder();
        for (int i = 0; i < count; i++) {
            value.ee = i;
            value.gg = i;
            value.ii = i;
            map.put(users(user, i), value);
        }
        for (int i = 0; i < count; i++) {
            assertNotNull(map.get(users(user, i), value));
            assertEquals(i, value.ee);
            assertEquals(i, value.gg, 0.0);
            assertEquals(i, value.ii);
        }
        for (int i = 0; i < count; i++)
            assertNotNull(map.get(users(user, i), value));
        for (int i = 0; i < count; i++)
            map.remove(users(user, i));
        long time = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second%n",
                (int) (count * 4 * 1e6 / time));
    }

    @Test
    public void testPutPerf() throws ExecutionException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        // use a simple pseudo-random distribution over 64-bits

        System.out.println("Starting test");

        HugeConfig config = HugeConfig.DEFAULT.clone()
                .setSegments(128)
                .setSmallEntrySize(72)
                .setCapacity(COUNT);

        final HugeHashMap<CharSequence, SampleValues> map =
                new HugeHashMap<CharSequence, SampleValues>(
                        config, CharSequence.class, SampleValues.class);

        final String[] users = new String[COUNT];
        for (int i = 0; i < COUNT; i++) users[i] = "user:" + i;

        long start = System.nanoTime();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (int t = 0; t < N_THREADS; t++) {
            final int finalT = t;
            futures.add(es.submit(new Runnable() {
                @Override
                public void run() {
                    final SampleValues value = new SampleValues();
                    StringBuilder user = new StringBuilder();
                    for (int i = finalT; i < COUNT; i += N_THREADS) {
                        value.ee = i;
                        value.gg = i;
                        value.ii = i;
                        map.put(users(user, i), value);
                    }
                    for (int i = finalT; i < COUNT; i += N_THREADS) {
                        assertNotNull(map.get(users(user, i), value));
                        assertEquals(i, value.ee);
                        assertEquals(i, value.gg, 0.0);
                        assertEquals(i, value.ii);
                    }
                    for (int i = finalT; i < COUNT; i += N_THREADS)
                        assertNotNull(map.get(users(user, i), value));
                    for (int i = finalT; i < COUNT; i += N_THREADS)
                        map.remove(users(user, i));
                }
            }));
        }
        for (Future<?> future : futures)
            future.get();
        long time = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second%n",
                (int) (COUNT * 4 * 1e6 / time));
        es.shutdown();
    }

    CharSequence users(StringBuilder user, int i) {
        user.setLength(0);
        user.append("user:");
        user.append(i);
        return user;
    }

    @Test
    public void testPutLong() {
        HugeConfig config = HugeConfig.DEFAULT.clone();

        HugeHashMap<Long, Long> map1 =
                new HugeHashMap<Long, Long>(config, Long.class, Long.class);

        long key = 55;

        map1.put(key, 10L);
        assertEquals(10L, map1.get(key));
        map1.put(key, 20L);
        assertEquals(20L, map1.get(key));
        map1.clear();
    }

    @Test
    public void mapRemoveReflectedInViews() {
        HugeHashMap<Integer, String> map = new HugeHashMap<Integer, String>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                Integer.class,
                String.class
        );
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        Set<Integer> keySet = map.keySet();
        assertKeySet(keySet, new int[]{2, 3, 1});
        Collection<String> values = map.values();
        assertValues(values, new String[]{"2", "3", "1"});


        map.remove(2);
        assertMap(map, new int[]{3, 1}, new String[]{"3", "1"});
        assertEntrySet(entrySet, new int[]{3, 1}, new String[]{"3", "1"});
        assertEntrySet(map.entrySet(), new int[]{3, 1}, new String[]{"3", "1"});
        assertKeySet(keySet, new int[]{3, 1});
        assertKeySet(map.keySet(), new int[]{3, 1});
        assertValues(values, new String[]{"3", "1"});
        assertValues(map.values(), new String[]{"3", "1"});
    }

    @Test
    public void mapPutReflectedInViews() {
        HugeHashMap<Integer, String> map = new HugeHashMap<Integer, String>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                Integer.class,
                String.class
        );
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        Set<Integer> keySet = map.keySet();
        assertKeySet(keySet, new int[]{2, 3, 1});
        Collection<String> values = map.values();
        assertValues(values, new String[]{"2", "3", "1"});

        map.put(4, "4");
        assertMap(map, new int[]{4, 2, 3, 1}, new String[]{"4", "2", "3", "1"});
        assertEntrySet(entrySet, new int[]{4, 2, 3, 1}, new String[]{"4", "2", "3", "1"});
        assertEntrySet(map.entrySet(), new int[]{4, 2, 3, 1}, new String[]{"4", "2", "3", "1"});
        assertKeySet(keySet, new int[]{4, 2, 3, 1});
        assertKeySet(map.keySet(), new int[]{4, 2, 3, 1});
        assertValues(values, new String[]{"4", "2", "3", "1"});
        assertValues(map.values(), new String[]{"4", "2", "3", "1"});
    }

    @Test
    public void viewRemoveReflectedInMap() {
        HugeHashMap<Integer, String> map = new HugeHashMap<Integer, String>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                Integer.class,
                String.class
        );
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        Set<Integer> keySet = map.keySet();
        assertKeySet(keySet, new int[]{2, 3, 1});
        Collection<String> values = map.values();
        assertValues(values, new String[]{"2", "3", "1"});

        entrySet.remove(new AbstractMap.SimpleEntry<Integer, String>(2, "2"));
        assertMap(map, new int[]{3, 1}, new String[]{"3", "1"});
        assertEntrySet(entrySet, new int[]{3, 1}, new String[]{"3", "1"});
        assertKeySet(keySet, new int[]{3, 1});
        assertValues(values, new String[]{"3", "1"});

        map.put(2, "2");
        assertMap(map, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        assertKeySet(keySet, new int[]{2, 3, 1});
        assertValues(values, new String[]{"2", "3", "1"});

        keySet.remove(2);
        assertMap(map, new int[]{3, 1}, new String[]{"3", "1"});
        assertEntrySet(entrySet, new int[]{3, 1}, new String[]{"3", "1"});
        assertKeySet(keySet, new int[]{3, 1});
        assertValues(values, new String[]{"3", "1"});

        map.put(2, "2");
        assertMap(map, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        assertKeySet(keySet, new int[]{2, 3, 1});
        assertValues(values, new String[]{"2", "3", "1"});
    }

    @Test
    public void setIteratorRemoveReflectedInMap() {
        HugeHashMap<Integer, String> map = new HugeHashMap<Integer, String>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                Integer.class,
                String.class
        );
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        Set<Integer> keySet = map.keySet();
        assertKeySet(keySet, new int[]{2, 3, 1});

        Iterator<Map.Entry<Integer, String>> entryIterator = entrySet.iterator();
        entryIterator.next();
        entryIterator.next();
        entryIterator.remove();
        assertEntrySet(entrySet, new int[]{2, 1}, new String[]{"2", "1"});
        assertMap(map, new int[]{2, 1}, new String[]{"2", "1"});
        assertKeySet(keySet, new int[]{2, 1});

        map.put(3, "3");
        assertMap(map, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});
        assertKeySet(keySet, new int[]{2, 3, 1});

        Iterator<Integer> keyIterator = keySet.iterator();
        keyIterator.next();
        keyIterator.next();
        keyIterator.remove();
        assertEntrySet(entrySet, new int[]{2, 1}, new String[]{"2", "1"});
        assertMap(map, new int[]{2, 1}, new String[]{"2", "1"});
        assertKeySet(keySet, new int[]{2, 1});
    }

    @Test
    public void entrySetRemoveAllReflectedInMap() {
        HugeHashMap<Integer, String> map = new HugeHashMap<Integer, String>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                Integer.class,
                String.class
        );

        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");

        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});

        entrySet.removeAll(
                Arrays.asList(
                        new AbstractMap.SimpleEntry<Integer, String>(1, "1"),
                        new AbstractMap.SimpleEntry<Integer, String>(2, "2")
                )
        );
        assertEntrySet(entrySet, new int[]{3}, new String[]{"3"});

        org.junit.Assert.assertEquals(1, map.size());
        org.junit.Assert.assertNull(map.get(1));
        org.junit.Assert.assertNull(map.get(2));
        org.junit.Assert.assertEquals("3", map.get(3));
    }

    @Test
    public void entrySetRetainAllReflectedInMap() {
        HugeHashMap<Integer, String> map = new HugeHashMap<Integer, String>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                Integer.class,
                String.class
        );

        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");

        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});

        entrySet.retainAll(
                Arrays.asList(
                        new AbstractMap.SimpleEntry<Integer, String>(1, "1"),
                        new AbstractMap.SimpleEntry<Integer, String>(2, "2")
                )
        );
        assertEntrySet(entrySet, new int[]{2, 1}, new String[]{"2", "1"});

        org.junit.Assert.assertEquals(2, map.size());
        org.junit.Assert.assertEquals("1", map.get(1));
        org.junit.Assert.assertEquals("2", map.get(2));
        org.junit.Assert.assertNull(map.get(3));
    }

    @Test
    public void entrySetClearReflectedInMap() {
        HugeHashMap<Integer, String> map = new HugeHashMap<Integer, String>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                Integer.class,
                String.class
        );

        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");

        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        assertEntrySet(entrySet, new int[]{2, 3, 1}, new String[]{"2", "3", "1"});

        entrySet.clear();
        org.junit.Assert.assertTrue(entrySet.isEmpty());
        org.junit.Assert.assertTrue(map.isEmpty());
    }
}
