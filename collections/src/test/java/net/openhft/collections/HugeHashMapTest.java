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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;

/**
 * User: plawrey Date: 07/12/13 Time: 11:48
 */
@SuppressWarnings("unchecked")
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
        Set<Integer> expectedSet = new HashSet<Integer>();
        for (int expectedKey : expectedKeys) {
            expectedSet.add(expectedKey);
        }
        org.junit.Assert.assertEquals(expectedSet, keySet);
    }

    static void assertValues(Collection<String> values, String[] expectedValues) {
        List<String> expectedList = new ArrayList<String>();
        Collections.addAll(expectedList, expectedValues);
        Collections.sort(expectedList);

        List<String> actualList = new ArrayList<String>();
        for (String actualValue : values) {
            actualList.add(actualValue);
        }
        Collections.sort(actualList);

        org.junit.Assert.assertEquals(expectedList, actualList);
    }

    static void assertEntrySet(Set<Map.Entry<Integer, String>> entrySet, int[] expectedKeys, String[] expectedValues) {
        Set<Map.Entry<Integer, String>> expectedSet = new HashSet<Map.Entry<Integer, String>>(entrySet);
        for (int i = 0; i < expectedKeys.length; i++) {
            expectedSet.add(new AbstractMap.SimpleEntry<Integer, String>(expectedKeys[i], expectedValues[i]));
        }
        org.junit.Assert.assertEquals(expectedSet, entrySet);
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
    @Ignore
    public void testPut() throws ExecutionException, InterruptedException {
        int count = 1000000;
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
    public void testPutPerf() throws ExecutionException, InterruptedException { //todo: not deterministic, sometimes it fails
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
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

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
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

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
    public void entrySetRemoveReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        entrySet.remove(new AbstractMap.SimpleEntry<Integer, String>(2, "2"));
        assertMap(map, new int[]{3, 1}, new String[]{"3", "1"});
        assertEntrySet(entrySet, new int[]{3, 1}, new String[]{"3", "1"});
        assertKeySet(keySet, new int[]{3, 1});
        assertValues(values, new String[]{"3", "1"});
    }

    @Test
    public void keySetRemoveReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        keySet.remove(2);
        assertMap(map, new int[]{3, 1}, new String[]{"3", "1"});
        assertEntrySet(entrySet, new int[]{3, 1}, new String[]{"3", "1"});
        assertKeySet(keySet, new int[]{3, 1});
        assertValues(values, new String[]{"3", "1"});
    }

    @Test
    public void valuesRemoveReflectedInMap() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        values.remove("2");
        assertMap(map, new int[]{3, 1}, new String[]{"3", "1"});
        assertEntrySet(entrySet, new int[]{3, 1}, new String[]{"3", "1"});
        assertKeySet(keySet, new int[]{3, 1});
        assertValues(values, new String[]{"3", "1"});
    }

    @Test
    public void entrySetIteratorRemoveReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        Iterator<Map.Entry<Integer, String>> entryIterator = entrySet.iterator();
        entryIterator.next();
        entryIterator.next();
        entryIterator.remove();
        assertMap(map, new int[]{2, 1}, new String[]{"2", "1"});
        assertEntrySet(entrySet, new int[]{2, 1}, new String[]{"2", "1"});
        assertKeySet(keySet, new int[]{2, 1});
        assertValues(values, new String[]{"2", "1"});
    }

    @Test
    public void keySetIteratorRemoveReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        Iterator<Integer> keyIterator = keySet.iterator();
        keyIterator.next();
        keyIterator.next();
        keyIterator.remove();
        assertMap(map, new int[]{2, 1}, new String[]{"2", "1"});
        assertEntrySet(entrySet, new int[]{2, 1}, new String[]{"2", "1"});
        assertKeySet(keySet, new int[]{2, 1});
        assertValues(values, new String[]{"2", "1"});
    }

    @Test
    public void valuesIteratorRemoveReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        Iterator<String> valueIterator = values.iterator();
        valueIterator.next();
        valueIterator.next();
        valueIterator.remove();
        assertMap(map, new int[]{2, 1}, new String[]{"2", "1"});
        assertEntrySet(entrySet, new int[]{2, 1}, new String[]{"2", "1"});
        assertKeySet(keySet, new int[]{2, 1});
        assertValues(values, new String[]{"2", "1"});
    }

    @Test
    public void entrySetRemoveAllReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        entrySet.removeAll(
                Arrays.asList(
                        new AbstractMap.SimpleEntry<Integer, String>(1, "1"),
                        new AbstractMap.SimpleEntry<Integer, String>(2, "2")
                )
        );
        assertMap(map, new int[]{3}, new String[]{"3"});
        assertEntrySet(entrySet, new int[]{3}, new String[]{"3"});
        assertKeySet(keySet, new int[]{3});
        assertValues(values, new String[]{"3"});
    }

    @Test
    public void keySetRemoveAllReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        keySet.removeAll(Arrays.asList(1, 2));
        assertMap(map, new int[]{3}, new String[]{"3"});
        assertEntrySet(entrySet, new int[]{3}, new String[]{"3"});
        assertKeySet(keySet, new int[]{3});
        assertValues(values, new String[]{"3"});
    }

    @Test
    public void valuesRemoveAllReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        values.removeAll(Arrays.asList("1", "2"));
        assertMap(map, new int[]{3}, new String[]{"3"});
        assertEntrySet(entrySet, new int[]{3}, new String[]{"3"});
        assertKeySet(keySet, new int[]{3});
        assertValues(values, new String[]{"3"});
    }

    @Test
    public void entrySetRetainAllReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        entrySet.retainAll(
                Arrays.asList(
                        new AbstractMap.SimpleEntry<Integer, String>(1, "1"),
                        new AbstractMap.SimpleEntry<Integer, String>(2, "2")
                )
        );
        assertMap(map, new int[]{2, 1}, new String[]{"2", "1"});
        assertEntrySet(entrySet, new int[]{2, 1}, new String[]{"2", "1"});
        assertKeySet(keySet, new int[]{2, 1});
        assertValues(values, new String[]{"2", "1"});
    }

    @Test
    public void keySetRetainAllReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        keySet.retainAll(Arrays.asList(1, 2));
        assertMap(map, new int[]{2, 1}, new String[]{"2", "1"});
        assertEntrySet(entrySet, new int[]{2, 1}, new String[]{"2", "1"});
        assertKeySet(keySet, new int[]{2, 1});
        assertValues(values, new String[]{"2", "1"});
    }

    @Test
    public void valuesRetainAllReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        values.retainAll(Arrays.asList("1", "2"));
        assertMap(map, new int[]{2, 1}, new String[]{"2", "1"});
        assertEntrySet(entrySet, new int[]{2, 1}, new String[]{"2", "1"});
        assertKeySet(keySet, new int[]{2, 1});
        assertValues(values, new String[]{"2", "1"});
    }

    @Test
    public void entrySetClearReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        entrySet.clear();
        org.junit.Assert.assertTrue(map.isEmpty());
        org.junit.Assert.assertTrue(entrySet.isEmpty());
        org.junit.Assert.assertTrue(keySet.isEmpty());
        org.junit.Assert.assertTrue(values.isEmpty());
    }

    @Test
    public void keySetClearReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        keySet.clear();
        org.junit.Assert.assertTrue(map.isEmpty());
        org.junit.Assert.assertTrue(entrySet.isEmpty());
        org.junit.Assert.assertTrue(keySet.isEmpty());
        org.junit.Assert.assertTrue(values.isEmpty());
    }

    @Test
    public void valuesClearReflectedInMapAndOtherViews() {
        HugeHashMap<Integer, String> map = getViewTestMap(3);
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        values.clear();
        org.junit.Assert.assertTrue(map.isEmpty());
        org.junit.Assert.assertTrue(entrySet.isEmpty());
        org.junit.Assert.assertTrue(keySet.isEmpty());
        org.junit.Assert.assertTrue(values.isEmpty());
    }

    @Test
     public void clearMapViaEntryIteratorRemoves() {
        int noOfElements = 16 * 1024;
        HugeHashMap<Integer, String> map = getViewTestMap(noOfElements);

        int sum = 0;
        for (Iterator it = map.entrySet().iterator(); it.hasNext(); ) {
            it.next();
            it.remove();
            ++sum;
        }

        assertEquals(noOfElements, sum);
    }

    @Test
    public void clearMapViaKeyIteratorRemoves() {
        int noOfElements = 16 * 1024;
        HugeHashMap<Integer, String> map = getViewTestMap(noOfElements);

        int sum = 0;
        for (Iterator it = map.keySet().iterator(); it.hasNext(); ) {
            it.next();
            it.remove();
            ++sum;
        }

        assertEquals(noOfElements, sum);
    }

    @Test
    public void clearMapViaValueIteratorRemoves() {
        int noOfElements = 16 * 1024;
        HugeHashMap<Integer, String> map = getViewTestMap(noOfElements);

        int sum = 0;
        for (Iterator it = map.values().iterator(); it.hasNext(); ) {
            it.next();
            it.remove();
            ++sum;
        }

        assertEquals(noOfElements, sum);
    }

    @Test
    public void entrySetValueReflectedInMapAndOtherViews() throws IOException {
        HugeHashMap<Integer, String> map = new HugeHashMap<Integer, String>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                Integer.class,
                String.class
        );

        map.put(1, "A");
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();
        Set<Integer> keySet = map.keySet();
        Collection<String> values = map.values();

        assertMap(map, new int[] {1}, new String[] {"A"});
        assertEntrySet(entrySet, new int[]{1}, new String[]{"A"});
        assertKeySet(keySet, new int[]{1});
        assertValues(values, new String[]{"A"});

        entrySet.iterator().next().setValue("B");
        assertMap(map, new int[]{1}, new String[]{"B"});
        assertEntrySet(entrySet, new int[]{1}, new String[]{"B"});
        assertEntrySet(map.entrySet(), new int[]{1}, new String[]{"B"});
        assertKeySet(keySet, new int[]{1});
        assertKeySet(map.keySet(), new int[]{1});
        assertValues(values, new String[]{"B"});
        assertValues(map.values(), new String[]{"B"});
    }

    @Test
    public void charSequenceMapIssue() {
        int noOfElements = 16 * 1024;
        HugeHashMap<CharSequence, CharSequence> map = new HugeHashMap<CharSequence, CharSequence>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                CharSequence.class,
                CharSequence.class
        );

        for (int i = 0; i < noOfElements; i++) {
            String string = "string" + i;
            map.put(string, string);
        }

        for (Map.Entry<CharSequence, CharSequence> entry : map.entrySet()) {
            org.junit.Assert.assertEquals(entry.getKey(), entry.getValue());
        }
    }

    private HugeHashMap<Integer, String> getViewTestMap(int noOfElements) {
        HugeHashMap<Integer, String> map = new HugeHashMap<Integer, String>(
                HugeConfig.DEFAULT.clone().setSegments(16),
                Integer.class,
                String.class
        );

        int[] expectedKeys = new int[noOfElements];
        String[] expectedValues = new String[noOfElements];
        for (int i = 1; i <= noOfElements; i++) {
            String value = "" + i;
            map.put(i, value);
            expectedKeys[i - 1] = i;
            expectedValues[i - 1] = value;
        }

//        assertEntrySet(map.entrySet(), expectedKeys, expectedValues); //todo
//        assertKeySet(map.keySet(), expectedKeys);
        assertValues(map.values(), expectedValues);

        return map;
    }
}
