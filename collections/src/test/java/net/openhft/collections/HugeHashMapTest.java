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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;

/**
 * User: plawrey Date: 07/12/13 Time: 11:48
 */
public class HugeHashMapTest {
    static final int N_THREADS = 128;
    // 32M needs 5 GB of memory
    // 64M needs 10 GB of memory
    // 128M needs 20 GB of memory
    // 256M needs 40 GB of memory
    static final int COUNT = 8 * 1000000;
    static final long stride;

    static {
        long _stride = Long.MAX_VALUE / COUNT;
        while (_stride % 2 == 0)
            _stride--;
        stride = _stride;
    }

    @Test
    public void testPut() throws ExecutionException, InterruptedException {

        int count = 4000000;
        HugeConfig config = HugeConfig.DEFAULT.clone()
                .setSegments(128)
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

    static void assertEquals(long a, long b) {
        if (a != b)
            org.junit.Assert.assertEquals(a, b);
    }

    static void assertEquals(double a, double b, double err) {
        if (a != b)
            org.junit.Assert.assertEquals(a, b, err);
    }

    @Test
    public void testPutPerf() throws ExecutionException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        // use a simple pseudo-random distribution over 64-bits

        System.out.println("Starting test");

        HugeConfig config = HugeConfig.DEFAULT.clone()
                .setSegments(64)
                .setSmallEntrySize(72)
                .setCapacity(COUNT);

        final HugeHashMap<CharSequence, SampleValues> map =
                new HugeHashMap<CharSequence, SampleValues>(
                        config, CharSequence.class, SampleValues.class);
        final int COUNT = 500000;
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
}
