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

import static junit.framework.Assert.assertNotNull;

/**
 * User: plawrey Date: 07/12/13 Time: 11:48
 */
public class HugeHashMapTest {
    static final int N_THREADS = 128;
    static final int COUNT = 50 * 1000000;
    static final long stride;

    static {
        long _stride = Long.MAX_VALUE / COUNT;
        while (_stride % 2 == 0)
            _stride--;
        stride = _stride;
    }

    @Test
    public void testPut() throws ExecutionException, InterruptedException {

        HugeConfig config = HugeConfig.DEFAULT.clone()
                .setSegments(128)
                .setSmallEntrySize(128)
                .setEntriesPerSegment(100000);

        final HugeHashMap<CharSequence, SampleValues> map =
                new HugeHashMap<CharSequence, SampleValues>(
                        config, CharSequence.class, SampleValues.class);
        long start = System.nanoTime();


        final SampleValues value = new SampleValues();
        StringBuilder user = new StringBuilder();
        int count = 4000000;
        for (int i = 0; i < count; i++)
            map.put(users(user, i), value);
        for (int i = 0; i < count; i++)
            assertNotNull(map.get(users(user, i), value));
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
                .setSmallEntrySize(128);
        config.setEntriesPerSegment(COUNT / config.getSegments());

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
                    for (int i = finalT; i < COUNT; i += N_THREADS)
                        map.put(users(user, i), value);
                    for (int i = finalT; i < COUNT; i += N_THREADS)
                        assertNotNull(map.get(users(user, i), value));
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
}
