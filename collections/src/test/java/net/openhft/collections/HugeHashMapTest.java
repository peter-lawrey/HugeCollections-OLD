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
 * User: plawrey
 * Date: 07/12/13
 * Time: 11:48
 */
public class HugeHashMapTest {
    private static final int N_THREADS = 4;

    @Test
    public void testPut() throws ExecutionException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(N_THREADS);
        long start = System.nanoTime();
        HugeConfig config = HugeConfig.DEFAULT.clone()
                .setSmallEntrySize(128)
                .setEntriesPerSegment(100000);

        final HugeHashMap<String, SampleValues> map =
                new HugeHashMap<String, SampleValues>(
                        config, String.class, SampleValues.class);
        final int COUNT = 5000000;
        final String[] users = new String[COUNT];
        for (int i = 0; i < COUNT; i++) users[i] = "user:" + i;

        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (int t = 0; t < N_THREADS; t++) {
            final int finalT = t;
            futures.add(es.submit(new Runnable() {
                @Override
                public void run() {
                    final SampleValues value = new SampleValues();
                    for (int i = finalT; i < COUNT; i += N_THREADS)
                        map.put(users[i], value);
                    for (int i = finalT; i < COUNT; i += N_THREADS)
                        assertNotNull(map.get(users[i], value));
                    for (int i = COUNT - 1 - finalT; i >= 0; i -= N_THREADS)
                        assertNotNull(map.get(users[i], value));
                    for (int i = COUNT - 1 - finalT; i >= 0; i -= N_THREADS)
                        map.remove(users[i]);
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
}
