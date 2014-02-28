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

import net.openhft.affinity.AffinityLock;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * Created by peter on 28/02/14.
 */
public class LatencyTest {
    static final int KEYS = 10 * 1000 * 1000;
    static final int UPDATES = 60 * 1000 * 1000;
    static final int RATE = 1000 * 1000; // per second.
    static final long START_TIME = System.currentTimeMillis();

    // TODO test passes but is under development.
    @Test
    @Ignore
    public void testSHMLatency() throws IOException {
        AffinityLock lock = AffinityLock.acquireCore();
        File file = File.createTempFile("testSHMLatency", "deleteme");
        SharedHashMap<LongValue, LongValue> countersMap = new SharedHashMapBuilder()
                .entries(KEYS * 3 / 2)
                .entrySize(24)
                .generatedKeyType(true)
                .generatedValueType(true)
                .create(file, LongValue.class, LongValue.class);

        // add keys
        LongValue key = DataValueClasses.newInstance(LongValue.class);
        LongValue value = DataValueClasses.newInstance(LongValue.class);
        for (long i = 0; i < KEYS; i++) {
            key.setValue(i);
            value.setValue(0);
            countersMap.put(key, value);
        }
        System.out.println("Keys created");
        Monitor monitor = new Monitor();
        LongValue value2 = DataValueClasses.newDirectReference(LongValue.class);
        for (int t = 0; t < 5; t++) {
            Histogram times = new Histogram();
            int u = 0;
            long start = System.nanoTime();
            long delay = 1000 * 1000 * 1000L / RATE;
            long next = start + delay;
            for (long j = 0; j < UPDATES; j += KEYS) {
                int stride = Math.max(1, KEYS / UPDATES);
                // the timed part
                for (int i = 0; i < KEYS && u < UPDATES; i += stride) {
                    // busy wait for next time.
                    while (System.nanoTime() < next - 12) ;
                    long start0 = System.nanoTime();
                    monitor.sample = start0;

                    // start the update.
                    key.setValue(i);
                    LongValue using = countersMap.getUsing(key, value2);
                    if (using == null)
                        assertNotNull(using);
//                    value2.addAtomicValue(1);
                    // calculate the time using the time it should have started, not when it was able.
                    long elapse = System.nanoTime() - start0;
                    times.sample(elapse);
                    next += delay;
                }
                monitor.sample = Long.MAX_VALUE;
            }
            System.out.println("\nrun " + t);
            times.printResults();
        }
        monitor.running = false;
        countersMap.close();
        file.delete();
    }

    static class Monitor implements Runnable {
        volatile boolean running = true;
        final Thread thread;
        volatile long sample;

        Monitor() {
            this.thread = Thread.currentThread();
            sample = Long.MAX_VALUE;
            new Thread(this).start();
        }

        @Override
        public void run() {
            while (running) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
                long delay = System.nanoTime() - sample;
                if (delay > 1000 * 1000) {
                    System.out.println("\n" + (System.currentTimeMillis() - START_TIME) + " : Delay of " + delay / 100000 / 10.0 + " ms.");
                    int count = 0;
                    for (StackTraceElement ste : thread.getStackTrace()) {
                        System.out.println("\tat " + ste);
                        if (count++ > 6) break;
                    }
                }
            }
        }
    }
}
