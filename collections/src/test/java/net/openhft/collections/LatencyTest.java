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

import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertNotNull;

/**
 * Created by peter on 28/02/14.
 */
public class LatencyTest {
    static final int KEYS = 5 * 1000 * 1000;
    static final int RATE = 500 * 1000; // per second.

    @Test
    @Ignore
    public void testSHMLatency() throws IOException {
        File file = File.createTempFile("testSHMLatency", "deleteme");
        SharedHashMap<LongValue, LongValue> countersMap = new SharedHashMapBuilder()
                .entries(KEYS * 2)
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

        long[] times = new long[KEYS];
        LongValue value2 = DataValueClasses.newDirectReference(LongValue.class);
        for (int t = 0; t < 5; t++) {
            long start = System.nanoTime();
            long delay = 1000 * 1000 * 1000L / RATE;
            long next = start + delay;
            // the timed part
            for (int i = 0; i < KEYS; i++) {
                // busy wait for next time.
                while (System.nanoTime() < next) ;
                // start the update.
                assertNotNull(countersMap.getUsing(key, value2));
                value2.addAtomicValue(1);
                // calculate the time using the time it should have started, not when it was able.
                long elapse = System.nanoTime() - next;
                times[i] = elapse;
                next += delay;
            }
            Arrays.sort(times);
            System.out.printf("For a rate of %,d per second, the 50/99 // 99.9/99.99%% (worst) latencies were %,d/%,d // %,d/%,d (%,d) ns%n",
                    RATE,
                    times[times.length / 2],
                    times[times.length - times.length / 100],
                    times[times.length - times.length / 1000],
                    times[times.length - times.length / 10000],
                    times[times.length - 1]
            );
        }

        countersMap.close();
        file.delete();
    }
}
