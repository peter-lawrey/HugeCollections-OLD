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

/**
 * Created by peter on 28/02/14.
 */
public class Histogram {
    static final int BITS_OF_ACCURACY = 5;
    static final int MANTISSA = 52;
    static final int BITS_TO_TRUNCATE = MANTISSA - BITS_OF_ACCURACY;
    final int[] counters = new int[30 << BITS_OF_ACCURACY];

    public void sample(long value) {
        long rawValue = Double.doubleToRawLongBits(value) >> BITS_TO_TRUNCATE;
        int bucket = (int) rawValue - (1023 << BITS_OF_ACCURACY);
        counters[bucket]++;
    }

    public void printResults() {
        for (int i = 0; i < counters.length; i++) {
            if (counters[i] == 0) continue;
            double d = Double.longBitsToDouble((i + (1023L << BITS_OF_ACCURACY)) << BITS_TO_TRUNCATE);
            if (d < 10000 && d * counters[i] < 20000) continue;
            System.out.println((long) d + ":" + counters[i]);
        }
    }
}
