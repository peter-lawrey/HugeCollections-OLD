/*
 * Copyright 2014 Higher Frequency Trading
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

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

abstract class AbstractReplicationBuilder<T extends AbstractReplicationBuilder<T>>
        implements Cloneable {

    private long throttle = 0;
    private TimeUnit throttlePerUnit = MILLISECONDS;
    private long throttleBucketInterval = 100;
    private TimeUnit throttleBucketIntervalUnit = MILLISECONDS;

    abstract T thisBuilder();

    /**
     * Default maximum bits is {@code 0}, i. e. there is no throttling.
     *
     * @param perUnit maximum bits is returned per this time unit
     * @return maximum bits per the given time unit
     */
    public long throttle(TimeUnit perUnit) {
        return throttlePerUnit.convert(throttle, perUnit);
    }

    /**
     * @param maxBits the preferred maximum bits. Non-positive value designates replicator shouldn't
     *                throttle.
     * @param perUnit the time unit per which maximum bits specified
     * @return this builder back
     */
    public T throttle(long maxBits, TimeUnit perUnit) {
        this.throttle = maxBits;
        this.throttlePerUnit = perUnit;
        return thisBuilder();
    }

    /**
     * Default throttle bucketing interval is 100 millis.
     *
     * @param unit the time unit of the interval
     * @return the bucketing interval for throttling
     */
    public long throttleBucketInterval(TimeUnit unit) {
        return unit.convert(throttleBucketInterval, throttleBucketIntervalUnit);
    }

    /**
     * @param throttleBucketInterval the bucketing interval for throttling
     * @param unit                   the time unit of the interval
     * @return this builder back
     * @throws IllegalArgumentException if the given bucketing interval is unrecognisably small
     *                                  for the current replicator implementation or negative.
     *                                  Current minimum interval is 1 millisecond.
     */
    public T throttleBucketInterval(long throttleBucketInterval, TimeUnit unit) {
        if (unit.toMillis(throttleBucketInterval) < 1) {
            throw new IllegalArgumentException(
                    "Minimum throttle bucketing interval is 1 millisecond, " +
                            throttleBucketInterval + " " + unit + " given");
        }
        this.throttleBucketInterval = throttleBucketInterval;
        this.throttleBucketIntervalUnit = unit;
        return thisBuilder();
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public T clone() {
        try {
            return (T) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
