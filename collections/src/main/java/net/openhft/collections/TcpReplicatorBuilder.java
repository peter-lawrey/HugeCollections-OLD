/*
* Copyright 2014 Higher Frequency Trading
*
* http://www.higherfrequencytrading.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package net.openhft.collections;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.collections.AbstractChannelReplicator.ChannelReplicatorBuilder;

/**
 * Configuration (builder) class for TCP replication feature of {@link SharedHashMap}.
 *
 * @see SharedHashMapBuilder#tcpReplicatorBuilder(TcpReplicatorBuilder)
 */
public final class TcpReplicatorBuilder implements ChannelReplicatorBuilder, Cloneable {

    private int serverPort;
    private Set<InetSocketAddress> endpoints;
    private int packetSize = 1024 * 8;
    private long throttleBucketInterval = 100;
    private TimeUnit throttleBucketIntervalUnit = MILLISECONDS;
    private long heartBeatInterval = 20;
    private TimeUnit heartBeatIntervalUnit = SECONDS;
    private long throttle = 0;
    private TimeUnit throttlePerUnit = MILLISECONDS;
    private int maxEntrySizeBytes;


    public TcpReplicatorBuilder(int serverPort, InetSocketAddress... endpoints) {
        this.serverPort = serverPort;
        for (final InetSocketAddress endpoint : endpoints) {
            if (endpoint.getPort() == serverPort && "localhost".equals(endpoint.getHostName()))
                throw new IllegalArgumentException("endpoint=" + endpoint
                        + " can not point to the same port as the server");
        }
        this.endpoints = unmodifiableSet(new HashSet<InetSocketAddress>(asList(endpoints)));
    }

    public int serverPort() {
        return serverPort;
    }

    public Set<InetSocketAddress> endpoints() {
        return endpoints;
    }

    public TcpReplicatorBuilder packetSize(int packetSize) {
        this.packetSize = packetSize;
        return this;
    }

    public int packetSize() {
        return packetSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TcpReplicatorBuilder that = (TcpReplicatorBuilder) o;

        if (serverPort() != that.serverPort()) return false;
        if (!endpoints().equals(that.endpoints())) return false;
        return packetSize() == that.packetSize();
    }


    @Override
    public String toString() {
        return "TcpReplication{" +
                "serverPort=" + serverPort() +
                ", endpoints=" + endpoints() +
                ", packetSize=" + packetSize() +
                "}";
    }

    public InetSocketAddress serverInetSocketAddress() {
        return new InetSocketAddress(serverPort());
    }

    public long heartBeatInterval(TimeUnit unit) {
        return unit.convert(heartBeatInterval, heartBeatIntervalUnit);
    }

    /**
     * @param heartBeatInterval heart beat interval
     * @param unit              the time unit of the interval
     * @return this builder back
     * @throws IllegalArgumentException if the given heart beat interval is unrecognisably small for the
     *                                  current TCP replicator implementation or negative. Current minimum
     *                                  interval is 1 millisecond.
     */
    public TcpReplicatorBuilder heartBeatInterval(long heartBeatInterval, TimeUnit unit) {
        if (unit.toMillis(heartBeatInterval) < 1) {
            throw new IllegalArgumentException(
                    "Minimum heart beat interval is 1 millisecond, " +
                            heartBeatInterval + " " + unit + " given");
        }
        this.heartBeatInterval = heartBeatInterval;
        this.heartBeatIntervalUnit = unit;
        return this;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public TcpReplicatorBuilder clone() {
        try {
            return (TcpReplicatorBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    private TcpReplicatorBuilder endpoints(Collection<InetSocketAddress> endpoints) {
        this.endpoints = unmodifiableSet(new HashSet<InetSocketAddress>(endpoints));
        return this;
    }

    private TcpReplicatorBuilder serverPort(int serverPort) {
        this.serverPort = serverPort;
        return this;
    }

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
     * @param maxBits the preferred maximum bits. Non-positive value designates TCP replicator shouldn't
     *                throttle.
     * @param perUnit the time unit per which maximum bits specified
     * @return this builder back
     */
    public TcpReplicatorBuilder throttle(long maxBits, TimeUnit perUnit) {
        this.throttle = maxBits;
        this.throttlePerUnit = perUnit;
        return this;
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
     * @throws IllegalArgumentException if the given bucketing interval is unrecognisably small for the
     *                                  current TCP replicator implementation or negative. Current minimum
     *                                  interval is 1 millisecond.
     */
    public TcpReplicatorBuilder throttleBucketInterval(long throttleBucketInterval, TimeUnit unit) {
        if (unit.toMillis(throttleBucketInterval) < 1) {
            throw new IllegalArgumentException(
                    "Minimum throttle bucketing interval is 1 millisecond, " +
                            throttleBucketInterval + " " + unit + " given");
        }
        this.throttleBucketInterval = throttleBucketInterval;
        this.throttleBucketIntervalUnit = unit;
        return this;
    }



}
