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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

/**
 * Configuration (builder) class for TCP replication feature of {@link SharedHashMap}.
 *
 * @see SharedHashMapBuilder#tcpReplicatorBuilder(TcpReplicatorBuilder)
 */
public class TcpReplicatorBuilder implements Cloneable {

    private int serverPort;
    private Set<InetSocketAddress> endpoints;
    private int packetSize = 1024 * 8;
    private int throttleBucketIntervalMS = 100;

    private int heartBeatIntervalMS = (int) TimeUnit.SECONDS.toMillis(20);
    private long throttle;
    private boolean deletedModIteratorFileOnExit;

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
    public int hashCode() {
        return Objects.hash(serverPort, endpoints, packetSize);
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

    public int heartBeatInterval() {
        return heartBeatIntervalMS;
    }

    /**
     * @param heartBeatInterval in milliseconds, must be greater than ZERO
     * @return
     */
    public TcpReplicatorBuilder heartBeatIntervalMS(int heartBeatInterval) {
        if (heartBeatInterval <= 0) throw new IllegalArgumentException("heartBeatInterval must be greater " +
                "than zero");
        this.heartBeatIntervalMS = heartBeatInterval;
        return this;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public TcpReplicatorBuilder clone() {
        try {
            final TcpReplicatorBuilder result = (TcpReplicatorBuilder) super.clone();
            result.endpoints(new HashSet<InetSocketAddress>(this.endpoints()));
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    private TcpReplicatorBuilder endpoints(Set<InetSocketAddress> endpoints) {
        this.endpoints = endpoints;
        return this;
    }

    private TcpReplicatorBuilder serverPort(int serverPort) {
        this.serverPort = serverPort;
        return this;
    }

    /**
     * @return bits per seconds
     */
    public long throttle() {
        return this.throttle;
    }

    /**
     * @param throttleInBitPerSecond the preferred maximum bit per seconds.
     * @return this
     */
    public TcpReplicatorBuilder throttle(long throttleInBitPerSecond) {
        this.throttle = throttleInBitPerSecond;
        return this;
    }


    /**
     * @return in milliseconds the size of the bucket for the token bucket algorithm
     */
    public int throttleBucketIntervalMS() {
        return throttleBucketIntervalMS;
    }

    /**
     * @param throttleBucketInterval in milliseconds the size of the bucket for the token bucket algorithm
     * @return this
     */
    public TcpReplicatorBuilder throttleBucketIntervalMS(int throttleBucketInterval) {
        this.throttleBucketIntervalMS = throttleBucketInterval;
        return this;
    }

    public int minIntervalMS() {
        return Math.min(throttleBucketIntervalMS, heartBeatIntervalMS);
    }

    /**
     * often used by unit test, to clear up the file generated by the ModIterator on exit
     *
     * @return true if the file should be deleted upon exit of the JVM, see {@link
     * java.io.File#deleteOnExit()}
     */
    public boolean deletedModIteratorFileOnExit() {
        return deletedModIteratorFileOnExit;
    }

    public TcpReplicatorBuilder deletedModIteratorFileOnExit(boolean deletedModIteratorFileOnExit) {
        this.deletedModIteratorFileOnExit = deletedModIteratorFileOnExit;
        return this;
    }
}
