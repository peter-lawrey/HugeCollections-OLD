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
 * @see SharedHashMapBuilder#tcpReplication(TcpReplication)
 */
public class TcpReplication implements Cloneable {

    private final int serverPort;
    private final Set<InetSocketAddress> endpoints;
    private short packetSize = 1024 * 8;

    private long heartBeatInterval = TimeUnit.SECONDS.toMillis(20);

    public TcpReplication(int serverPort, InetSocketAddress... endpoints) {
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

    public TcpReplication packetSize(short packetSize) {
        this.packetSize = packetSize;
        return this;
    }

    public short packetSize() {
        return packetSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TcpReplication that = (TcpReplication) o;

        if (serverPort() != that.serverPort()) return false;
        if (!endpoints().equals(that.endpoints())) return false;
        return packetSize() == that.packetSize();
    }

    @Override
    protected TcpReplication clone() {
        try {
            return (TcpReplication) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
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

    public long heartBeatInterval() {
        return heartBeatInterval;
    }

    /**
     * @param heartBeatInterval in milliseconds
     * @return
     */
    public TcpReplication heartBeatInterval(long heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
        return this;
    }
}
