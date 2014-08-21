/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import java.io.IOException;
import java.net.InetAddress;

import static net.openhft.collections.SharedHashMapBuilder.UDP_REPLICATION_MODIFICATION_ITERATOR_ID;

/**
 * @author Rob Austin.
 */
public final class ClusterReplicatorBuilder {

    final byte identifier;
    final int maxEntrySize;

    private UdpReplicationConfig udpReplicationConfig = null;
    private TcpReplicationConfig tcpReplicationConfig = null;
    int maxNumberOfChronicles = 128;

    ClusterReplicatorBuilder(byte identifier, final int maxEntrySize) {
        this.identifier = identifier;
        this.maxEntrySize = maxEntrySize;
        if (identifier <= 0) {
            throw new IllegalArgumentException("Identifier must be positive, identifier=" +
                    identifier);
        }
    }

    public ClusterReplicatorBuilder udpReplication(UdpReplicationConfig replicationConfig) {
        this.udpReplicationConfig = replicationConfig;
        return this;
    }

    public ClusterReplicatorBuilder tcpReplication(TcpReplicationConfig replicationConfig) {
        this.tcpReplicationConfig = replicationConfig;
        return this;
    }

    public ClusterReplicatorBuilder maxNumberOfChronicles(int maxNumberOfChronicles) {
        this.maxNumberOfChronicles = maxNumberOfChronicles;
        return this;
    }

    public ClusterReplicator create() throws IOException {
        final ClusterReplicator clusterReplicator = new ClusterReplicator(this);
        if (tcpReplicationConfig != null) {
            final TcpReplicator tcpReplicator = new TcpReplicator(
                    clusterReplicator.asReplica,
                    clusterReplicator.asEntryExternalizable,
                    tcpReplicationConfig,
                    maxEntrySize);
            clusterReplicator.add(tcpReplicator);
        }

        if (udpReplicationConfig != null) {
            final UdpReplicator udpReplicator =
                    new UdpReplicator(clusterReplicator.asReplica,
                            clusterReplicator.asEntryExternalizable,
                            udpReplicationConfig,
                            maxEntrySize);
            clusterReplicator.add(udpReplicator);
        }
        return clusterReplicator;
    }
}
