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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static net.openhft.collections.SharedHashMapBuilder.UDP_REPLICATION_MODIFICATION_ITERATOR_ID;

/**
 * @author Rob Austin.
 */
public class ClusterReplicatorBuilder<K, V> {

    Set<Closeable> closeables = new HashSet<Closeable>();

    private final byte identifier;
    private UdpReplicatorBuilder udpReplicatorBuilder = null;
    private TcpReplicatorBuilder tcpReplicatorBuilder = null;
    private final List<SharedHashMapBuilder> sharedHashMapBuilders = new ArrayList<SharedHashMapBuilder>();
    private int bufferSize = 1024;

    private TcpReplicatorBuilder tcpClusterReplicator;
    private ClusterReplicator<K, V> udpClusterReplicator;


    ClusterReplicatorBuilder(byte identifier) {
        this.identifier = identifier;
    }

    List<ReplicaExternalizable<K, V>> replicas = new CopyOnWriteArrayList<ReplicaExternalizable<K, V>>();

    public ClusterReplicatorBuilder udpReplicator(UdpReplicatorBuilder udpReplicatorBuilder) throws IOException {
        this.udpReplicatorBuilder = udpReplicatorBuilder;
        return this;
    }

    public ClusterReplicatorBuilder<K, V> tcpReplicatorBuilder(TcpReplicatorBuilder tcpReplicatorBuilder) {
        this.tcpClusterReplicator = tcpReplicatorBuilder;
        return this;
    }

    public SharedHashMap createSharedHashMap(SharedHashMapBuilder builder) throws IOException {

        if (builder == null || !builder.file().exists())
            throw new FileNotFoundException("Unable to create file");

        if (!builder.canReplicate())
            throw new IllegalArgumentException("Please set the 'identifier', " +
                    "as this is required for clustering.");


        if (builder.identifier() <= 0)
            throw new IllegalArgumentException("Identifier must be positive, " + builder.identifier() + " given");

        final VanillaSharedReplicatedHashMap<K, V> result =
                new VanillaSharedReplicatedHashMap<K, V>(builder, builder.file(), builder.<K>kClass(), builder.<V>kClass());

        replicas.add(result);

        return result;
    }


    public ClusterReplicator create() throws IOException {

        final ClusterReplicator<K, V> clusterReplicator = new ClusterReplicator<K, V>(identifier);
        clusterReplicator.addAll(replicas);

        if (tcpReplicatorBuilder != null) {
            final TcpReplicator tcpReplicator = new TcpReplicator(clusterReplicator, clusterReplicator, tcpReplicatorBuilder,
                    bufferSize);
            closeables.add(tcpReplicator);
            clusterReplicator.add(tcpReplicator);
        }

        if (udpReplicatorBuilder != null) {
            final InetAddress address = udpReplicatorBuilder.address();

            if (address == null)
                throw new IllegalArgumentException("address can not be null");

            if (address.isMulticastAddress() && udpReplicatorBuilder.networkInterface() == null) {
                throw new IllegalArgumentException("MISSING: NetworkInterface, " +
                        "When using a multicast addresses, please provided a  networkInterface");
            }

            final UdpReplicator udpReplicator =
                    new UdpReplicator(clusterReplicator,
                            udpReplicatorBuilder.clone(),
                            bufferSize,
                            identifier,
                            UDP_REPLICATION_MODIFICATION_ITERATOR_ID);

            closeables.add(udpReplicator);
            clusterReplicator.add(udpReplicator);
        }

        this.udpClusterReplicator = clusterReplicator;
        return clusterReplicator;

    }

}
