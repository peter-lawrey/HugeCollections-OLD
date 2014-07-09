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

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.constraints.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.Math.min;

/**
 * @author Rob Austin.
 */
public class ClusterReplicator<K, V>
        implements ReplicaExternalizable<K, V>, Closeable {

    private byte identifier;


    private List<ReplicaExternalizable<K, V>> replicas = new CopyOnWriteArrayList<ReplicaExternalizable<K, V>>();

    private final AtomicReferenceArray<ModificationIterator> modificationIterator = new
            AtomicReferenceArray<ModificationIterator>(130);

    Set<AbstractChannelReplicator> replicators = new CopyOnWriteArraySet<AbstractChannelReplicator>();

    public ClusterReplicator(final byte identifier) {
        this.identifier = identifier;
    }

    public void add(ReplicaExternalizable<K, V> replica) {
        replicas.add(replica);
        bootstrap();
    }

    private void bootstrap() {
        for (AbstractChannelReplicator replicator : replicators) {
            replicator.forceBootstrap();
        }

    }

    public void addAll(Collection<ReplicaExternalizable<K, V>> replicas) {
        this.replicas.addAll(replicas);
        bootstrap();
    }

    @Override
    public void writeExternalEntry(@NotNull NativeBytes entry, @NotNull Bytes destination, int chronicleId) {
        destination.writeStopBit(chronicleId);
        replicas.get(chronicleId).writeExternalEntry(entry, destination, chronicleId);
    }

    @Override
    public void readExternalEntry(@NotNull Bytes source) {
        final int chronicleId = (int) source.readStopBit();
        replicas.get(chronicleId).readExternalEntry(source);
    }

    @Override
    public byte identifier() {
        return this.identifier;
    }

    @Override
    public ModificationIterator acquireModificationIterator(final short id,
                                                            final ModificationNotifier notifier) {
        final ModificationIterator result = modificationIterator.get(id);
        if (result != null)
            return result;

        final ModificationIterator result0 = new ModificationIterator() {

            @Override
            public boolean hasNext() {
                for (int i = 0; i < replicas.size(); i++) {
                    if (replicas.get(i).acquireModificationIterator(id, notifier).hasNext())
                        return true;
                }
                return false;
            }

            @Override
            public boolean nextEntry(@NotNull AbstractEntryCallback callback, final int na) {
                for (int chronicleId = 0; chronicleId < replicas.size(); chronicleId++) {
                    if (replicas.get(chronicleId).acquireModificationIterator(id, notifier).nextEntry(callback, chronicleId))
                        return true;
                }
                return false;
            }

            @Override
            public void dirtyEntries(long fromTimeStamp) {
                for (int i = 0; i < replicas.size(); i++) {
                    replicas.get(i).acquireModificationIterator(id, notifier).dirtyEntries(fromTimeStamp);
                }
            }
        };

        modificationIterator.set((int) id, result0);
        return result0;

    }

    /**
     * gets the earliest modification time for all of the chronicles
     *
     * @param identifier
     * @return
     */
    @Override
    public long lastModificationTime(byte identifier) {
        long t = System.currentTimeMillis();
        for (int i = 0; i < replicas.size(); i++) {
            t = min(t, replicas.get(i).lastModificationTime(identifier));
        }
        return t;
    }

    @Override
    public void close() throws IOException {
        for (ReplicaExternalizable<K, V> closeable : replicas) {
            closeable.close();
        }
    }

    public void add(AbstractChannelReplicator replicator) {
        replicators.add(replicator);
    }
}
