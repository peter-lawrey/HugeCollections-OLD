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

import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.Math.min;
import static java.nio.ByteBuffer.wrap;

/**
 * @author Rob Austin.
 */
public class ClusterReplicator<K, V> implements ReplicaExternalizable<K, V>, Closeable {

    private final SingleThreadedDirectBitSet bitSet;
    private byte identifier;


    final ReplicaExternalizable replicaExternalizables[];

    private final AtomicReferenceArray<ModificationIterator> modificationIterator = new
            AtomicReferenceArray<ModificationIterator>(130);

    Set<AbstractChannelReplicator> replicators = new CopyOnWriteArraySet<AbstractChannelReplicator>();

    public ClusterReplicator(final byte identifier, int maxNumberOfChronicles) {
        this.identifier = identifier;
        final int size = (int) Math.ceil(maxNumberOfChronicles / 8.0);
        final ByteBufferBytes bytes = new ByteBufferBytes(wrap(new byte[size]));
        bitSet = new SingleThreadedDirectBitSet(bytes);
        replicaExternalizables = new ReplicaExternalizable[maxNumberOfChronicles];

    }


    public synchronized void add(short chronicleId, ReplicaExternalizable replica) {
        replicaExternalizables[chronicleId] = replica;
        forceBootstrap();
    }


    /**
     * forcing the bootstrap ensures that {@code net.openhft.collections.Replica.ModificationIterator#dirtyEntries(long)}
     * is called and this taking into account all recently added replicas
     */
    private void forceBootstrap() {
        for (AbstractChannelReplicator replicator : replicators) {
            replicator.forceBootstrap();
        }
    }


    @Override
    public void writeExternalEntry(@NotNull NativeBytes entry, @NotNull Bytes destination, int chronicleId) {
        destination.writeStopBit(chronicleId);
        replicaExternalizables[chronicleId].writeExternalEntry(entry, destination, chronicleId);
    }

    @Override
    public void readExternalEntry(@NotNull Bytes source) {
        final int chronicleId = (int) source.readStopBit();
        replicaExternalizables[chronicleId].readExternalEntry(source);
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
                for (int i = (int) bitSet.nextSetBit(0); i > 0; i = (int) bitSet.nextSetBit(i + 1)) {
                    if (replicaExternalizables[i].acquireModificationIterator(id, notifier).hasNext())
                        return true;
                }
                return false;
            }

            @Override
            public boolean nextEntry(@NotNull AbstractEntryCallback callback, final int na) {
                for (int i = (int) bitSet.nextSetBit(0); i > 0; i = (int) bitSet.nextSetBit(i + 1)) {
                    if (replicaExternalizables[i].acquireModificationIterator(id,
                            notifier).nextEntry(callback, i))
                        return true;
                }
                return false;
            }

            @Override
            public void dirtyEntries(long fromTimeStamp) {
                for (int i = (int) bitSet.nextSetBit(0); i > 0; i = (int) bitSet.nextSetBit(i + 1)) {
                    replicaExternalizables[i].acquireModificationIterator(id, notifier).dirtyEntries(fromTimeStamp);
                }
            }
        };

        modificationIterator.set((int) id, result0);
        return result0;

    }

    /**
     * gets the earliest modification time for all of the chronicles
     *
     * @param remoteIdentifier the remote identifer
     * @return
     */
    @Override
    public long lastModificationTime(byte remoteIdentifier) {
        long t = System.currentTimeMillis();
        for (int i = (int) bitSet.nextSetBit(0); i > 0; i = (int) bitSet.nextSetBit(i + 1)) {
            t = min(t, replicaExternalizables[i].lastModificationTime(remoteIdentifier));
        }
        return t;
    }

    @Override
    public void close() throws IOException {
        for (int i = (int) bitSet.nextSetBit(0); i > 0; i = (int) bitSet.nextSetBit(i + 1)) {
            replicaExternalizables[i].close();
        }
    }

    public void add(AbstractChannelReplicator replicator) {
        replicators.add(replicator);
    }
}
