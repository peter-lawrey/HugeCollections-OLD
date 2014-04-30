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

import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.constraints.Nullable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.EnumSet;

import static java.util.Arrays.asList;
import static java.util.EnumSet.copyOf;
import static net.openhft.collections.SegmentModificationIterator.State.PUT;
import static net.openhft.collections.SegmentModificationIterator.State.REMOVE;

/**
 * Once a change occurs to a map, map replication requires that these changes are picked up by another thread,
 * this class provides an iterator like interface to poll for such changes.
 * In most cases the thread that adds data to the node is unlikely to be the same thread that replicates the data over to the other nodes,
 * so data will have to be marshaled between the main thread storing data to the map, and the thread running the replication.
 * One way to perform this marshalling, would be to pipe the data into a queue. However, This class takes another approach.
 * It uses a bit set, and marks bits which correspond to the indexes of the entries that have changed.
 * It then provides an iterator like interface to poll for such changes.
 *
 * @author Rob Austin.
 */
public class SegmentModificationIterator<K, V> implements SharedMapEventListener<K, V, ReplicatedSharedHashMap<K, V>> {


    private final Object notifier;

    public enum State {PUT, REMOVE}

    private SegmentInfoProvider segmentInfoProvider;
    private SingleThreadedDirectBitSet changes;

    private final EnumSet<State> watchList;

    private final int identifier;


    public SegmentModificationIterator() {
        this(null, 1);
    }


    /**
     * @param notifier  if not NULL, notifyAll() is called on this object when ever an item is added to the watchlist
     * @param watchList if you don't provide a {@code watchList} all the items are deemed to be in the watchlist
     */
    public SegmentModificationIterator(@Nullable final Object notifier, final State... watchList) {
        this(null, 1, watchList);
    }

    /**
     * @param notifier   if not NULL, notifyAll() is called on this object when ever an item is added to the watchlist
     * @param identifier
     * @param watchList  if you don't provide a {@code watchList} all the items are deemed to be in the watchlist
     */
    public SegmentModificationIterator(@Nullable final Object notifier, int identifier, final State... watchList) {
        this.notifier = notifier;
        this.identifier = identifier;
        this.watchList = (watchList.length == 0) ? EnumSet.allOf(State.class) : copyOf(asList(watchList));
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V onGetMissing(ReplicatedSharedHashMap<K, V> map, Bytes keyBytes, K key, V usingValue) {
        // do nothing
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onGetFound(ReplicatedSharedHashMap<K, V> map, Bytes entry, int metaDataBytes, K key, V value) {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onPut(ReplicatedSharedHashMap<K, V> map, Bytes entry, int metaDataBytes, boolean added, K key, V value, long pos, SharedSegment segment) {

        if (this.identifier == map.getIdentifier() && !watchList.contains(PUT))
            return;
        final long bitIndex = (segment.getIndex() * segmentInfoProvider.getEntriesPerSegment()) + pos;
        changes.set(bitIndex);

        if (notifier != null)
            synchronized (notifier) {
                notifier.notifyAll();
            }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onRemove(ReplicatedSharedHashMap<K, V> map, Bytes entry, int metaDataBytes, K key, V value, int pos, SharedSegment segment) {

        if (this.identifier == map.getIdentifier() && !watchList.contains(REMOVE))
            return;
        changes.set((segment.getIndex() * segmentInfoProvider.getEntriesPerSegment()) + pos);

        if (notifier != null)
            synchronized (notifier) {
                notifier.notifyAll();
            }

    }

    private long position = DirectBitSet.NOT_FOUND;

    /**
     * you can continue to poll hasNext() until data becomes available.
     *
     * @return true if there is an entry
     */
    public boolean hasNext() {

        long oldOffset = position;

        final long offset0 = changes.nextSetBit(oldOffset + 1);

        if (offset0 == DirectBitSet.NOT_FOUND) {
            if (oldOffset == DirectBitSet.NOT_FOUND)
                return false;
            position = -1;
            return hasNext();
        }
        return true;
    }

    interface EntryProcessor {
        void onEntry(final NativeBytes entry);
    }


    /**
     * @param entryProcessor
     * @return true if an entry was processed
     */
    public boolean nextEntry(@NotNull final EntryProcessor entryProcessor) {

        long oldOffset = position;
        position = changes.nextSetBit(position + 1);

        if (position != DirectBitSet.NOT_FOUND)
            changes.clear(position);

        if (position == DirectBitSet.NOT_FOUND)
            return oldOffset != DirectBitSet.NOT_FOUND && nextEntry(entryProcessor);

        final int segmentIndex = (int) (position / segmentInfoProvider.getEntriesPerSegment());
        final SharedSegment segment = segmentInfoProvider.getSegments()[segmentIndex];

        final int segmentPos = (int) position - (segmentInfoProvider.getEntriesPerSegment() * segmentIndex);

        segment.lock();
        try {

            final NativeBytes entry = segment.entry(segment.offsetFromPos(segmentPos));
            entryProcessor.onEntry(entry);
            return true;
        } finally {
            segment.unlock();
        }
    }


    /**
     * this must be called just after construction
     *
     * @param segmentInfoProvider information about the Segment
     */
    public void setSegmentInfoProvider(@NotNull final SegmentInfoProvider segmentInfoProvider) {
        this.segmentInfoProvider = segmentInfoProvider;
        changes = new SingleThreadedDirectBitSet(new ByteBufferBytes(
                ByteBuffer.allocate(1 + (int) ((segmentInfoProvider.getEntriesPerSegment() * segmentInfoProvider.getSegments().length) / 8.0))));
    }
}
