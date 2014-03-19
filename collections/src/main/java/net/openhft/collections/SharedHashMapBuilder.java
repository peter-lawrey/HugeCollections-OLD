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

import net.openhft.lang.Maths;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class SharedHashMapBuilder implements Cloneable {

    static final int HEADER_SIZE = 128;
    static final int SEGMENT_HEADER = 64;
    private static final byte[] MAGIC = "SharedHM".getBytes();

    // used when configuring the number of segments.
    private int minSegments = 128;
    private int actualSegments = -1;
    // used when reading the number of entries per
    private int actualEntriesPerSegment = -1;

    private int entrySize = 256;
    private long entries = 1 << 20;
    private int replicas = 0;
    private boolean transactional = false;
    private long lockTimeOutMS = 1000;
    private int metaDataBytes = 0;
    private SharedMapEventListener eventListener = SharedMapEventListeners.NOP;
    private SharedMapErrorListener errorListener = SharedMapErrorListeners.LOGGING;
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;
    private boolean generatedKeyType = false;
    private boolean generatedValueType = false;
    private boolean largeSegments = false;

    @Override
    public SharedHashMapBuilder clone() {
        try {
            return (SharedHashMapBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Set minimum number of segments.
     * <p></p>
     * See concurrencyLevel in {@link java.util.concurrent.ConcurrentHashMap}.
     *
     * @return this builder object back
     */
    public SharedHashMapBuilder minSegments(int minSegments) {
        this.minSegments = minSegments;
        return this;
    }

    public int minSegments() {
        return minSegments;
    }


    public SharedHashMapBuilder entrySize(int entrySize) {
        this.entrySize = entrySize;
        return this;
    }

    public int entrySize() {
        return entrySize;
    }

    public SharedHashMapBuilder entries(long entries) {
        this.entries = entries;
        return this;
    }

    public long entries() {
        return entries;
    }

    public SharedHashMapBuilder replicas(int replicas) {
        this.replicas = replicas;
        return this;
    }

    public int replicas() {
        return replicas;
    }

    public SharedHashMapBuilder actualEntriesPerSegment(int actualEntriesPerSegment) {
        this.actualEntriesPerSegment = actualEntriesPerSegment;
        return this;
    }

    public int actualEntriesPerSegment() {
        if (actualEntriesPerSegment > 0)
            return actualEntriesPerSegment;
        // round up to the next multiple of 64.
        int as = actualSegments();
        int aeps = segmentsForEntries(as);
        return aeps;
    }

    private int segmentsForEntries(int as) {
        return (int) (Math.max(1, entries * 2L / as) + 63) & ~63;
    }

    public SharedHashMapBuilder actualSegments(int actualSegments) {
        this.actualSegments = actualSegments;
        return this;
    }

    public int actualSegments() {
        if (actualSegments > 0)
            return actualSegments;
        if (!largeSegments && entries > (long) minSegments << 15) {
            long segments = Maths.nextPower2(entries >> 15, 128);
            if (segments < 1 << 20)
                return (int) segments;
        }
        // try to keep it 16-bit sizes segments
        return (int) Maths.nextPower2(Math.max((entries >> 30) + 1, minSegments), 1);
    }

    /**
     * Not supported yet.
     */
    public SharedHashMapBuilder transactional(boolean transactional) {
        this.transactional = transactional;
        return this;
    }

    public boolean transactional() {
        return transactional;
    }

    public <K, V> SharedHashMap<K, V> create(File file, Class<K> kClass, Class<V> vClass) throws IOException {
        SharedHashMapBuilder builder = clone();

        for (int i = 0; i < 10; i++) {
            if (file.exists() && file.length() > 0) {
                readFile(file, builder);
                break;
            }
            if (file.createNewFile() || file.length() == 0) {
                newFile(file);
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        if (builder == null || !file.exists())
            throw new FileNotFoundException("Unable to create " + file);
        return new VanillaSharedHashMap<K, V>(builder, file, kClass, vClass);
    }

    private static void readFile(File file, SharedHashMapBuilder builder) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_SIZE).order(ByteOrder.nativeOrder());
        FileInputStream fis = new FileInputStream(file);
        fis.getChannel().read(bb);
        fis.close();
        bb.flip();
        if (bb.remaining() < 22) throw new IOException("File too small, corrupted? " + file);
        byte[] bytes = new byte[8];
        bb.get(bytes);
        if (!Arrays.equals(bytes, MAGIC))
            throw new IOException("Unknown magic number, was " + new String(bytes, "ISO-8859-1"));
        builder.actualSegments(bb.getInt());
        builder.actualEntriesPerSegment(bb.getInt());
        builder.entrySize(bb.getInt());
        builder.replicas(bb.getInt());
        builder.transactional(bb.get() == 'Y');
        builder.metaDataBytes(bb.get() & 0xFF);
        if (builder.actualSegments() <= 0 || builder.actualEntriesPerSegment() <= 0 || builder.entrySize() <= 0)
            throw new IOException("Corrupt header for " + file);
    }

    private void newFile(File file) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_SIZE).order(ByteOrder.nativeOrder());
        bb.put(MAGIC);
        bb.putInt(actualSegments());
        bb.putInt(actualEntriesPerSegment());
        bb.putInt(entrySize());
        bb.putInt(replicas());
        bb.put((byte) (transactional ? 'Y' : 'N'));
        bb.put((byte) metaDataBytes);
        bb.flip();
        FileOutputStream fos = new FileOutputStream(file);
        fos.getChannel().write(bb);
        fos.close();
    }

    public SharedHashMapBuilder lockTimeOutMS(long lockTimeOutMS) {
        this.lockTimeOutMS = lockTimeOutMS;
        return this;
    }

    public long lockTimeOutMS() {
        return lockTimeOutMS;
    }

    public SharedHashMapBuilder errorListener(SharedMapErrorListener errorListener) {
        this.errorListener = errorListener;
        return this;
    }

    public SharedMapErrorListener errorListener() {
        return errorListener;
    }

    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param putReturnsNull false if you want SharedHashMap.put() to not return the object that was replaced but instead return null
     */
    public SharedHashMapBuilder putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @return true if SharedHashMap.put() is not going to return the object that was replaced but instead return null
     */
    public boolean putReturnsNull() {
        return putReturnsNull;
    }

    /**
     * Map.remove()  returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param removeReturnsNull false if you want SharedHashMap.remove() to not return the object that was removed but instead return null
     */
    public SharedHashMapBuilder removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }


    /**
     * Map.remove() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @return true if SharedHashMap.remove() is not going to return the object that was removed but instead return null
     */
    public boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    public boolean generatedKeyType() {
        return generatedKeyType;
    }

    public SharedHashMapBuilder generatedKeyType(boolean generatedKeyType) {
        this.generatedKeyType = generatedKeyType;
        return this;
    }

    public boolean generatedValueType() {
        return generatedValueType;
    }

    public SharedHashMapBuilder generatedValueType(boolean generatedValueType) {
        this.generatedValueType = generatedValueType;
        return this;
    }

    public boolean largeSegments() {
        return entries > 1L << (20 + 15) || largeSegments;
    }

    public SharedHashMapBuilder largeSegments(boolean largeSegments) {
        this.largeSegments = largeSegments;
        return this;
    }


    public SharedHashMapBuilder metaDataBytes(int metaDataBytes) {
        if ((metaDataBytes & 0xFF) != metaDataBytes)
            throw new IllegalArgumentException("MetaDataBytes must be [0..255] was " + metaDataBytes);
        this.metaDataBytes = metaDataBytes;
        return this;
    }

    public int metaDataBytes() {
        return metaDataBytes;
    }

    public SharedHashMapBuilder eventListener(SharedMapEventListener eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    public SharedMapEventListener eventListener() {
        return eventListener;
    }

    @Override
    public String toString() {
        return "SharedHashMapBuilder{" +
                "actualSegments=" + actualSegments() +
                (actualSegments > 0 ? ", actualSegments=" + actualSegments() : ", minSegments=" + minSegments()) +
                ", actualEntriesPerSegment=" + actualEntriesPerSegment() +
                ", entrySize=" + entrySize() +
                ", entries=" + entries() +
                ", replicas=" + replicas() +
                ", transactional=" + transactional() +
                ", lockTimeOutMS=" + lockTimeOutMS() +
                ", errorListener=" + errorListener() +
                ", putReturnsNull=" + putReturnsNull() +
                ", removeReturnsNull=" + removeReturnsNull() +
                ", generatedKeyType=" + generatedKeyType() +
                ", generatedValueType=" + generatedValueType() +
                ", largeSegments=" + largeSegments() +
                ", metaDataBytes=" + metaDataBytes() +
                ", eventListener=" + eventListener() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SharedHashMapBuilder that = (SharedHashMapBuilder) o;

        if (actualEntriesPerSegment() != that.actualEntriesPerSegment()) return false;
        if (actualSegments() != that.actualSegments()) return false;
        if (entries() != that.entries()) return false;
        if (entrySize() != that.entrySize()) return false;
        if (generatedKeyType() != that.generatedKeyType()) return false;
        if (generatedValueType() != that.generatedValueType()) return false;
        if (lockTimeOutMS() != that.lockTimeOutMS()) return false;
        if (minSegments() != that.minSegments()) return false;
        if (putReturnsNull() != that.putReturnsNull()) return false;
        if (removeReturnsNull() != that.removeReturnsNull()) return false;
        if (replicas() != that.replicas()) return false;
        if (transactional() != that.transactional()) return false;
        if (metaDataBytes() != that.metaDataBytes()) return false;
        return errorListener().equals(that.errorListener());

    }
}
