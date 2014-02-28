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
import net.openhft.lang.io.MappedStore;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class SharedHashMapBuilder implements Cloneable {
    static final int HEADER_SIZE = 128;
    static final int SEGMENT_HEADER = 64;
    private static final byte[] MAGIC = "SharedHM".getBytes();
    private static final double INCREASE_ENTRIES_PER_SECTOR = 1.5;
    private int segments = 128;
    private int entrySize = 256;
    private long entries = 1 << 20;
    private int replicas = 0;
    private boolean transactional = false;
    private long lockTimeOutMS = 1000;
    private SharedMapErrorListener errorListener = SharedMapErrorListeners.LOGGING;
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;
    private boolean generatedKeyType = false;
    private boolean generatedValueType = false;


    public SharedHashMapBuilder segments(int segments) {
        this.segments = segments;
        return this;
    }

    @Override
    public SharedHashMapBuilder clone() {
        try {
            return (SharedHashMapBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    public int segments() {
        return segments;
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

    public SharedHashMapBuilder transactional(boolean transactional) {
        this.transactional = transactional;
        return this;
    }

    public boolean transactional() {
        return transactional;
    }

    public <K, V> SharedHashMap<K, V> create(File file, Class<K> kClass, Class<V> vClass) throws IOException {
        SharedHashMapBuilder builder = null;
        for (int i = 0; i < 10; i++) {
            if (file.exists() && file.length() > 0) {
                builder = readFile(file);
                break;
            }
            if (file.createNewFile() || file.length() == 0) {
                newFile(file);
                builder = clone();
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
        MappedStore ms = new MappedStore(file, FileChannel.MapMode.READ_WRITE, size());
        return new VanillaSharedHashMap<K, V>(builder, ms, kClass, vClass);
    }

    private SharedHashMapBuilder readFile(File file) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_SIZE).order(ByteOrder.nativeOrder());
        FileInputStream fis = new FileInputStream(file);
        fis.getChannel().read(bb);
        fis.close();
        bb.flip();
        if (bb.remaining() <= 20) throw new IOException("File too small, corrupted? " + file);
        byte[] bytes = new byte[8];
        bb.get(bytes);
        if (!Arrays.equals(bytes, MAGIC)) throw new IOException("Unknown magic number, was " + new String(bytes, 0));
        SharedHashMapBuilder builder = new SharedHashMapBuilder();
        builder.segments(bb.getInt());
        builder.entries((long)(bb.getLong() * builder.segments() / INCREASE_ENTRIES_PER_SECTOR));
        builder.entrySize(bb.getInt());
        builder.replicas(bb.getInt());
        builder.transactional(bb.get() == 'Y');
        if (segments() <= 0 || entries() <= 0 || entrySize() <= 0)
            throw new IOException("Corrupt header for " + file);
        return builder;
    }

    private void newFile(File file) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_SIZE).order(ByteOrder.nativeOrder());
        bb.put(MAGIC);
        bb.putInt(segments);
        bb.putLong(entriesPerSegment());
        bb.putInt(entrySize);
        bb.putInt(replicas);
        bb.put((byte) (transactional ? 'Y' : 'N'));
        bb.flip();
        FileOutputStream fos = new FileOutputStream(file);
        fos.getChannel().write(bb);
        fos.close();
    }

    public long entriesPerSegment() {
        long epg1 = (long)((entries * INCREASE_ENTRIES_PER_SECTOR) / segments);
        return (Math.max(1, epg1) + 63) & ~63; // must be a multiple of 64 for the bit set to work;
    }

    long size() {
        return HEADER_SIZE + segments * segmentSize();
    }

    long segmentSize() {
        return (SEGMENT_HEADER
                + Maths.nextPower2(entriesPerSegment() * 12, 16 * 8) // the IntIntMultiMap
                + (1 + replicas) * bitSetSize() // the free list and 0+ dirty lists.
                + entriesPerSegment() * entrySize); // the actual entries used.
    }

    int bitSetSize() {
        return (int) ((entriesPerSegment() + 63) / 64 * 8);
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

    public SharedHashMapBuilder putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    public boolean putReturnsNull() {
        return putReturnsNull;
    }

    public SharedHashMapBuilder removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }

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
}
