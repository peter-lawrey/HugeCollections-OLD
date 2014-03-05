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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class SharedHashMapBuilder implements Cloneable {

    static final int HEADER_SIZE = 128;
    static final int SEGMENT_HEADER = 64;
    private static final byte[] MAGIC = "SharedHM".getBytes();

    private int minSegments = 128;
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
     *
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
        return new VanillaSharedHashMap<K, V>(builder, file, kClass, vClass);
    }

    private static SharedHashMapBuilder readFile(File file) throws IOException {
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
        builder.minSegments(bb.getInt());
        builder.entries(bb.getLong());
        builder.entrySize(bb.getInt());
        builder.replicas(bb.getInt());
        builder.transactional(bb.get() == 'Y');
        if (builder.minSegments() <= 0 || builder.entries() <= 0 || builder.entrySize() <= 0)
            throw new IOException("Corrupt header for " + file);
        return builder;
    }

    private void newFile(File file) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_SIZE).order(ByteOrder.nativeOrder());
        bb.put(MAGIC);
        bb.putInt(minSegments);
        bb.putLong(entries);
        bb.putInt(entrySize);
        bb.putInt(replicas);
        bb.put((byte) (transactional ? 'Y' : 'N'));
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
     * {@link this.put()} returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param putReturnsNull false if you want {@link this.put()} to not return the object that was replaced but instead return null
     */
    public SharedHashMapBuilder putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    /**
     * {@link this.put()} returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @return true if {@link this.put()} is not going to return the object that was replaced but instead return null
     */
    public boolean putReturnsNull() {
        return putReturnsNull;
    }

    /**
     * {@link this.remove()}  returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param removeReturnsNull false if you want {@link this.remove()} to not return the object that was removed but instead return null
     * @return
     */
    public SharedHashMapBuilder removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }


    /**
     * {@link this.put()} returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @return true if {@link this.remove()} is not going to return the object that was removed but instead return null
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
}
