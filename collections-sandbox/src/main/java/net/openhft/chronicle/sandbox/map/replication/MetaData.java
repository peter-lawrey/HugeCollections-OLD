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

package net.openhft.chronicle.sandbox.map.replication;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import org.jetbrains.annotations.NotNull;


/**
 * @author Rob Austin.
 */
public class MetaData<V extends Object> implements BytesMarshallable {

    private boolean generatedValueType;
    private long timestamp;
    private byte sequenceId;
    private byte action;
    private V value;
    private Class<V> clazz;

    private Bytes in;
    boolean hasLazyValue;

    private long valuePosition;


    /**
     * clears all the fields, for reuse
     */
    public void clear() {
        timestamp = 0;
        sequenceId = 0;
        action = 0;
        value = null;
        clazz = null;
        in = null;
        hasLazyValue = false;
        valuePosition = 0;
    }


    public static <V> MetaData create(long timestamp, byte sequenceId, byte action, V value, Class<V> clazz, boolean generatedValueType) {


        final MetaData metaData = new MetaData();

        metaData.generatedValueType = generatedValueType;
        metaData.timestamp = timestamp;
        metaData.sequenceId = sequenceId;
        metaData.action = action;
        metaData.value = value;
        metaData.hasLazyValue = true;
        metaData.clazz = clazz;
        return metaData;
    }


    /**
     * used for remove
     *
     * @param sequenceId
     * @param action
     */
    public static MetaData create(long timestamp, byte sequenceId, byte action) {
        final MetaData metaData = new MetaData();
        metaData.timestamp = timestamp;
        metaData.sequenceId = sequenceId;
        metaData.action = action;
        metaData.hasLazyValue = false;
        return metaData;

    }

    public byte getSequenceId() {
        return sequenceId;
    }

    /**
     * returns true if this is new than the values provided {@code timestamp} and {@code sequenceId}
     *
     * @param timestamp
     * @param sequenceId
     * @return
     */

    public boolean isNewer(long timestamp, byte sequenceId) {


        // if the called to put has an earlier timestamp then the update was ignored
        if (this.timestamp < timestamp)
            return true;

        // If two nodes update the map in exactly the same millisecond,
        // then we have to deterministically resolve which update wins,
        // we do this by using the sequence id,
        // the update with the smallest sequence id wins.
        return (this.timestamp == timestamp && sequenceId < this.sequenceId);

    }


    public boolean isNewer(MetaData metaData) {

        if (metaData == null)
            return false;

        // if the called to put has an earlier timestamp then the update was ignored
        final long timestamp = metaData.getTimestamp();

        if (this.timestamp < timestamp)
            return true;

        // If two nodes update the map in exactly the same millisecond,
        // then we have to deterministically resolve which update wins,
        // we do this by using the sequence id,
        // the update with the smallest sequence id wins.
        return (this.timestamp == timestamp && metaData.getSequenceId() < this.sequenceId);

    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte getAction() {
        return action;
    }


    /**
     * the object the value will be written to
     *
     * @param vClass
     * @return
     */
    public V get(Class<V> vClass) {


        return getUsing(vClass, null);
    }


    /**
     * the object the value will be written to
     * Will lazily load the value from the {@code in} bytes
     *
     * @param vClass
     * @param using
     * @return
     */
    public V getUsing(Class<V> vClass, final V using) {

        if (this.hasLazyValue)
            return value;

        if (in == null)
            return null;

        long oldPos = in.position();
        in.position(this.valuePosition);

        if (generatedValueType)
            if (value == null)
                value = DataValueClasses.newDirectReference(vClass);
            else
                assert value instanceof Byteable;
        if (value instanceof Byteable) {
            ((Byteable) value).bytes(in, this.valuePosition);
            return value;
        }

        this.value = in.readInstance(vClass, value);
        in.position(oldPos);
        hasLazyValue = true;

        return value;
    }

    @Override
    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {

        this.in = in;

        timestamp = in.readLong();
        sequenceId = in.readByte();
        action = in.readByte();


        if (getAction() != 'P')
            return;


        this.valuePosition = in.position();
        this.in = in;

    }

    @Override
    public void writeMarshallable(@NotNull Bytes out) {
        out.writeLong(timestamp);
        out.writeByte(sequenceId);
        out.writeByte(action);

        if (action == 'P')
            out.writeInstance(clazz, value);

    }


    @Override
    public String toString() {
        return "MetaData{" +
                "timestamp=" + timestamp +
                ", sequenceId=" + sequenceId +
                ", action=" + action +
                ", value=" + value +
                ", clazz=" + clazz +
                '}';
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetaData metaData = (MetaData) o;

        if (action != metaData.action) return false;
        if (sequenceId != metaData.sequenceId) return false;
        if (timestamp != metaData.timestamp) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (int) sequenceId;
        result = 31 * result + (int) action;
        return result;
    }
}

