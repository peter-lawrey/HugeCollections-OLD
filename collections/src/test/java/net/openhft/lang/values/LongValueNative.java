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

package net.openhft.lang.values;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.Copyable;

import static net.openhft.lang.Compare.calcLongHashCode;
import static net.openhft.lang.Compare.isEqual;

public class LongValueNative implements LongValue, BytesMarshallable, Byteable, Copyable<net.openhft.lang.values.LongValue> {
    private static final int VALUE = 0;


    private Bytes _bytes;
    private long _offset;

    public void setValue(long _) {
        _bytes.writeLong(_offset + VALUE, _);
    }

    public long getValue() {
        return _bytes.readLong(_offset + VALUE);
    }

    public long addValue(long _) {
        return _bytes.addLong(_offset + VALUE, _);
    }

    public long addAtomicValue(long _) {
        return _bytes.addAtomicLong(_offset + VALUE, _);
    }

    public boolean compareAndSwapValue(long _1, long _2) {
        return _bytes.compareAndSwapLong(_offset + VALUE, _1, _2);
    }

    public void copyFrom(net.openhft.lang.values.LongValue from) {
        setValue(from.getValue());
    }

    public void writeMarshallable(Bytes out) {
        out.writeLong(getValue());
    }

    public void readMarshallable(Bytes in) {
        setValue(in.readLong());
    }

    public void bytes(Bytes bytes) {
        bytes(bytes, 0L);
    }

    public void bytes(Bytes bytes, long offset) {
        this._bytes = bytes;
        this._offset = offset;
    }

    public Bytes bytes() {
        return _bytes;
    }

    public long offset() {
        return _offset;
    }

    public int maxSize() {
        return 8;
    }

    public int hashCode() {
        long lhc = longHashCode();
        return (int) ((lhc >>> 32) ^ lhc);
    }

    public long longHashCode() {
        return calcLongHashCode(getValue());
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LongValue)) return false;
        LongValue that = (LongValue) o;

        if (!isEqual(getValue(), that.getValue())) return false;
        return true;
    }

    public String toString() {
        return "LongValue {" +
                ", value= " + getValue() + " }";
    }
}