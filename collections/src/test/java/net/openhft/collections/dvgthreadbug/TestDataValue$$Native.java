/*
 * Copyright 2014 Higher Frequency Trading
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


package net.openhft.collections.dvgthreadbug;

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.Copyable;

import static net.openhft.lang.Compare.calcLongHashCode;
import static net.openhft.lang.Compare.isEqual;

// GENERATED using -Ddvg.dumpcode=true
public class TestDataValue$$Native implements TestDataValue, BytesMarshallable, Byteable, Copyable<TestDataValue> {
    private static final int LONGSTRING = 0;


    private Bytes _bytes;
    private long _offset;


    public void setLongString(java.lang.String $) {
        _bytes.writeUTFΔ(_offset + LONGSTRING, 240, $);
    }

    public java.lang.String getLongString() {
        return _bytes.readUTFΔ(_offset + LONGSTRING);
    }

    @Override
    public void copyFrom(TestDataValue from) {
        setLongString(from.getLongString());
    }

    @Override
    public void writeMarshallable(Bytes out) {
        out.writeUTFΔ(getLongString());
    }

    @Override
    public void readMarshallable(Bytes in) {
        setLongString(in.readUTFΔ());
    }

    @Override
    public void bytes(Bytes bytes, long offset) {
        this._bytes = bytes;
        this._offset = offset;
    }

    @Override
    public Bytes bytes() {
        return _bytes;
    }

    @Override
    public long offset() {
        return _offset;
    }

    @Override
    public int maxSize() {
        return 240;
    }

    public int hashCode() {
        long lhc = longHashCode();
        return (int) ((lhc >>> 32) ^ lhc);
    }

    public long longHashCode() {
        return calcLongHashCode(getLongString());
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TestDataValue)) return false;
        TestDataValue that = (TestDataValue) o;

        if (!isEqual(getLongString(), that.getLongString())) return false;
        return true;
    }

    public String toString() {
        if (_bytes == null) return "bytes is null";
        StringBuilder sb = new StringBuilder();
        sb.append("TestDataValue{ ");
        sb.append("longString= ").append(getLongString());
        sb.append(" }");
        return sb.toString();
    }
}
