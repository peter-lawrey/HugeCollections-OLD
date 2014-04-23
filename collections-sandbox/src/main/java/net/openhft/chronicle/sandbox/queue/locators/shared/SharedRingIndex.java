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

package net.openhft.chronicle.sandbox.queue.locators.shared;

import net.openhft.chronicle.sandbox.queue.locators.RingIndex;
import net.openhft.lang.io.DirectBytes;

import java.io.IOException;

/**
 * Created by Rob Austin
 */
public class SharedRingIndex implements RingIndex {

    private static final int READ_OFFSET = 0;
    private static final int WRITE_OFFSET = READ_OFFSET + 4;

    private final DirectBytes indexLocationBytes;
    private int producerWriteLocation;

    public SharedRingIndex(DirectBytes indexLocationBytes) throws IOException {
        this.indexLocationBytes = indexLocationBytes;
    }

    @Override
    public int getWriterLocation() {
        return indexLocationBytes.readVolatileInt(WRITE_OFFSET);
    }

    @Override
    public void setWriterLocation(int nextWriteLocation) {
        indexLocationBytes.writeOrderedInt(WRITE_OFFSET, nextWriteLocation);
    }

    @Override
    public int getReadLocation() {
        return indexLocationBytes.readVolatileInt(READ_OFFSET);
    }

    @Override
    public void setReadLocation(int nextReadLocation) {
        indexLocationBytes.writeOrderedInt(READ_OFFSET, nextReadLocation);
    }

    @Override
    public int getProducerWriteLocation() {
        return this.producerWriteLocation;
    }

    @Override
    public void setProducerWriteLocation(int nextWriteLocation) {
        this.producerWriteLocation = nextWriteLocation;
    }

    @Override
    public String toString() {
        return "readLocation=" + getReadLocation() + ", writerLocation=" + getWriterLocation();
    }
}
