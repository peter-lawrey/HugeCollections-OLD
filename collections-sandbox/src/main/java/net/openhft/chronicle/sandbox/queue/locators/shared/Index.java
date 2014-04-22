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

/**
 * can either be a writer index or a reader index
 */
public interface Index {

    /**
     * if this is being used for a producer then it will setReadLocation, if its being used by a producer it will setWriteLocation
     *
     * @param index the index to be set
     */
    void setNextLocation(int index);

    /**
     * get the index where the data is stored in the byte buffer
     */
    int getWriterLocation();
}
