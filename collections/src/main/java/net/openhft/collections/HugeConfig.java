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

/**
 * User: plawrey
 * Date: 07/12/13
 * Time: 10:39
 */
public class HugeConfig implements Cloneable {
    // reserve 4 MB
    public static final HugeConfig SMALL = new HugeConfig()
            .setSmallEntrySize(256)
            .setEntriesPerSegment(256)
            .setSegments(16);
    // reserve 32 MB
    public static final HugeConfig DEFAULT = new HugeConfig()
            .setSmallEntrySize(512)
            .setEntriesPerSegment(1024)
            .setSegments(64);
    // reserve 256 MB
    public static final HugeConfig BIG = new HugeConfig()
            .setSmallEntrySize(1024)
            .setEntriesPerSegment(4 * 1024)
            .setSegments(64);
    // reserve 2 GB
    public static final HugeConfig LARGE = new HugeConfig()
            .setSmallEntrySize(2 * 1024)
            .setEntriesPerSegment(8 * 1024)
            .setSegments(128);
    // reserve 16 GB
    public static final HugeConfig HUGE = new HugeConfig()
            .setSmallEntrySize(4 * 1024)
            .setEntriesPerSegment(16 * 1024)
            .setSegments(256);

    private int segments;
    private int smallEntrySize;
    private int entriesPerSegment;

    public HugeConfig clone() {
        try {
            return (HugeConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    public int getSmallEntrySize() {
        return smallEntrySize;
    }

    public HugeConfig setSmallEntrySize(int smallEntrySize) {
        this.smallEntrySize = Maths.nextPower2(smallEntrySize, 64);
        return this;
    }

    public int getSegments() {
        return segments;
    }

    public HugeConfig setSegments(int segments) {
        this.segments = Maths.nextPower2(segments, 16);
        return this;
    }

    public int getEntriesPerSegment() {
        return entriesPerSegment;
    }

    public HugeConfig setEntriesPerSegment(int entriesPerSegment) {
        this.entriesPerSegment = Maths.nextPower2(entriesPerSegment, 1);
        return this;
    }

}
