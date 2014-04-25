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

package net.openhft.chronicle.sandbox.map.replication.socket;

import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import net.openhft.collections.SegmentModificationIterator;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;

/**
 * @author Rob Austin.
 */
public class MapSocketWriter {


    private final String name;


    /**
     * @param producerService
     * @param socketChannelProvider
     * @param name
     * @param buffer
     * @param notifier              used to notify that data is available to send via the socket.
     * @param liveItems
     * @param deletedItems
     */
    public MapSocketWriter(@NotNull final ExecutorService producerService,
                           @NotNull final SocketChannelProvider socketChannelProvider,
                           @NotNull final String name,
                           @NotNull final ByteBuffer buffer,
                           @NotNull final Object notifier,
                           @NotNull final SegmentModificationIterator liveItems,
                           @NotNull final SegmentModificationIterator deletedItems) {

        this.name = name;


        // make a local safe copy
        final ByteBuffer byteBuffer = buffer.duplicate();
        final ByteBuffer intBuffer = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());

        producerService.submit(new Runnable() {
            @Override
            public void run() {

                try {

                    final SocketChannel socketChannel = socketChannelProvider.getSocketChannel();


                    for (; ; ) {

                        boolean hasLiveItems;
                        boolean hasDeletedItems;

                        while ((hasLiveItems = liveItems.hasNext()) |
                                (hasDeletedItems = deletedItems.hasNext())) {

                            if (hasLiveItems) {
                                liveItems.nextEntry();
                                // send the data
                            }

                            if (hasDeletedItems) {
                                deletedItems.nextEntry();
                                // send the data
                            }
                        }

                        synchronized (notifier) {
                            if (!liveItems.hasNext() && !deletedItems.hasNext())
                                notifier.wait();
                        }


                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }


    @Override
    public String toString() {
        return "MapSocketWriter{" +
                "name='" + name + '\'' +
                '}';
    }
}
