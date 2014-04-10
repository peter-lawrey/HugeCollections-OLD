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

package net.openhft.chronicle.sandbox.queue.locators.shared.replication;

import net.openhft.chronicle.sandbox.queue.locators.shared.SharedDataLocator;
import net.openhft.chronicle.sandbox.queue.locators.shared.SharedRingIndex;
import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listens to data from a remote writer, pushed updates of reader location
 */
public class TcpReader<E> extends SharedDataLocator<E, ByteBufferBytes> {

    private static Logger LOG = Logger.getLogger(TcpReader.class.getName());
    private final ByteBuffer writeIntBuffer = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
    @NotNull
    private final ExecutorService producerService;
    private Connection connection;

    /**
     * @param valueClass
     * @param capacity        the maximum number of values that can be stored in the ring
     * @param valueMaxSize    the maximum size that an value can be, if a value is written to the buffer that is larger than this an exception will be thrown
     * @param readerSlice     an instance of the MessageStore used by the reader
     * @param writerSlice     an instance of the MessageStore used by the writer
     * @param producerService
     */


    public TcpReader(@NotNull final Class<E> valueClass,
                     final int capacity, int valueMaxSize,
                     @NotNull final ByteBufferBytes readerSlice,
                     @NotNull final ByteBufferBytes writerSlice,
                     @NotNull final String hostname,
                     final int port,
                     @NotNull final ExecutorService consumerService,
                     @NotNull final SharedRingIndex sharedRingIndex,
                     @NotNull final ExecutorService producerService) throws IOException {
        super(valueClass, capacity, valueMaxSize, readerSlice, writerSlice);
        this.producerService = producerService;
        consumerService.submit(new SocketReader(sharedRingIndex, writerSlice.createSlice(), new Connection(hostname, port)));
        connection = new Connection(hostname, port + 1);
    }


    public void start() {
        final SocketChannel socket = this.connection.isOpen() ? this.connection.getSocket() : this.connection.openSocket();
    }


    /**
     * when we read data, we have to tell the writer so that it knows that it can free up this part of its ring buffer
     *
     * @param index
     * @return
     */
    @Override
    public E getData(final int index) {
        final E data = super.getData(index);

        tcpBoradcastCellRead(index);
        return data;

    }

    /**
     * tells the write that the read has now read this cell
     *
     * @param index
     */
    private void tcpBoradcastCellRead(final int index) {
        producerService.submit(new Runnable() {

            @Override
            public void run() {
                writeIntBuffer.clear();
                writeIntBuffer.putInt(index);
                try {
                    connection.getSocket().write(writeIntBuffer);
                } catch (IOException e) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }
        });
    }


    class SocketReader implements Runnable {

        private final SharedRingIndex ringIndex;
        private final Connection connection;

        // use one buffer for
        private final ByteBuffer buffer = ByteBuffer.allocateDirect(valueMaxSize).order(ByteOrder.nativeOrder());
        private final ByteBuffer rbuffer = buffer.slice();
        private final ByteBuffer wbuffer = buffer.slice();
        private final ByteBuffer intBuffer = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
        private final ByteBufferBytes writerSlice;

        public SocketReader(@NotNull final SharedRingIndex ringIndex,
                            @NotNull final ByteBufferBytes writerSlice,
                            @NotNull final Connection connection) {
            this.ringIndex = ringIndex;
            this.writerSlice = writerSlice;
            this.connection = connection;
        }

        @Override
        public void run() {

            final SocketChannel socket = this.connection.isOpen() ? this.connection.getSocket() : this.connection.openSocket();


            for (; ; ) {

                try {
                    // TODO Rob to have a look at this code.
                    if (false) {
                        while (buffer.position() < 4) {
                            int len = socket.read(buffer);
                            if (len < 0) throw new EOFException();
                        }
                        int msgLen = buffer.getInt(0);
                        buffer.flip();
                        while (buffer.remaining() + 4 < msgLen) {
                            buffer.flip();
                            int len = socket.read(buffer);
                            if (len < 0) throw new EOFException();
                            buffer.flip();
                        }
                        // copy data.
                        // todo not needed in read case.
                        buffer.position(buffer.position() + msgLen);
                        if (buffer.remaining() == 0)
                            buffer.clear();
                        else
                            buffer.compact();
                    }

                    intBuffer.reset();

                    // first we will read the size
                    final int expectedSize = readInt(socket);

                    // non volatile read ( which is quicker )
                    int offset = getOffset(ringIndex.getProducerWriteLocation());

                    // read the data from tcp/ip into the buffer
                    writerSlice.position(offset);
                    writerSlice.limit(offset + expectedSize);
                    final int actualSize = socket.read(writerSlice.buffer());

                    if (expectedSize != actualSize)
                        throw new IllegalStateException("expectedSize=" + expectedSize + "bytes but actualSize=" + actualSize + "bytes");

                    // now we will read the next write location
                 /*   final int nextWriteLocation = readInt(socket);
                    ringIndex.setWriterLocation(nextWriteLocation);
*/
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }
        }


        private int readInt(@NotNull final SocketChannel socketChannel) throws IOException {

            // read the size
            final int size = socketChannel.read(intBuffer);

            if (size != 4)
                throw new IllegalStateException("expecting a size of 4bytes.");

            return intBuffer.getInt();
        }

    }
}
