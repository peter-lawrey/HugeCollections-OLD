package net.openhft.chronicle.sandbox.queue.locators.shared.remote;

import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Rob Austin
 */
public class SocketWriter<E> {

    private static Logger LOG = Logger.getLogger(SocketWriter.class.getName());
    final ByteBuffer intBuffer = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
    @NotNull
    private final ExecutorService producerService;
    private final SocketChannelProvider socketChannelProvider;
    @NotNull
    private final String name;


    /**
     * @param producerService       this must be a single threaded executor
     * @param socketChannelProvider
     * @param name
     */
    public SocketWriter(@NotNull final ExecutorService producerService,
                        @NotNull final SocketChannelProvider socketChannelProvider,
                        @NotNull final String name) {
        this.producerService = producerService;
        this.socketChannelProvider = socketChannelProvider;
        this.name = name;
    }


    /**
     * used to writeBytes a byte buffer bytes to the socket at {@param offset} and {@param length}
     * It is assumed that the byte buffer will contain the bytes of a serialized instance,
     * The first thing that is written to the socket is the {@param length}, this should be size of your serialized instance
     *
     * @param directBytes
     * @param offset
     * @param length      this should be size of your serialized instance
     */
    public void writeBytes(final ByteBufferBytes directBytes, int offset, final int length) {

        final ByteBuffer buffer = directBytes.buffer();

        final ByteBuffer slice = buffer.slice();
        slice.position(offset);
        slice.limit(offset + length);

        // final ByteBufferBytes slice = directBytes.createSlice(offset, length);

        producerService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    final SocketChannel socketChannel = socketChannelProvider.getSocketChannel();
                    socketChannel.write(slice);
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }
        });

    }

    /**
     * the index is encode as a negative number when put on the wire, this is because positive number are used to demote the size of preceding serialized instance
     *
     * @param value used to write an int to the socket
     */
    public void writeInt(final int value) {

        producerService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    final SocketChannel socketChannel = socketChannelProvider.getSocketChannel();

                    intBuffer.clear();
                    intBuffer.putInt(value);
                    intBuffer.flip();

                    socketChannel.write(intBuffer);

                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                    e.printStackTrace();
                }
            }

        });
    }

    @Override
    public String toString() {
        return "SocketWriter{" +
                ", name='" + name + '\'' +
                '}';
    }
}