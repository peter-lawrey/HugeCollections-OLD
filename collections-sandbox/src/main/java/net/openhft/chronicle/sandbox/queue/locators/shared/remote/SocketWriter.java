package net.openhft.chronicle.sandbox.queue.locators.shared.remote;

import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Rob Austin
 */
public class SocketWriter<E> {

    private static Logger LOG = Logger.getLogger(SocketWriter.class.getName());


    @NotNull
    private final String name;
    private final AtomicBoolean isBusy = new AtomicBoolean(true);
    // intentionally not volatile
    private int offset = Integer.MIN_VALUE;
    private long value = Long.MIN_VALUE;
    private int length = Integer.MIN_VALUE;
    private Type type = Type.INT;

    /**
     * @param producerService       this must be a single threaded executor
     * @param socketChannelProvider
     * @param name
     * @param buffer
     */
    public SocketWriter(@NotNull final ExecutorService producerService,
                        @NotNull final SocketChannelProvider socketChannelProvider,
                        @NotNull final String name,
                        @NotNull final ByteBuffer buffer) {

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
                        try {

                            synchronized (isBusy) {
                                isBusy.set(false);
                                isBusy.wait();
                            }

                            if (type == Type.INT) {
                                intBuffer.clear();
                                final long value1 = SocketWriter.this.value;
                                intBuffer.putInt((int) value1);
                                intBuffer.flip();
                                socketChannel.write(intBuffer);
                            } else if (type == Type.BYTES) {
                                int offset = SocketWriter.this.offset;
                                byteBuffer.limit(offset + SocketWriter.this.length);
                                byteBuffer.position(offset);
                                socketChannel.write(byteBuffer);
                            } else {
                                throw new IllegalArgumentException("unsupported type=" + type);
                            }


                        } catch (Exception e) {
                            LOG.log(Level.SEVERE, "", e);
                        }

                    }

                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }
        });
    }

    /**
     * used to writeBytes a byte buffer bytes to the socket at {@param offset} and {@param length}
     * It is assumed that the byte buffer will contain the bytes of a serialized instance,
     * The first thing that is written to the socket is the {@param length}, this should be size of your serialized instance
     *
     * @param offset
     * @param length this should be size of your serialized instance
     */

    public void writeBytes(int offset, final int length) {

        while (!isBusy.compareAndSet(false, true)) {
            // spin lock -  we have to add the spin lock so that messages are not skipped
        }

        synchronized (isBusy) {
            this.type = Type.BYTES;
            this.length = length;
            this.offset = offset;
            isBusy.notifyAll();
        }


    }

    /**
     * the index is encode as a negative number when put on the wire, this is because positive number are used to demote the size of preceding serialized instance
     *
     * @param value used to write an int to the socket
     */
    public void writeInt(final int value) {

        while (!isBusy.compareAndSet(false, true)) {
            // spin lock -  we have to add the spin lock so that messages are not skipped
        }

        synchronized (isBusy) {
            this.type = Type.INT;
            this.value = value;
            isBusy.notifyAll();
        }
    }

    @Override
    public String toString() {
        return "SocketWriter{" +
                ", name='" + name + '\'' +
                '}';
    }


    private enum Type {INT, BYTES}

}