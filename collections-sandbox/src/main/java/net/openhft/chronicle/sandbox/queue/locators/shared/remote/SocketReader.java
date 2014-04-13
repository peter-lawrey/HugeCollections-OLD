package net.openhft.chronicle.sandbox.queue.locators.shared.remote;

import net.openhft.chronicle.sandbox.queue.locators.shared.Index;
import net.openhft.chronicle.sandbox.queue.locators.shared.OffsetProvider;
import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Starts a thread and reads of the socket
 */
public class SocketReader implements Runnable {

    public static final int RECEIVE_BUFFER_SIZE = 256 * 1024;
    private static Logger LOG = Logger.getLogger(SocketReader.class.getName());
    private final Index ringIndex;

    @NotNull
    private final ByteBuffer targetBuffer;

    @NotNull
    private final OffsetProvider offsetProvider;
    @NotNull
    private final SocketChannelProvider socketChannelProvider;

    // use one buffer for
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(RECEIVE_BUFFER_SIZE).order(ByteOrder.nativeOrder());
    private final ByteBuffer rbuffer = buffer.slice();
    private final ByteBuffer wbuffer = buffer.slice();


    /**
     * @param ringIndex
     * @param targetBuffer          the buffer that supports the offset provider
     * @param offsetProvider        the location into the buffer for an index location
     * @param socketChannelProvider
     */
    public SocketReader(@NotNull final Index ringIndex,
                        @NotNull final ByteBuffer targetBuffer,
                        @NotNull final OffsetProvider offsetProvider,
                        @NotNull final SocketChannelProvider socketChannelProvider) {
        this.ringIndex = ringIndex;
        this.offsetProvider = offsetProvider;
        this.socketChannelProvider = socketChannelProvider;
        this.targetBuffer = targetBuffer.slice();
    }


    @Override
    public void run() {


        try {
            final SocketChannel socketChannel = socketChannelProvider.getSocketChannel();

            wbuffer.clear();

            for (; ; ) {
                rbuffer.clear();
                rbuffer.limit(0);

                // read an int from the socket
                while (wbuffer.position() < 4) {
                    int len = socketChannel.read(wbuffer);
                    if (len < 0) throw new EOFException();
                }

                rbuffer.limit(wbuffer.position());
                int intValue = rbuffer.getInt();

                // if this int is negative then we are using it to demote and writerLocation change
                if (intValue <= 0) {
                    ringIndex.setNextLocation(-intValue);
                } else {

                    int endOfMessageOffset = intValue + 4;

                    while (wbuffer.position() < endOfMessageOffset) {
                        int len = socketChannel.read(wbuffer);
                        if (len < 0) throw new EOFException();
                    }

                    // to allow the target buffer to read uo to the end of the message
                    rbuffer.limit(endOfMessageOffset);

                    int offset = offsetProvider.getOffset(ringIndex.getProducerWriteLocation());
                    targetBuffer.position(offset);
                    targetBuffer.put(rbuffer);

                }

                wbuffer.limit(rbuffer.position());
                wbuffer.position(rbuffer.position());

                wbuffer.compact();

            }

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "", e);
        }
    }

}
