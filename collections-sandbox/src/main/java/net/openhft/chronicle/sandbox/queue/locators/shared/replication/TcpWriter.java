package net.openhft.chronicle.sandbox.queue.locators.shared.replication;

import net.openhft.chronicle.sandbox.queue.locators.shared.SharedDataLocator;
import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;

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
public class TcpWriter<E> extends SharedDataLocator<E, ByteBufferBytes> {

    private static Logger LOG = Logger.getLogger(TcpReader.class.getName());
    @NotNull
    private final ExecutorService producerService;
    private final ByteBuffer sizeBuffer = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
    private Connection connection;

    /**
     * @param valueClass
     * @param capacity        the maximum number of values that can be stored in the ring
     * @param valueMaxSize    the maximum size that an value can be, if a value is written to the buffer that is larger than this an exception will be thrown
     * @param readerSlice     an instance of the MessageStore used by the reader
     * @param writerSlice     an instance of the MessageStore used by the writer
     * @param producerService
     */
    public TcpWriter(@NotNull final Class<E> valueClass,
                     final int capacity, int valueMaxSize,
                     @NotNull final ByteBufferBytes readerSlice,
                     @NotNull final ByteBufferBytes writerSlice,
                     @NotNull final String hostname,
                     final int port,
                     @NotNull final ExecutorService producerService) throws IOException {
        super(valueClass, capacity, valueMaxSize, readerSlice, writerSlice);
        this.producerService = producerService;

        new Connection(hostname, port + 1);
    }

    @Override
    public int setData(int index, E value) {

        final SocketChannel socket = this.connection.isOpen() ? this.connection.getSocket() : this.connection.openSocket();

        final int size = super.setData(index, value);
        sizeBuffer.clear();
        sizeBuffer.putInt(size);

        final int offset = getOffset(index);
        final int len = getWriterSlice().readInt(offset);
        final ByteBufferBytes data = getWriterSlice().createSlice(0, offset + len);

        try {
            socket.write(sizeBuffer);
            socket.write(data.buffer());
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "", e);
        }

        return size;
    }


}
