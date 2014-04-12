package net.openhft.chronicle.sandbox.queue;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;
import net.openhft.chronicle.sandbox.queue.locators.RingIndex;
import net.openhft.chronicle.sandbox.queue.locators.local.LocalDataLocator;
import net.openhft.chronicle.sandbox.queue.locators.local.LocalRingIndex;
import net.openhft.chronicle.sandbox.queue.locators.shared.BytesDataLocator;
import net.openhft.chronicle.sandbox.queue.locators.shared.SharedRingIndex;
import net.openhft.chronicle.sandbox.queue.locators.shared.remote.Consumer;
import net.openhft.chronicle.sandbox.queue.locators.shared.remote.Producer;
import net.openhft.chronicle.sandbox.queue.locators.shared.remote.SocketWriter;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.MappedStore;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

/**
 * Created by Rob Austin
 */
public class ConcurrentBlockingObjectQueueBuilder<E> {

    public static final int SIZE_OF_INT = 4;
    public static final int RECEIVE_BUFFER_SIZE = 256 * 1024;
    public static final int LOCK_TIME_OUT_NS = 100;
    private static Logger LOG = Logger.getLogger(SocketWriter.class.getName());
    int capacity;
    boolean isShared;
    int maxSize;
    Class<E> clazz;
    Type type = Type.LOCAL;
    private int portNumber;
    private String host;

    /**
     * returns the value to nearest {@parm powerOf2}
     */
    public static int align(int capacity, int powerOf2) {
        return (capacity + powerOf2 - 1) & ~(powerOf2 - 1);
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public void setCapacity(int capacity) {
        // the ring buffer works by having 1 item spare, that's why we add one
        this.capacity = capacity + 1;
    }

    public void setClazz(Class<E> clazz) {
        this.clazz = clazz;
    }

    public void isShared(boolean isShared) {
        this.isShared = isShared;
    }

    public BlockingQueue<E> create() throws IOException {

        final DataLocator dataLocator;
        final RingIndex ringIndex;

        if (type == Type.LOCAL) {
            ringIndex = new LocalRingIndex();
            dataLocator = new LocalDataLocator(capacity);

        } else if (type == Type.SHARED) {

            final String tmp = System.getProperty("java.io.tmpdir");
            final File file = new File(tmp + "/share-queue-test" + System.nanoTime());

            int ringIndexLocationsStart = 0;
            int ringIndexLocationsLen = SIZE_OF_INT * 2;
            int storeLen = capacity * align(maxSize, 4);

            final MappedStore ms = new MappedStore(file, FileChannel.MapMode.READ_WRITE, ringIndexLocationsLen + storeLen);
            final DirectBytes ringIndexSlice = ms.createSlice(ringIndexLocationsStart, ringIndexLocationsLen);
            ringIndex = new SharedRingIndex(ringIndexSlice);

            // provides an index to the data in the ring buffer, the size of this index is proportional to the capacity of the ring buffer
            final DirectBytes storeSlice = ms.createSlice(ringIndexLocationsLen, storeLen);
            final DirectBytes writerSlice = ms.createSlice(ringIndexLocationsLen, storeLen);

            dataLocator = new BytesDataLocator<E, DirectBytes>(clazz, capacity, maxSize, storeSlice, writerSlice);

        } else if (type == Type.REMOTE_PRODUCER || type == Type.REMOTE_CONSUMER) {

            final SocketChannel socketChannel = (type == Type.REMOTE_PRODUCER) ? producerConnectSocketChannel() : consumerConnectSocketChannel();

            final int bufferSize = capacity * align(maxSize, 4);
            final ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());
            final ByteBufferBytes byteBufferBytes = new ByteBufferBytes(buffer);

            final BytesDataLocator<E, ByteBufferBytes> bytesDataLocator = new BytesDataLocator<E, ByteBufferBytes>(
                    clazz,
                    capacity,
                    maxSize,
                    byteBufferBytes.createSlice(),
                    byteBufferBytes.createSlice());

            ringIndex = (type == Type.REMOTE_PRODUCER) ?
                    new Producer<ByteBufferBytes>(socketChannel, new LocalRingIndex(), bytesDataLocator, bytesDataLocator) :
                    new Consumer<ByteBufferBytes>(socketChannel, new LocalRingIndex(), bytesDataLocator, bytesDataLocator);

            dataLocator = bytesDataLocator;

        } else {
            throw new IllegalArgumentException("Unsupported Type=" + type);

        }

        return new ConcurrentBlockingObjectQueue<E>(ringIndex, dataLocator);

    }

    private SocketChannel producerConnectSocketChannel() throws IOException {
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.socket().setReuseAddress(true);
        serverSocket.socket().bind(new InetSocketAddress(portNumber));
        serverSocket.configureBlocking(true);
        LOG.info("Server waiting for client on port " + portNumber);
        serverSocket.socket().setReceiveBufferSize(RECEIVE_BUFFER_SIZE);
        return serverSocket.accept();
    }

    private SocketChannel consumerConnectSocketChannel() throws IOException {

        final SocketChannel sc = SocketChannel.open(new InetSocketAddress(host, portNumber));
        sc.socket().setReceiveBufferSize(256 * 1024);

        return sc;
    }

    public enum Type {

        LOCAL("running within one JVM"),
        SHARED("one producer JVM, one Consumer JVM, with the Jvm's on the same Host"),
        REMOTE_PRODUCER("One Producer that connects via tcp/ip to single REMOTE_CONSUMER"),
        REMOTE_CONSUMER("One Consumer that connects via tcp/ip to single REMOTE_PRODUCER");

        private final String about;

        Type(String about) {
            this.about = about;
        }

        @Override
        public String toString() {
            return about;
        }
    }

}
