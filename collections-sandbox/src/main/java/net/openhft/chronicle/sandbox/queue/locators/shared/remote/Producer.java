package net.openhft.chronicle.sandbox.queue.locators.shared.remote;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;
import net.openhft.chronicle.sandbox.queue.locators.RingIndex;
import net.openhft.chronicle.sandbox.queue.locators.shared.BytesDataLocator;
import net.openhft.chronicle.sandbox.queue.locators.shared.Index;
import net.openhft.chronicle.sandbox.queue.locators.shared.OffsetProvider;
import net.openhft.chronicle.sandbox.queue.locators.shared.SliceProvider;
import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Rob Austin
 */
public class Producer<E, BYTES extends ByteBufferBytes> implements RingIndex, DataLocator<E> {

    @NotNull
    private final RingIndex ringIndex;
    private final SocketWriter tcpSender;
    private final BytesDataLocator<E, BYTES> bytesDataLocator;
    @NotNull
    private final SliceProvider<BYTES> sliceProvider;
    @NotNull
    private final OffsetProvider offsetProvider;


    public Producer(@NotNull final RingIndex ringIndex,
                    @NotNull final SliceProvider<BYTES> sliceProvider,
                    @NotNull final OffsetProvider offsetProvider,
                    @NotNull final SocketChannelProvider socketChannelProvider,
                    @NotNull final BytesDataLocator<E, BYTES> bytesDataLocator) {
        this.sliceProvider = sliceProvider;
        this.offsetProvider = offsetProvider;

        final Index index = new Index() {

            @Override
            public void setNextLocation(int index) {
                ringIndex.setReadLocation(index);
            }

            @Override
            public int getProducerWriteLocation() {
                throw new UnsupportedOperationException();
            }

        };

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new SocketReader(index, sliceProvider.getWriterSlice().buffer(), offsetProvider, socketChannelProvider, "Producer"));

        final ExecutorService producerService = Executors.newSingleThreadExecutor();
        tcpSender = new SocketWriter(producerService, socketChannelProvider, "Producer");
        this.ringIndex = ringIndex;
        this.bytesDataLocator = bytesDataLocator;
    }

    @Override
    public int getWriterLocation() {
        return ringIndex.getWriterLocation();
    }

    @Override
    public void setWriterLocation(int nextWriteLocation) {
        ringIndex.setWriterLocation(nextWriteLocation);
        tcpSender.writeInt(-nextWriteLocation);
    }

    @Override
    public int getReadLocation() {
        return ringIndex.getReadLocation();
    }

    @Override
    public void setReadLocation(int nextReadLocation) {
        ringIndex.setReadLocation(nextReadLocation);
    }

    @Override
    public int getProducerWriteLocation() {
        return ringIndex.getProducerWriteLocation();
    }

    @Override
    public void setProducerWriteLocation(int nextWriteLocation) {
        ringIndex.setProducerWriteLocation(nextWriteLocation);
    }

    @Override
    public E getData(int readLocation) {
        return bytesDataLocator.getData(readLocation);
    }

    @Override
    public int setData(int index, E value) {
        final int len = bytesDataLocator.setData(index, value);

        // todo we maybe able to optomize this out
        int offset = offsetProvider.getOffset(index);

        tcpSender.writeInt(len);
        tcpSender.writeBytes(sliceProvider.getWriterSlice(), offset, len);

        return len;
    }

    @Override
    public void writeAll(E[] newData, int length) {
        bytesDataLocator.writeAll(newData, length);
    }

    @Override
    public int getCapacity() {
        return bytesDataLocator.getCapacity();
    }
}
