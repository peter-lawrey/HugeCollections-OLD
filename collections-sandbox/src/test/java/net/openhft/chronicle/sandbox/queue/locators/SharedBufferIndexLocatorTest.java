package net.openhft.chronicle.sandbox.queue.locators;

import junit.framework.TestCase;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

/**
 * Created by Rob Austin
 */
@RunWith(value = Parameterized.class)
public class SharedBufferIndexLocatorTest extends TestCase {


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        int capacityInBytes = 256 / 8;
        return Arrays.asList(new Object[][]{
                {
                        new ATSDirectBitSet(new ByteBufferBytes(
                                ByteBuffer.allocate(capacityInBytes)))
                },
                {
                        new SingleThreadedDirectBitSet(new ByteBufferBytes(
                                ByteBuffer.allocate(capacityInBytes)))
                }
        });
    }


}
