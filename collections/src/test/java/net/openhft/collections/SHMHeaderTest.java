package net.openhft.collections;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Created by win7 on 08/03/14.
 */
public class SHMHeaderTest {
    @Test
    public void testDifferentHeaders() throws IOException {
        Random rand = new Random(1);
        for (int i = 1; i <= 1000; i++) {
            rand.setSeed(i);
//            System.out.println("i: " + i);
            File file = new File(System.getProperty("java.io.tmpdir"), "headers-" + i);
            file.deleteOnExit();
            SharedHashMapBuilder builder1 = createBuilder(rand);
            SharedHashMap<String, String> map = builder1.create(file, String.class, String.class);
            // this is the sanitized builder
            SharedHashMapBuilder builder2 = map.builder();
            map.close();
            // on reopening
            SharedHashMapBuilder builder3 = createBuilder(rand);
            SharedHashMap<String, String> map2 = builder3.create(file, String.class, String.class);
            // this is the sanitized builder
            SharedHashMapBuilder builder4 = map2.builder();
            assertEquals(builder2.toString(), builder4.toString());
            assertEquals(builder2, builder4);
            map2.close();
            // delete now if possible.
            file.delete();
        }
    }

    private static SharedHashMapBuilder createBuilder(Random rand) {
        return new SharedHashMapBuilder()
                .entrySize(rand.nextInt(100) * 3 + 1)
                .entries(rand.nextInt(1000) * 1111 + 1)
                .minSegments(rand.nextInt(200) + 1)
                .replicas(rand.nextInt(3));
    }
}
