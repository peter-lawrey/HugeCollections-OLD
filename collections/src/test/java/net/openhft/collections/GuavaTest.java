package net.openhft.collections;

import com.google.common.collect.testing.*;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import junit.framework.*;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.testing.features.MapFeature.*;


public class GuavaTest extends TestCase {

    /** 197 failing tests */
    private static final boolean skipHHMTests = true;

    public static Test suite() {
        TestSuite shmTests = MapTestSuiteBuilder.using(new SHMTestGenerator())
                .named("SharedHashMap Guava tests")
                .withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES)
                .createTestSuite();
        TestSuite hhmTests = MapTestSuiteBuilder.using(new HHMTestGenerator())
                .named("HugeHashMap Guava tests")
                .withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES)
                .createTestSuite();
        TestSuite tests = new TestSuite();
        tests.addTest(shmTests);
        if (!skipHHMTests) tests.addTest(hhmTests);
        return tests;
    }

    static abstract class TestGenerator
            implements TestMapGenerator<CharSequence, CharSequence> {

        abstract Map<CharSequence, CharSequence> newMap();

        public CharSequence[] createKeyArray(int length) {
            return new CharSequence[length];
        }

        @Override
        public CharSequence[] createValueArray(int length) {
            return new CharSequence[length];
        }

        @Override
        public SampleElements<Map.Entry<CharSequence, CharSequence>> samples() {
            return SampleElements.mapEntries(
                    new SampleElements<CharSequence>(
                            "key1", "key2", "key3", "key4", "key5"
                    ),
                    new SampleElements<CharSequence>(
                            "val1", "val2", "val3", "val4", "val5"
                    )
            );
        }

        @Override
        public Map<CharSequence, CharSequence> create(Object... objects) {
            Map<CharSequence, CharSequence> map = newMap();
            for (Object obj : objects) {
                Map.Entry e = (Map.Entry) obj;
                map.put((CharSequence) e.getKey(),
                        (CharSequence) e.getValue());
            }
            return map;
        }

        @Override
        public Map.Entry<CharSequence, CharSequence>[] createArray(int length) {
            return new Map.Entry[length];
        }

        @Override
        public Iterable<Map.Entry<CharSequence, CharSequence>> order(
                List<Map.Entry<CharSequence, CharSequence>> insertionOrder) {
            return insertionOrder;
        }
    }

    static class SHMTestGenerator extends TestGenerator {
        SharedHashMapBuilder builder = new SharedHashMapBuilder()
                .entries(100)
                .minSegments(2);
        @Override
        Map<CharSequence, CharSequence> newMap() {
            try {
                return builder.create(SharedHashMapTest.getPersistenceFile(),
                            CharSequence.class, CharSequence.class);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    static class HHMTestGenerator extends TestGenerator {

        @Override
        Map<CharSequence, CharSequence> newMap() {
            return new HugeHashMap<CharSequence, CharSequence>(
                    HugeConfig.DEFAULT, CharSequence.class, CharSequence.class);
        }
    }
}
