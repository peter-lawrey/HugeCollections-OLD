HugeCollections
===============

Huge Collections for Java using efficient off heap storage

HugeHashMap supports concurrent access to very large, off heap key/value pairs.  From HugeHashMapTest

    HugeConfig config = HugeConfig.DEFAULT.clone()
            .setSegments(128)
            .setSmallEntrySize(128)
            .setCapacity(count);

    final HugeHashMap<CharSequence, SampleValues> map =
            new HugeHashMap<CharSequence, SampleValues>(
                    config, CharSequence.class, SampleValues.class);

    final SampleValues value = new SampleValues();
    StringBuilder user = new StringBuilder();
    for (int i = 0; i < count; i++) {
        value.ee = i;
        value.gg = i;
        value.ii = i;
        map.put(users(user, i), value);
    }
    for (int i = 0; i < count; i++) {
        assertNotNull(map.get(users(user, i), value));
        // test we get back the values we stored.
        assertEquals(i, value.ee);
        assertEquals(i, value.gg, 0.0);
        assertEquals(i, value.ii);
    }
    for (int i = 0; i < count; i++)
        map.remove(users(user, i));

HugeHashMapTest.testPutPerf() with the options "-ea -mx128m -Xmn64m -verbose:gc" prints on an Hex core i7 @ 3.5 GHz

    Starting test
    Put/get 14,020 K operations per second

This test adds, gets twice and removes 100 million keys without triggering a GC with a young generation of 64 MB.


The use of StringBuilder as the key allows the key to recycled efficiently.  String can be used but creating millions of Strings can trigger GCs.

Also note that the value object can be recycled.  There is also a get(key) method which returns a new object each time, however this can trigger GCs.

###  JavaDoc
Check out our documentation at [JavaDoc] (http://openhft.github.io/HugeCollections/apidocs/)