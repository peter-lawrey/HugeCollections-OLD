package eg;

import net.openhft.affinity.AffinitySupport;
import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.MaxSize;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/*
 tune the kernel to maximise the amount of cached write data.

 vm.dirty_background_ratio = 30
 vm.dirty_expire_centisecs = 3000
 vm.dirty_ratio = 60
 vm.dirty_writeback_centisecs = 500
 */
public class BigData {
    static SharedHashMap<LongWrapper, BigDataStuff> TheSharedMap;
    public static final int ENTRY_SIZE = 8 * 1024;
    final static long MAX_ENTRIES = 10 * 1000 * 1000L; // 128L * 100000000L / ENTRY_SIZE;
    final static long Highwatermark = MAX_ENTRIES / 10; // assume 10% of data is active.

    static final SharedHashMapBuilder<LongWrapper, BigDataStuff> builder = new SharedHashMapBuilder<LongWrapper, BigDataStuff>();


    static {
        builder.actualSegments(8 * 1024);
        builder.entrySize(ENTRY_SIZE);
        builder.entries(MAX_ENTRIES);
        builder.largeSegments(builder.entries() / builder.actualSegments() > 32768);
        String dir = System.getProperty("dir", null);
        if (dir == null) {
            if (new File("/tmp").exists()) dir = "/tmp";
            else dir = ".";
        }
        String shmPath;
        if (dir.startsWith("/dev/")) {
            shmPath = dir;
        } else {
            shmPath = dir.startsWith("/dev/") ? dir : dir + "/testmap-" + Long.toString(System.nanoTime(), 36);
            new File(shmPath).deleteOnExit();
        }
        System.out.println("SharedHashMap: " + builder);
        try {
            TheSharedMap = builder.create(new File(shmPath), LongWrapper.class, BigDataStuff.class);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        initialbuild();
/*
        System.out.println("Start highwatermark " + Highwatermark / 1000000 + "M, entrySize="+ENTRY_SIZE);
        for (int i = 0; i < 3; i++) {
            Thread t1 = new Thread("test 1") {
                public void run() {
                    _test();
                }
            };
            Thread t2 = new Thread("test 2") {
                public void run() {
                    _test();
                }
            };
            Thread t3 = new Thread("test 3") {
                public void run() {
                    _test();
                }
            };
            t1.start();
            t2.start();
            t3.start();
            _test();
            t1.join();
            t2.join();
            t3.join();
        }
        System.out.println("End highwatermark " + Highwatermark / 1000000 + "M");
*/
        long time = System.currentTimeMillis() - start;
        System.out.printf("End to end took %.1f%n", time / 1e3);
    }

    public static void initialbuild() throws IOException, InterruptedException {
        System.out.println("building an empty map");
        long start = System.currentTimeMillis();
        final List<Thread> threads = new ArrayList<Thread>();
        final int n_threads = 8;
        for (int i = 1; i < n_threads; i++) {
            final int n = i;
            Thread t1 = new Thread("test " + n) {
                public void run() {
                    try {
                        populate(n, n_threads);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
            t1.start();
            threads.add(t1);
        }
        populate(0, n_threads);
        for (Thread thread : threads) {
            thread.join();
        }
        long now = System.currentTimeMillis();
        System.out.println(builder);
        System.out.println("Time taken to insert all entries " + ((now - start) / 1000.0) + " seconds");
    }

    public static void populate(int n, int threads) throws InterruptedException {
        AffinitySupport.setThreadId();

        long start = System.currentTimeMillis();
        BigDataStuff value = DataValueClasses.newDirectReference(BigDataStuff.class);
        StringBuilder text = new StringBuilder();
        LongWrapper lw = new LongWrapper();
        for (long i = n; i < MAX_ENTRIES; i += threads) {
            if (n == 0 && i % (10 * 1000 * 1000) == 0) {
                System.out.println("Now inserted to " + i + " seconds since start = " + ((System.currentTimeMillis() - start) / 1000L));
            }
            lw.l = i;
            assertNotNull(TheSharedMap.acquireUsing(lw, value));
            text.setLength(0);
            text.append("text-");
            text.append(i);

            value.busyLockGuard();
            try {
                value.setX(i);
                value.setText(text);
            } finally {
                value.unlockGuard();
            }
        }
    }

    public static void test() throws IOException, InterruptedException {
        //do a sequence 1m of each of insert/read/update
        //inserts
        long LOOPCOUNT = MAX_ENTRIES / 10;
        long[] rand = {0L};
        long start = System.currentTimeMillis();
        BigDataStuff value = DataValueClasses.newDirectReference(BigDataStuff.class);
        StringBuilder text = new StringBuilder();
        LongWrapper lw = new LongWrapper();
        for (long i = 0; i < LOOPCOUNT; i++) {
            long current = randomKey(rand);
            lw.l = i;
            text.setLength(0);
            text.append("text-");
            text.append(i);

            assertNotNull(TheSharedMap.getUsing(lw, value));
            value.busyLockGuard();
            try {
                value.setX(current);
                value.setText(text);
            } finally {
                value.unlockGuard();
            }
        }
        long now = System.currentTimeMillis();
        System.out.println("Time taken to replace " + LOOPCOUNT / 1000 / 1e3 + "M entries " + ((now - start) / 1000.0) + " seconds");

        int count = 0;
        start = System.currentTimeMillis();
        for (long i = 0; i < LOOPCOUNT; i++) {
            long keyval = randomKey(rand);
            count++;
            lw.l = keyval;
            BigDataStuff stuff = TheSharedMap.getUsing(lw, value);
            if (stuff == null) {
                System.out.println("hit an empty at key " + keyval);
            }
        }
        now = System.currentTimeMillis();
        System.out.println("Time taken to read " + count / 1000 / 1e3 + "M entries " + ((now - start) / 1000.0) + " seconds");

        start = System.currentTimeMillis();
        count = 0;
        for (long i = 0; i < LOOPCOUNT; i++) {
            long keyval = randomKey(rand);
            lw.l = keyval;
            BigDataStuff stuff = TheSharedMap.getUsing(lw, value);
            if (stuff == null) {
                System.out.println("hit an empty at key " + keyval);
            } else {
                count++;
                TheSharedMap.getUsing(lw, value);
                value.busyLockGuard();
                try {
                    value.addX(1);
                    value.getUsingText(text);
                    text.append("1");
                    value.setText(text);
                } finally {
                    value.unlockGuard();
                }
            }
        }
        now = System.currentTimeMillis();
        System.out.println("Time taken to read+update " + count / 1000 / 1e3 + "M entries " + ((now - start) / 1000.0) + " seconds");
    }

    private static long randomKey(long[] value) {
        long v = value[0];
        http:
//primes.utm.edu/curios/page.php/100019.html
        v += 100019;
        if (v >= Highwatermark)
            v -= Highwatermark;
        return value[0] = v;
    }
}

interface BigDataStuff {

    void busyLockGuard() throws InterruptedException, IllegalStateException;

    void unlockGuard() throws IllegalMonitorStateException;

    public long getX();

    public void setX(long x);

    public long addX(long i);

    public String getText();

    public StringBuilder getUsingText(StringBuilder sb);

    public void setText(@MaxSize(100) CharSequence cs);
}

class LongWrapper implements Externalizable {
    long l;

    public LongWrapper() {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Bytes b = (Bytes) out;
        b.writeInt48(l);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        Bytes b = (Bytes) in;
        l = b.readInt48();
    }
}
