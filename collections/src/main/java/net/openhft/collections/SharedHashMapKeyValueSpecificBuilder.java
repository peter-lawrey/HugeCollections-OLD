/*
 * Copyright 2014 Higher Frequency Trading
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import net.openhft.lang.io.*;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.io.serialization.impl.*;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.model.constraints.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;

import static net.openhft.collections.Objects.builderEquals;
import static net.openhft.collections.Objects.hash;

public final class SharedHashMapKeyValueSpecificBuilder<K, V> implements Cloneable {
    private static final Logger LOG = LoggerFactory.getLogger(SharedHashMapKeyValueSpecificBuilder.class);

    private static boolean marshallerUseFactory(Class c) {
        return Byteable.class.isAssignableFrom(c) ||
                BytesMarshallable.class.isAssignableFrom(c) ||
                Externalizable.class.isAssignableFrom(c);
    }

    @SuppressWarnings("unchecked")
    private static <T> BytesMarshaller<T> chooseDefaultMarshaller(@NotNull Class<T> tClass) {
        if (Byteable.class.isAssignableFrom(tClass))
            return new ByteableMarshaller(tClass);
        if (BytesMarshallable.class.isAssignableFrom(tClass))
            return new BytesMarshallableMarshaller(tClass);
        if (Externalizable.class.isAssignableFrom(tClass))
            return new ExternalizableMarshaller(tClass);
        if (tClass == CharSequence.class)
            return (BytesMarshaller<T>) CharSequenceMarshaller.INSTANCE;
        if (tClass == String.class)
            return (BytesMarshaller<T>) StringMarshaller.INSTANCE;
        if (tClass == Integer.class)
            return (BytesMarshaller<T>) IntegerMarshaller.INSTANCE;
        if (tClass == Long.class)
            return (BytesMarshaller<T>) LongMarshaller.INSTANCE;
        if (tClass == Double.class)
            return (BytesMarshaller<T>) DoubleMarshaller.INSTANCE;
        return SerializableMarshaller.INSTANCE;
    }

    private static class ByteableMarshaller<T extends Byteable> implements BytesMarshaller<T> {
        private static final long serialVersionUID = 0L;

        @NotNull final Class<T> tClass;

        ByteableMarshaller(@NotNull Class<T> tClass) {
            this.tClass = tClass;
        }

        @Override
        public void write(Bytes bytes, T t) {
            bytes.write(t.bytes(), t.offset(), t.maxSize());
        }

        @Nullable
        @Override
        public T read(Bytes bytes) {
            return read(bytes, null);
        }

        @Nullable
        @Override
        public T read(Bytes bytes, @Nullable T t) {
            try {
                if (t == null)
                    t = getInstance();
                t.bytes(bytes, bytes.position());
                bytes.skip(t.maxSize());
                return t;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @NotNull
        T getInstance() throws Exception {
            return (T) NativeBytes.UNSAFE.allocateInstance(tClass);
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && obj.getClass() == getClass() &&
                    ((ByteableMarshaller) obj).tClass == tClass;
        }

        @Override
        public int hashCode() {
            return tClass.hashCode();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{tClass=" + tClass + "}";
        }
    }

    private static class ByteableMarshallerWithCustomFactory<T extends Byteable>
            extends ByteableMarshaller<T> {
        private static final long serialVersionUID = 0L;

        @NotNull private final ObjectFactory<T> factory;

        ByteableMarshallerWithCustomFactory(@NotNull Class<T> tClass,
                                            @NotNull ObjectFactory<T> factory) {
            super(tClass);
            this.factory = factory;
        }

        @NotNull
        @Override
        T getInstance() throws Exception {
            return factory.create();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass())
                return false;
            ByteableMarshallerWithCustomFactory that = (ByteableMarshallerWithCustomFactory) obj;
            return that.tClass == tClass && that.factory.equals(this.factory);
        }

        @Override
        public int hashCode() {
            return hash(tClass, factory);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{tClass=" + tClass + ",factory=" + factory + "}";
        }
    }

    private static
    class BytesMarshallableMarshallerWithCustomFactory<T extends BytesMarshallable>
            extends BytesMarshallableMarshaller<T> {
        private static final long serialVersionUID = 0L;

        @NotNull private final ObjectFactory<T> factory;

        BytesMarshallableMarshallerWithCustomFactory(@NotNull Class<T> tClass,
                                                     @NotNull ObjectFactory<T> factory) {
            super(tClass);
            this.factory = factory;
        }

        @NotNull
        @Override
        protected T getInstance() throws Exception {
            return factory.create();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass())
                return false;
            BytesMarshallableMarshallerWithCustomFactory that =
                    (BytesMarshallableMarshallerWithCustomFactory) obj;
            return that.marshaledClass() == marshaledClass() && that.factory.equals(this.factory);
        }

        @Override
        public int hashCode() {
            return hash(marshaledClass(), factory);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{marshaledClass=" + marshaledClass() +
                    ",factory=" + factory + "}";
        }
    }

    private static class ExternalizableMarshallerWithCustomFactory<T extends Externalizable>
            extends ExternalizableMarshaller<T> {
        private static final long serialVersionUID = 0L;

        @NotNull private final ObjectFactory<T> factory;

        ExternalizableMarshallerWithCustomFactory(@NotNull Class<T> tClass,
                                                  @NotNull ObjectFactory<T> factory) {
            super(tClass);
            this.factory = factory;
        }

        @NotNull
        @Override
        protected T getInstance() throws Exception {
            return factory.create();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass())
                return false;
            ExternalizableMarshallerWithCustomFactory that =
                    (ExternalizableMarshallerWithCustomFactory) obj;
            return that.marshaledClass() == marshaledClass() && that.factory.equals(this.factory);
        }

        @Override
        public int hashCode() {
            return hash(marshaledClass(), factory);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{marshaledClass=" + marshaledClass() +
                    ",factory=" + factory + "}";
        }
    }

    private static enum CharSequenceMarshaller implements BytesMarshaller<CharSequence> {
        INSTANCE;
        @Override
        public void write(Bytes bytes, CharSequence s) {
            bytes.writeUTFΔ(s);
        }

        @Nullable
        @Override
        public CharSequence read(Bytes bytes) {
            return bytes.readUTFΔ();
        }

        @Nullable
        @Override
        public CharSequence read(Bytes bytes, @Nullable CharSequence s) {
            if (s instanceof StringBuilder) {
                if (bytes.readUTFΔ((StringBuilder) s))
                    return s;
                return null;
            }
            return bytes.readUTFΔ();
        }
    }

    private static enum StringMarshaller implements BytesMarshaller<String> {
        INSTANCE;
        @Override
        public void write(Bytes bytes, String s) {
            bytes.writeUTFΔ(s);
        }

        @Nullable
        @Override
        public String read(Bytes bytes) {
            return bytes.readUTFΔ();
        }

        @Nullable
        @Override
        public String read(Bytes bytes, @Nullable String s) {
            return bytes.readUTFΔ();
        }
    }

    private static enum IntegerMarshaller implements BytesMarshaller<Integer> {
        INSTANCE;
        @Override public void write(Bytes bytes, Integer v) { bytes.writeInt(v); }
        @Nullable @Override public Integer read(Bytes bytes) { return bytes.readInt(); }
        @Nullable @Override
        public Integer read(Bytes bytes, @Nullable Integer v) { return bytes.readInt(); }
    }

    private static enum LongMarshaller implements BytesMarshaller<Long> {
        INSTANCE;
        @Override public void write(Bytes bytes, Long v) { bytes.writeLong(v); }
        @Nullable @Override public Long read(Bytes bytes) { return bytes.readLong(); }
        @Nullable @Override
        public Long read(Bytes bytes, @Nullable Long v) { return bytes.readLong(); }
    }

    private static enum DoubleMarshaller implements BytesMarshaller<Double> {
        INSTANCE;
        @Override public void write(Bytes bytes, Double v) { bytes.writeDouble(v); }
        @Nullable @Override public Double read(Bytes bytes) { return bytes.readDouble(); }
        @Nullable @Override
        public Double read(Bytes bytes, @Nullable Double v) { return bytes.readDouble(); }
    }

    private static enum SerializableMarshaller implements BytesMarshaller {
        INSTANCE;
        @Override
        public void write(Bytes bytes, Object obj) {
            try {
                new ObjectOutputStream(bytes.outputStream()).writeObject(obj);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Nullable
        @Override
        public Object read(Bytes bytes) {
            try {
                return new ObjectInputStream(bytes.inputStream()).readObject();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }

        @Nullable
        @Override
        public Object read(Bytes bytes, @Nullable Object obj) {
            return read(bytes);
        }
    }

    final SharedHashMapBuilder builder;
    final Class<K> keyClass;
    final Class<V> valueClass;

    @NotNull private BytesMarshaller<K> keyMarshaller;
    @NotNull private BytesMarshaller<V> valueMarshaller;
    @NotNull private ObjectFactory<V> valueFactory;

    private SharedMapEventListener<K, V, SharedHashMap<K, V>> eventListener =
            SharedMapEventListeners.nop();

    SharedHashMapKeyValueSpecificBuilder(SharedHashMapBuilder builder,
                                         Class<K> keyClass, Class<V> valueClass) {
        this.builder = builder;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        Class<K> keyClassForMarshaller =
                marshallerUseFactory(keyClass) && keyClass.isInterface() ?
                        DataValueClasses.directClassFor(keyClass) : keyClass;
        keyMarshaller = chooseDefaultMarshaller(keyClassForMarshaller);
        Class<V> valueClassForMarshaller =
                marshallerUseFactory(valueClass) && valueClass.isInterface() ?
                        DataValueClasses.directClassFor(valueClass) : valueClass;
        valueMarshaller = chooseDefaultMarshaller(valueClassForMarshaller);

        valueFactory = marshallerUseFactory(valueClass) ?
                new AllocateInstanceObjectFactory(valueClass.isInterface() ?
                        DataValueClasses.directClassFor(valueClass) :
                        valueClass) :
                NullObjectFactory.INSTANCE;
    }

    @NotNull
    public BytesMarshaller<K> keyMarshaller() {
        return keyMarshaller;
    }

    public SharedHashMapKeyValueSpecificBuilder<K, V> keyMarshaller(
            @NotNull BytesMarshaller<K> keyMarshaller) {
        this.keyMarshaller = keyMarshaller;
        return this;
    }

    @NotNull
    public BytesMarshaller<V> valueMarshaller() {
        return valueMarshaller;
    }

    public SharedHashMapKeyValueSpecificBuilder<K, V> valueMarshallerAndFactory(
            @NotNull BytesMarshaller<V> valueMarshaller, @NotNull ObjectFactory<V> valueFactory) {
        this.valueMarshaller = valueMarshaller;
        this.valueFactory = valueFactory;
        return this;
    }

    @NotNull
    public ObjectFactory<V> valueFactory() {
        return valueFactory;
    }

    @SuppressWarnings("unchecked")
    public SharedHashMapKeyValueSpecificBuilder<K, V> valueFactory(
            @NotNull ObjectFactory<V> valueFactory) {
        if (!marshallerUseFactory(valueClass)) {
            throw new IllegalStateException("Default marshaller for " + valueClass +
                    " value don't use object factory");
        }
        else if (valueMarshaller instanceof ByteableMarshaller) {
            if (valueFactory instanceof AllocateInstanceObjectFactory) {
                valueMarshaller = new ByteableMarshaller(
                        ((AllocateInstanceObjectFactory) valueFactory).allocatedClass());
            } else {
                valueMarshaller = new ByteableMarshallerWithCustomFactory(
                        ((ByteableMarshaller) valueMarshaller).tClass, valueFactory);
            }
        }
        else if (valueMarshaller instanceof BytesMarshallableMarshaller) {
            if (valueFactory instanceof AllocateInstanceObjectFactory) {
                valueMarshaller = new BytesMarshallableMarshaller(
                        ((AllocateInstanceObjectFactory) valueFactory).allocatedClass());
            } else {
                valueMarshaller = new BytesMarshallableMarshallerWithCustomFactory(
                        ((BytesMarshallableMarshaller) valueMarshaller).marshaledClass(),
                        valueFactory
                );
            }
        }
        else if (valueMarshaller instanceof ExternalizableMarshaller) {
            if (valueFactory instanceof AllocateInstanceObjectFactory) {
                valueMarshaller = new ExternalizableMarshaller(
                        ((AllocateInstanceObjectFactory) valueFactory).allocatedClass());
            } else {
                valueMarshaller = new ExternalizableMarshallerWithCustomFactory(
                        ((ExternalizableMarshaller) valueMarshaller).marshaledClass(),
                        valueFactory
                );
            }
        }
        else {
            // valueMarshaller is custom, it is user's responsibility to use the same factory inside
            // marshaller and standalone
            throw new IllegalStateException(
                    "Change the value factory simultaneously with marshaller " +
                            "using valueMarshallerAndFactory() method");
        }
        this.valueFactory = valueFactory;
        return this;
    }

    public SharedHashMapKeyValueSpecificBuilder<K, V> eventListener(
            SharedMapEventListener<K, V, SharedHashMap<K, V>> eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    public SharedMapEventListener<K, V, SharedHashMap<K, V>> eventListener() {
        return eventListener;
    }


    public SharedHashMap<K, V> create(File file) throws IOException {
        for (int i = 0; i < 10; i++) {
            if (file.exists() && file.length() > 0) {
                FileInputStream fis = new FileInputStream(file);
                ObjectInputStream ois = new ObjectInputStream(fis);
                try {
                    VanillaSharedHashMap<K, V> map = (VanillaSharedHashMap<K, V>) ois.readObject();
                    map.headerSize = roundUpMapHeaderSize(fis.getChannel().position());
                    map.createMappedStoreAndSegments(file);
                    return map;
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                } finally {
                    ois.close();
                }
            }
            if (file.createNewFile() || file.length() == 0) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        // new file
        if (!file.exists())
            throw new FileNotFoundException("Unable to create " + file);

        VanillaSharedHashMap<K, V> map = newMap();

        FileOutputStream fos = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        try {
            oos.writeObject(map);
            oos.flush();
            map.headerSize = roundUpMapHeaderSize(fos.getChannel().position());
            map.createMappedStoreAndSegments(file);

        } finally {
            oos.close();
        }

        ObjectSerializer objectSerializer = builder.objectSerializer();
        BytesStore bytesStore = new MappedStore(file, FileChannel.MapMode.READ_WRITE,
                map.sizeInBytes(), objectSerializer);
        map.createMappedStoreAndSegments(bytesStore);

        return configureMap(map);
    }

    public SharedHashMap<K, V> create() throws IOException {
        VanillaSharedHashMap<K, V> map = newMap();
        ObjectSerializer objectSerializer = builder.objectSerializer();
        BytesStore bytesStore = DirectStore.allocateLazy(map.sizeInBytes(), objectSerializer);
        map.createMappedStoreAndSegments(bytesStore);
        return configureMap(map);
    }

    private VanillaSharedHashMap<K, V> newMap() throws IOException {
        if (builder.firstReplicator == null && !builder.forceReplicatedImpl) {
            return new VanillaSharedHashMap<K, V>(this);
        } else {
            return new VanillaSharedReplicatedHashMap<K, V>(this);
        }
    }

    private VanillaSharedHashMap<K, V> configureMap(VanillaSharedHashMap<K, V> map)
            throws IOException {
        if (map instanceof VanillaSharedReplicatedHashMap) {
            VanillaSharedReplicatedHashMap result = (VanillaSharedReplicatedHashMap) map;
            for (Replicator replicator : builder.replicators.values()) {
                Closeable token = replicator.applyTo(builder, result, result);
                if (builder.replicators.size() == 1 && token.getClass() == UdpReplicator.class) {
                    LOG.warn(
                            "MISSING TCP REPLICATION : The UdpReplicator only attempts to read data " +
                            "(it does not enforce or guarantee delivery), you should use" +
                            "the UdpReplicator if you have a large number of nodes, and you wish" +
                            "to receive the data before it becomes available on TCP/IP. Since data" +
                            "delivery is not guaranteed, it is recommended that you only use" +
                            "the UDP Replicator in conjunction with a TCP Replicator"
                    );
                }
                result.addCloseable(token);
            }
        }
        return map;
    }


    private static long roundUpMapHeaderSize(long headerSize) {
        long roundUp = (headerSize + 127L) & ~127L;
        if (roundUp - headerSize < 64)
            roundUp += 128;
        return roundUp;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SharedHashMapKeyValueSpecificBuilder<K, V> clone() {
        try {
            return (SharedHashMapKeyValueSpecificBuilder<K, V>) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return builderEquals(this, o);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return "SharedHashMapKeyValueSpecificBuilder{" +
                "builder=" + builder +
                ", keyClass=" + keyClass +
                ", valueClass=" + valueClass +
                ", keyMarshaller=" + keyMarshaller +
                ", valueMarshaller=" + valueMarshaller +
                ", valueFactory=" + valueFactory +
                ", eventListener=" + eventListener +
                '}';
    }
}
