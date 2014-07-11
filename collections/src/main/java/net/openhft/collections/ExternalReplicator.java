/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.annotation.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static net.openhft.collections.Replica.EntryResolver;

/**
 * Used for both file and and database replication
 *
 * @author Rob Austin.
 */
public interface ExternalReplicator<K, V> extends Replica.EntryCallback,
        Replica.ModificationNotifier, Closeable {

    /**
     * gets the data <V> from an external source
     *
     * @param k the key of the entry
     * @return the value of the entry
     */
    V getExternal(K k);

    /**
     * reads all the entries from the external source and writes into the {@code usingMap}. This method will
     * alter the contents of {@code usingMap}
     *
     * @param usingMap the map to which the data will be written
     * @return the {@code usingMap}
     */
    Map<K, V> getAllExternal(@NotNull final Map<K, V> usingMap);

    /**
     * @return the timezone of the external source, for example database
     */
    DateTimeZone getZone();

    /**
     * an base implementation of an ExternalReplicator
     *
     * @param <K>
     * @param <V>
     */
    abstract class AbstractExternalReplicator<K, V>
            extends Replica.AbstractEntryCallback
            implements ExternalReplicator<K, V> {

        private final V usingValue;
        private final K usingKey;
        private final EntryResolver<K, V> entryResolver;
        private Object changeNotifier = new Object();


        private static final Logger LOG = LoggerFactory.getLogger(AbstractExternalReplicator.class);

        protected AbstractExternalReplicator(final Class<K> kClass,
                                             final Class<V> vClass,
                                             final EntryResolver<K, V> entryResolver)
                throws InstantiationException {

            usingValue = (V) NativeBytes.UNSAFE.allocateInstance(vClass);
            usingKey = (K) NativeBytes.UNSAFE.allocateInstance(kClass);
            this.entryResolver = entryResolver;
        }

        @Override
        public boolean onEntry(AbstractBytes entry, final int chronicleId) {

            final K key = entryResolver.key(entry, usingKey);

            if (entryResolver.wasRemoved(entry))
                removeExternal(key);

            final V value = entryResolver.value(entry, usingValue);
            if (value == null)
                return false;

            putExternal(key, value, false);
            return true;
        }

        public void setModificationIterator(final Replica.ModificationIterator modIterator) {
            final ExecutorService executorService = Executors.newSingleThreadExecutor();

            executorService.execute(new Runnable() {
                @Override
                public void run() {

                    for (; ; ) {

                        // this will update the external replication source with the new values added to the
                        // map
                        // ideally this will be run on its own thread
                        while (modIterator.hasNext()) {
                            modIterator.nextEntry(AbstractExternalReplicator.this, 0);
                        }

                        // waits here unit there are more events to process
                        synchronized (changeNotifier) {
                            if (modIterator.hasNext())
                                continue;

                            try {
                                changeNotifier.wait();
                            } catch (InterruptedException e) {
                                LOG.error("", e);
                            }
                        }
                    }
                }
            });
        }

        /**
         * called when the modification iterator events to process
         */
        public final void onChange() {
            synchronized (changeNotifier) {
                changeNotifier.notifyAll();
            }
        }

        /**
         * write the entry to an external source, be it a database or file, this is called indirectly when
         * ever data is put() on a shared hash map
         *
         * @param key   the key of the entry
         * @param value the value of the entry
         * @param added true if the entry has just been added to the map rather than updated
         */
        abstract void putExternal(K key, V value, boolean added);

        /**
         * removes a single entry from the external source which relates to key {@code k},
         */
        abstract void removeExternal(K k);


        /**
         * removes form the external source, all the entries who's keys are in {@code keys}
         */
        abstract void removeAllExternal(final Set<K> keys);

    }


    /**
     * @author Rob Austin.
     */
    interface FieldMapper<V> {

        /**
         * The Field and it's associated value
         */
        static class ValueWithFieldName {

            CharSequence name;
            CharSequence value;

            public ValueWithFieldName(CharSequence name, CharSequence value) {
                this.name = name;
                this.value = value;
            }
        }


        /**
         * @return the database row name of the field which is annotated with @Key
         */
        CharSequence keyName();

        /**
         * @return the database row name of the field which is annotated with @Key
         */
        Field keyField();

        /**
         * @return all the field names including the name of the key and their assassinated java type
         */
        Map<Field, String> columnsNamesByField();


        /**
         * getExternal all fields what re mapping to a database tableName based on annotations
         *
         * @param value   a list of all the fields that have the
         * @param skipKey if set to true include the @Key field
         * @return
         */
        Set<ValueWithFieldName> getFields(V value, boolean skipKey);

        class ReflectionBasedFieldMapperBuilder<V> {


            static final Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());


            boolean wrapTextAndDateFieldsInQuotes = false;

            public boolean wrapTextAndDateFieldsInQuotes() {
                return wrapTextAndDateFieldsInQuotes;
            }

            public ReflectionBasedFieldMapperBuilder wrapTextAndDateFieldsInQuotes(boolean wrapTextAndDateFieldsInQuotes) {
                this.wrapTextAndDateFieldsInQuotes = wrapTextAndDateFieldsInQuotes;
                return this;
            }


            /**
             * @return an annotation based Object Relational Mapper (ORM) - Uses reflection and the
             * annotations provided in the value object ( of type <V> ) to define the relationship between the
             * database tableName and the value object
             */
            public <V> FieldMapper<V> create(final Class<V> vClass, final DateTimeFormatter dateTimeFormatter) {

                String keyFieldName0 = null;
                Field keyField0 = null;
                final Map<Field, String> columnsNamesByField = new HashMap<Field, String>();

                for (final Field f : vClass.getDeclaredFields()) {

                    f.setAccessible(true);

                    for (final Annotation annotation : f.getAnnotations()) {

                        try {

                            if (annotation.annotationType().equals(Key.class)) {
                                if (keyFieldName0 != null)
                                    throw new IllegalArgumentException("@Key is already set : Only one field can be " +
                                            "annotated with @Key, " +
                                            "The field '" + keyFieldName0 + "' already has this annotation, so '" +
                                            f.getName() + "' can not be set well.");

                                keyFieldName0 = f.getName();

                                final String fieldName = ((Key) annotation).name();
                                keyFieldName0 = fieldName.isEmpty() ? f.getName() : fieldName;
                                columnsNamesByField.put(f, keyFieldName0);
                                keyField0 = f;
                            }

                            if (annotation.annotationType().equals(Column.class)) {

                                final String fieldName = ((Column) annotation).name();
                                final String columnName = fieldName.isEmpty() ?
                                        toUpperCase(f.getName()) :
                                        fieldName;

                                columnsNamesByField.put(f, columnName);

                            }

                        } catch (Exception exception) {
                            LOG.error("", exception);
                        }

                    }

                }


                final Field keyField = keyField0;
                final CharSequence keyFieldName = keyFieldName0;

                return new FieldMapper<V>() {

                    @Override
                    public CharSequence keyName() {
                        if (keyFieldName == null) {
                            throw new IllegalStateException("@Key is not set on any of the fields in " + vClass.getName());
                        }
                        return keyFieldName;
                    }

                    @Override
                    public Field keyField() {
                        if (keyField == null)
                            throw new IllegalStateException("Field named=" + keyFieldName + " can not be found.");
                        return keyField;
                    }

                    @Override
                    public Map<Field, String> columnsNamesByField() {
                        return Collections.unmodifiableMap(columnsNamesByField);
                    }

                    @Override
                    public Set<ValueWithFieldName> getFields(V value, boolean skipKey) {

                        final Set<ValueWithFieldName> result = new HashSet<ValueWithFieldName>();

                        for (Map.Entry<Field, String> entry : columnsNamesByField.entrySet()) {

                            final CharSequence columnName = entry.getValue();

                            if (skipKey && columnName.equals(keyFieldName))
                                continue;

                            final Field field = entry.getKey();
                            try {

                                final Class<?> type = field.getType();

                                if (type == null)
                                    continue;

                                if (Number.class.isAssignableFrom(type) ||
                                        (field.getType().isPrimitive() && field.getType() != char.class)) {
                                    result.add(new ValueWithFieldName(columnName, field.get(value).toString()));
                                    continue;
                                }

                                Object v = null;

                                if (CharSequence.class.isAssignableFrom(type) || field.getType().equals(java.sql.Date.class))
                                    v = field.get(value);

                                else if (field.getType().equals(Date.class)) {
                                    final long time = ((Date) field.get(value)).getTime();
                                    final DateTime instant = new DateTime(time);

                                    v = dateTimeFormatter.print(instant);
                                } else if (field.getType().equals(DateTime.class))
                                    v = dateTimeFormatter.print(((DateTime) field.get(value)));

                                if (v == null)
                                    v = field.get(value);

                                if (v == null) {
                                    if (LOG.isDebugEnabled())
                                        LOG.debug("unable to map field=" + field);
                                    continue;
                                }

                                result.add(!wrapTextAndDateFieldsInQuotes ?
                                        new ValueWithFieldName(columnName, v.toString()) :
                                        new ValueWithFieldName(columnName,
                                                new StringBuilder("'").append(v.toString()).append("'")));

                            } catch (Exception e) {
                                LOG.error("", e);
                            }

                        }

                        return result;
                    }
                };

            }

            /**
             * converts from camelCase to UPPER_CASE  ( like this )
             *
             * @param fromCamelCase from this format, camelCase
             * @return to this format, UPPER_CASE
             */
            private String toUpperCase(String fromCamelCase) {
                return fromCamelCase.replaceAll("(.)(\\p{Upper})", "$1_$2").toUpperCase();
            }

        }


        @Retention(RetentionPolicy.RUNTIME) // Make this annotation accessible at runtime via reflection.
        @Target({ElementType.FIELD})
                // This annotation can only be applied to class methods.
        @interface Key {
            String name() default "";
        }

        @Retention(RetentionPolicy.RUNTIME) // Make this annotation accessible at runtime via reflection.
        @Target({ElementType.FIELD})
                // This annotation can only be applied to class methods.
        @interface Column {
            String name() default "";
        }
    }
}


