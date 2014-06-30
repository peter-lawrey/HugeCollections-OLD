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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;

/**
 * @author Rob Austin.
 */
public interface FieldMapper<V> {

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
     * getExternal all fields what re mapping to a database table based on annotations
     *
     * @param value   a list of all the fields that have the
     * @param skipKey if set to true include the @Key field
     * @return
     */
    Set<ValueWithFieldName> getFields(V value, boolean skipKey);

    class ReflectionBasedFieldMapperBuilder<V> {


        static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());


        boolean wrapTextAndDateFieldsInQuotes = false;

        public boolean wrapTextAndDateFieldsInQuotes() {
            return wrapTextAndDateFieldsInQuotes;
        }

        public ReflectionBasedFieldMapperBuilder wrapTextAndDateFieldsInQuotes(boolean wrapTextAndDateFieldsInQuotes) {
            this.wrapTextAndDateFieldsInQuotes = wrapTextAndDateFieldsInQuotes;
            return this;
        }


        /**
         * @return an annotation based Object Relational Mapper (ORM) - Uses reflection and the annotations
         * provided in the value object ( of type <V> ) to define the relationship between the database table
         * and the value object
         */
        public <V> FieldMapper create(final Class<V> vClass, final DateTimeFormatter dateTimeFormatter) {


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
                        throw new IllegalStateException("@DBKey is not set on any of the fields in " + vClass.getName());
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
                public Map<java.lang.reflect.Field, String> columnsNamesByField() {
                    return Collections.unmodifiableMap(columnsNamesByField);
                }

                @Override
                public Set<ValueWithFieldName> getFields(V value, boolean skipKey) {

                    final Set<ValueWithFieldName> result = new HashSet<ValueWithFieldName>();

                    for (Map.Entry<java.lang.reflect.Field, String> entry : columnsNamesByField.entrySet()) {

                        final CharSequence columnName = entry.getValue();

                        if (skipKey && columnName.equals(keyFieldName))
                            continue;

                        final java.lang.reflect.Field field = entry.getKey();
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

}

