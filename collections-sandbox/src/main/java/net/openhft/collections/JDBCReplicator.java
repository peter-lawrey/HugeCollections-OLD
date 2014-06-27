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

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.constraints.NotNull;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @author Rob Austin.
 */
public class JDBCReplicator<K, V, M extends SharedHashMap<K, V>> extends SharedMapEventListener<K, V, M> {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");

    private final Fields<V> fields;
    private final Class<V> vClass;
    private Statement stmt;
    private final String table;

    public JDBCReplicator(@NotNull final Class<V> vClass,
                          @NotNull final Fields<V> fields,
                          @NotNull final Statement stmt,
                          @NotNull final String table) {
        this.fields = fields;
        this.stmt = stmt;
        this.table = table;
        this.vClass = vClass;
    }

    public JDBCReplicator(@NotNull final Class<V> vClass,
                          @NotNull final Statement stmt,
                          @NotNull final String tableName) {
        fields = createAnnotationBasedFieldMapper(vClass);
        this.stmt = stmt;
        this.table = tableName;
        this.vClass = vClass;
    }

    /**
     * Annotations
     *
     * @param vClass
     * @return
     */
    private Fields createAnnotationBasedFieldMapper(final Class<V> vClass) {

        String keyFieldName0 = null;

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
                    }

                    if (annotation.annotationType().equals(Column.class)) {

                        final String fieldName = ((Column) annotation).name();
                        final String columnName = fieldName.isEmpty() ? f.getName() : fieldName;

                        columnsNamesByField.put(f, columnName);

                    }

                } catch (Exception exception) {
                    LOG.error("", exception);
                }


            }

        }

        final CharSequence keyFieldName = keyFieldName0;

        return new Fields<V>() {

            @Override
            public CharSequence keyName() {
                if (keyFieldName == null) {
                    throw new IllegalStateException("@DBKey is not set on any of the fields in " + vClass.getName());
                }
                return keyFieldName;
            }

            @Override
            public Map<java.lang.reflect.Field, String> columnsNamesByField() {
                return Collections.unmodifiableMap(columnsNamesByField);
            }

            @Override
            public Set<Field> getFields(V value, boolean skipKey) {

                final Set<Field> result = new HashSet<Field>();

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
                            result.add(new Field(columnName, field.get(value).toString()));
                            continue;
                        }

                        Object v = null;

                        if (CharSequence.class.isAssignableFrom(type) || field.getType().equals(java.sql.Date.class))
                            v = field.get(value);

                        else if (field.getType().equals(Date.class))
                            v = new java.sql.Date(((Date) field.get(value)).getTime());

                        else if (field.getType().equals(DateTime.class))
                            v = dateTimeFormatter.print((DateTime) field.get(value));

                        if (v == null)
                            v = field.get(value);

                        if (v == null) {
                            if (LOG.isDebugEnabled())
                                LOG.debug("unable to map field=" + field);
                            continue;
                        }

                        result.add(new Field(columnName, new StringBuilder("'").append(v.toString()).append("'")));

                    } catch (Exception e) {
                        LOG.error("", e);
                    }

                }

                return result;
            }
        };


    }


    void onPut(M map, Bytes entry, int metaDataBytes, boolean added, K key, V value,
               long pos, SharedSegment segment) {
        try {
            if (added)
                onInsert(key, value);
            else
                onUpdate(key, value);

        } catch (SQLException e) {
            LOG.error("", e);
        }
    }

    /**
     * This method is called if a key/value is put in the map.
     *
     * @param map           accessed
     * @param entry         added/modified
     * @param metaDataBytes length of the meta data
     * @param added         if this is a new entry
     * @param key           looked up
     * @param value         set for key
     */
    public void onPut(M map, Bytes entry, int metaDataBytes, boolean added, K key, V value) {
        // do nothing
    }

    void onRemove(M map, Bytes entry, int metaDataBytes, K key, V value,
                  int pos, SharedSegment segment) {
        //  onRemove(map, entry, metaDataBytes, key, value);
    }

    /**
     * This is called when an entry is removed. Misses are not notified.
     *
     * @param map           accessed
     * @param entry         removed
     * @param metaDataBytes length of meta data
     * @param key           removed
     * @param value         removed
     */
    public void onRemove(M map, Bytes entry, int metaDataBytes, K key, V value) {
        // do nothing
    }


    /**
     * since we are on the only system read and writing this data to the database, we will know, if the record
     * has already been inserted or updated so can call the appropriate sql
     */
    public void onInsert(K key, V value) throws SQLException {
        try {

            final StringBuilder values = new StringBuilder();
            final StringBuilder fields = new StringBuilder();

            for (final Fields.Field field : this.fields.getFields(value, true)) {
                values.append(field.value).append(",");
                fields.append(field.name).append(",");
            }

            fields.deleteCharAt(fields.length() - 1);
            values.deleteCharAt(values.length() - 1);

            final StringBuilder sql = new StringBuilder("INSERT INTO ")
                    .append(table).
                            append(" (").
                            append(this.fields.keyName()).
                            append(",").
                            append(fields).
                            append(") VALUES (").
                            append(key).append(",").
                            append(values).
                            append(")");

            stmt.execute(sql.toString());

        } catch (SQLException e) {
            // 23505 is the error code for duplicate
            if ("23505".equals(e.getSQLState()) || "duplicate".equalsIgnoreCase(e.getMessage())) {
                onUpdate(key, value);
            }

        }
    }


    /**
     * since we are on the only system read and writing this data to the database, we will know, if the record
     * has already been inserted or updated so can call the appropriate sql
     */
    public void onUpdate(K key, V value) throws SQLException {

        final StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(this.table).append(" SET ");

        final Set<Fields.Field> fields1 = fields.getFields(value, true);

        if (fields1.isEmpty())
            return;

        for (final Fields.Field field : fields1) {
            sql.append(field.name).append("=");
            sql.append(field.value).append(",");
        }

        sql.deleteCharAt(sql.length() - 1);

        sql.append(" WHERE ").append(fields.keyName()).append("=").append(key);
        final int rowCount = stmt.executeUpdate(sql.toString());

        if (rowCount == 0) {
            onInsert(key, value);
        }


    }


    public interface Fields<V> {

        CharSequence keyName();

        /**
         * @return all the field names including the name of the key and there assassinated type
         */
        Map<java.lang.reflect.Field, String> columnsNamesByField();


        static class Field {

            CharSequence name;
            CharSequence value;

            public Field(CharSequence name, CharSequence value) {
                this.name = name;
                this.value = value;
            }
        }

        Set<Field> getFields(V value, boolean skipKey);

    }

    /**
     * gets all the records from the database as a Map<K,V>
     *
     * @return
     * @throws SQLException
     * @throws InstantiationException
     */
    public V get(K key) throws SQLException, InstantiationException, IllegalAccessException {
        final String sql = "SELECT * FROM " + this.table + " WHERE " + fields.keyName() + "=" + key.toString();
        final ResultSet resultSet = stmt.executeQuery(sql);
        return toResult(resultSet, vClass);
    }


    /**
     * gets all the records from the database for a list of keys
     *
     * @return
     * @throws SQLException
     * @throws InstantiationException
     */
    public Set<V> get(K... key) throws SQLException, InstantiationException, IllegalAccessException {

        final StringBuilder keys = new StringBuilder(Arrays.toString(key));

        // remove the {}
        keys.deleteCharAt(keys.length() - 1);
        keys.deleteCharAt(0);

        final String sql = "SELECT * FROM " + this.table + " WHERE " + fields.keyName() +
                " in (" + keys.toString() + ")";

        final ResultSet resultSet = stmt.executeQuery(sql);
        return toResult(resultSet, HashSet.class);
    }


    /**
     * gets all the records from the database as a Map<K,V>
     *
     * @return
     * @throws SQLException
     * @throws InstantiationException
     */
    public Map<K, V> getAll() throws SQLException, InstantiationException, IllegalAccessException {
        final String sql = "SELECT * FROM " + this.table;
        return toResult(stmt.executeQuery(sql), HashMap.class);
    }

    /**
     * converts from a result set to Map
     *
     * @param resultSet
     * @return
     * @throws SQLException
     * @throws InstantiationException
     */
    private <R> R toResult(ResultSet resultSet, Class<R> rClass) throws SQLException,
            InstantiationException, IllegalAccessException {

        R result = null;

        if (Collection.class.isAssignableFrom(rClass) || Map.class.isAssignableFrom(rClass))
            result = rClass.newInstance();


        final CharSequence keyName = fields.keyName();
        while (resultSet.next()) {

            K key = null;

            final V o = (V) NativeBytes.UNSAFE.allocateInstance(this.vClass);

            for (Map.Entry<Field, String> fieldNameByType : fields.columnsNamesByField().entrySet()) {


                final Field field = fieldNameByType.getKey();
                try {

                    if (field.getType().equals(boolean.class))
                        field.setBoolean(o, resultSet.getBoolean(fieldNameByType.getValue()));

                    else if (field.getType().equals(byte.class))
                        field.setByte(o, resultSet.getByte(fieldNameByType.getValue()));

                    if (field.getType().equals(char.class)) {
                        final String string = resultSet.getString(fieldNameByType.getValue());
                        if (string.length() > 0)
                            field.setChar(o, string.charAt(0));
                    } else if (field.getType().equals(short.class))
                        field.setShort(o, resultSet.getShort(fieldNameByType.getValue()));

                    else if (field.getType().equals(Float.class))
                        field.setFloat(o, resultSet.getFloat(fieldNameByType.getValue()));

                    else if (field.getType().equals(int.class))
                        field.setInt(o, resultSet.getInt(fieldNameByType.getValue()));

                    else if (field.getType().equals(Long.class))
                        field.setLong(o, resultSet.getLong(fieldNameByType.getValue()));

                    else if (field.getType().equals(double.class))
                        field.setDouble(o, resultSet.getDouble(fieldNameByType.getValue()));

                    else if (field.getType().equals(double.class))
                        field.setDouble(o, resultSet.getBigDecimal(fieldNameByType.getValue()).doubleValue());

                    else if (field.getType().equals(BigDecimal.class))
                        field.set(o, resultSet.getBigDecimal(fieldNameByType.getValue()));

                    else if (field.getType().equals(String.class))
                        field.set(o, resultSet.getString(fieldNameByType.getValue()));

                    else if (field.getType().equals(Date.class))
                        field.set(o, new Date(resultSet.getDate(fieldNameByType.getValue()).getTime()));

                    else
                        LOG.error("unsupported type " + field);

                    if (Map.class.isAssignableFrom(rClass) && fieldNameByType.getValue().equals(keyName))
                        key = (K) field.get(o);

                } catch (Exception e) {
                    LOG.error("", e);
                }

            }

            if (rClass.isAssignableFrom(vClass))
                return (R) o;

            if (Collection.class.isAssignableFrom(rClass))
                ((Collection) result).add(o);

            else if (Map.class.isAssignableFrom(rClass)) {

                if (key == null) {
                    LOG.debug("skipping value=" + o + " as no key was found.");
                    continue;
                }

                ((Map) result).put(key, o);

            } else
                throw new IllegalStateException("Unexpected class type class=" + vClass);

        }
        return result;
    }
}





