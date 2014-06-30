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
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static net.openhft.collections.ExternalReplicator.AbstractExternalReplicator;

/**
 * @author Rob Austin.
 */
public class JDBCReplicator<K, V, M extends SharedHashMap<K, V>>
        extends AbstractExternalReplicator<K, V, M> {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(JDBCReplicator.class.getName());

    private final FieldMapper<V> fieldMapper;
    private final Class<V> vClass;
    private Statement stmt;
    private final String table;


    public JDBCReplicator(@NotNull final Class<V> vClass,
                          @NotNull final Statement stmt,
                          @NotNull final String tableName) {
        this.stmt = stmt;
        this.table = tableName;
        this.vClass = vClass;

        final FieldMapper.ReflectionBasedFieldMapperBuilder builder = new FieldMapper.ReflectionBasedFieldMapperBuilder();
        builder.wrapTextAndDateFieldsInQuotes(true);

        fieldMapper = builder.create(vClass);
    }

    /**
     * @param vClass      the class of <V>
     * @param stmt
     * @param table
     * @param fieldMapper used to identifier the fields when serializing to the database
     */
    public JDBCReplicator(@NotNull final Class<V> vClass,
                          @NotNull final Statement stmt,
                          @NotNull final String table,
                          @NotNull final FieldMapper<V> fieldMapper) {
        this.fieldMapper = fieldMapper;
        this.stmt = stmt;
        this.table = table;
        this.vClass = vClass;
    }


    void onPut(M map, Bytes entry, int metaDataBytes, boolean added, K key, V value,
               long pos, SharedSegment segment) {
        put(key, value, added);
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
        put(key, value, added);
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
    private void insert(K key, V value) throws SQLException {
        try {

            final StringBuilder values = new StringBuilder();
            final StringBuilder fields = new StringBuilder();

            for (final FieldMapper.ValueWithFieldName valueWithFieldName : this.fieldMapper.getFields(value, true)) {
                values.append(valueWithFieldName.value).append(",");
                fields.append(valueWithFieldName.name).append(",");
            }

            fields.deleteCharAt(fields.length() - 1);
            values.deleteCharAt(values.length() - 1);

            final StringBuilder sql = new StringBuilder("INSERT INTO ")
                    .append(table).
                            append(" (").
                            append(this.fieldMapper.keyName()).
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
                update(key, value);
            } else
                LOG.error("key=" + key + ", value=" + value, e);

        }
    }


    /**
     * since we are on the only system read and writing this data to the database, we will know, if the record
     * has already been inserted or updated so can call the appropriate sql
     */
    private void update(K key, V value) throws SQLException {

        final StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(this.table).append(" SET ");

        final Set<FieldMapper.ValueWithFieldName> fields = fieldMapper.getFields(value, true);

        if (fields.isEmpty())
            return;

        for (final FieldMapper.ValueWithFieldName valueWithFieldName : fields) {
            sql.append(valueWithFieldName.name).append("=");
            sql.append(valueWithFieldName.value).append(",");
        }

        sql.deleteCharAt(sql.length() - 1);

        sql.append(" WHERE ").append(fieldMapper.keyName()).append("=").append(key);
        final int rowCount = stmt.executeUpdate(sql.toString());

        if (rowCount == 0) {
            insert(key, value);
        }

    }


    @Override
    public void put(K key, V value, boolean added) {
        try {
            if (added)
                insert(key, value);
            else
                update(key, value);

        } catch (SQLException e) {
            LOG.error("", e);
        }
    }

    /**
     * gets all the records from the database as a Map<K,V>
     *
     * @return
     * @throws SQLException
     * @throws InstantiationException
     */
    public V get(K key) {
        try {
            final String sql = "SELECT * FROM " + this.table + " WHERE " + fieldMapper.keyName() + "=" + key.toString();
            final ResultSet resultSet = stmt.executeQuery(sql);
            return toResult(resultSet, vClass);
        } catch (Exception e) {
            LOG.error("", e);
            return null;
        }
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

        final String sql = "SELECT * FROM " + this.table + " WHERE " + fieldMapper.keyName() +
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


        final CharSequence keyName = fieldMapper.keyName();
        while (resultSet.next()) {

            K key = null;

            final V o = (V) NativeBytes.UNSAFE.allocateInstance(this.vClass);

            for (Map.Entry<Field, String> fieldNameByType : fieldMapper.columnsNamesByField().entrySet()) {


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

                    else if (field.getType().equals(DateTime.class)) {
                        String date = resultSet.getString(fieldNameByType.getValue());
                        final DateTime dateTime = dateTimeFormatter.parseDateTime(date).toDateTime();

                        field.set(o, dateTime);
                    } else if (field.getType().equals(Date.class)) {
                        String date = resultSet.getString(fieldNameByType.getValue());
                        final DateTime dateTime = dateTimeFormatter.parseDateTime(date);

                        field.set(o, dateTime.toDate());
                    } else
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





