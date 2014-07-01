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

import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static net.openhft.collections.ExternalReplicator.AbstractExternalReplicator;
import static net.openhft.collections.FieldMapper.ReflectionBasedFieldMapperBuilder;
import static net.openhft.collections.ReplicatedSharedHashMap.EntryResolver;

/**
 * @author Rob Austin.
 */
public class JDBCReplicator<K, V> extends
        AbstractExternalReplicator<K, V> {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(JDBCReplicator.class.getName());
    public static final String YYYY_MM_DD = "YYYY-MM-dd";

    private final FieldMapper<V> fieldMapper;
    private final Class<V> vClass;
    private final DateTimeFormatter dateTimeFormatter;
    private final DateTimeFormatter shortDateTimeFormatter;

    private final Statement stmt;
    private final String table;
    private final DateTimeZone dateTimeZone;

    /**
     * @param kClass        the type of key to persist
     * @param vClass        the type of value to persist
     * @param stmt          the database statement
     * @param tableName     the name of the table that we are writing to
     * @param dateTimeZone  the timezone of the database we are replicating into
     * @param entryResolver
     */
    public JDBCReplicator(@NotNull final Class<K> kClass,
                          @NotNull final Class<V> vClass,
                          @NotNull final Statement stmt,
                          @NotNull final String tableName,
                          @NotNull final DateTimeZone dateTimeZone,
                          @NotNull final EntryResolver<K, V> entryResolver)
            throws InstantiationException {
        super(kClass, vClass, entryResolver);
        this.stmt = stmt;
        this.table = tableName;
        this.vClass = vClass;
        this.dateTimeZone = dateTimeZone;

        this.dateTimeFormatter = DEFAULT_DATE_TIME_FORMATTER.withZone(dateTimeZone);
        shortDateTimeFormatter = DateTimeFormat.forPattern(YYYY_MM_DD).withZone(dateTimeFormatter.getZone());

        final ReflectionBasedFieldMapperBuilder builder = new ReflectionBasedFieldMapperBuilder();
        builder.wrapTextAndDateFieldsInQuotes(true);

        fieldMapper = builder.create(vClass, dateTimeFormatter);
    }


    /**
     * @param map           the map which the data will be written to
     * @param kClass
     * @param vClass        the type of class to persist
     * @param stmt          the database statement
     * @param fieldMapper   used to identifier the fields when serializing to the database
     * @param entryResolver
     */
    public JDBCReplicator(@NotNull final Map<K, V> map,
                          @NotNull final Class<K> kClass,
                          @NotNull final Class<V> vClass,
                          @NotNull final Statement stmt,
                          @NotNull final String table,
                          @NotNull final FieldMapper<V> fieldMapper,
                          @NotNull final ReplicatedSharedHashMap.EntryResolver entryResolver) throws InstantiationException {
        super(kClass, vClass, entryResolver);

        this.fieldMapper = fieldMapper;
        this.stmt = stmt;
        this.table = table;
        this.vClass = vClass;

        this.dateTimeZone = DEFAULT_DATE_TIME_FORMATTER.getZone();
        this.dateTimeFormatter = DEFAULT_DATE_TIME_FORMATTER;
        shortDateTimeFormatter = DateTimeFormat.forPattern(YYYY_MM_DD).withZone(dateTimeFormatter.getZone());
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

            if (LOG.isDebugEnabled()) {
                LOG.debug("insert-sql=" + sql.toString());
            }
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("update-sql=" + sql.toString());
        }

        final int rowCount = stmt.executeUpdate(sql.toString());


        if (rowCount == 0) {
            insert(key, value);
        }

    }


    @Override
    public void putExternal(K key, V value, boolean added) {
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
    public V getExternal(K key) {
        try {
            final String sql = "SELECT * FROM " + this.table + " WHERE " + fieldMapper.keyName() + "=" + key.toString();
            final ResultSet resultSet = stmt.executeQuery(sql);
            return applyResultsSet(null, resultSet);
        } catch (Exception e) {
            LOG.error("", e);
            return null;
        }
    }


    @Override
    public DateTimeZone getZone() {
        return this.dateTimeZone;
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
        return applyResultsSet(new HashSet(), resultSet);
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
        return applyResultsSet(new HashMap(), stmt.executeQuery(sql));
    }


    @Override
    public Map<K, V> getAllExternal(@NotNull Map<K, V> usingMap) {

        final String sql = "SELECT * FROM " + this.table;
        try {
            applyResultsSet(usingMap, stmt.executeQuery(sql));

        } catch (Exception e) {
            LOG.error("", e);
        }
        return usingMap;
    }


    /**
     * apply the result set to the object
     *
     * @param using the object to update  ( pass null if you want to return an object of type V )
     * @return the object that represents the result set
     * @throws SQLException
     * @throws InstantiationException
     */
    private <R> R applyResultsSet(@Nullable R using,
                                  @NotNull final ResultSet resultSet) throws SQLException,
            InstantiationException, IllegalAccessException {

        final Class<?> rClass = (using == null) ? vClass : using.getClass();

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

                    else if (field.getType().equals(char.class)) {
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
                        final DateTime dateTime = dateFormat(date).parseDateTime(date).toDateTime();

                        field.set(o, dateTime);
                    } else if (field.getType().equals(Date.class)) {
                        String date = resultSet.getString(fieldNameByType.getValue());

                        final DateTime dateTime = dateFormat(date).parseDateTime(date);

                        field.set(o, dateTime.toDate());
                    } else
                        LOG.error("",
                                new UnsupportedOperationException("unsupported type " + field.getType()));

                    if (Map.class.isAssignableFrom(rClass) && fieldNameByType.getValue().equals(keyName))
                        key = (K) field.get(o);

                } catch (Exception e) {
                    LOG.error("", e);
                }

            }

            if (rClass.isAssignableFrom(vClass))
                return (R) o;

            if (Collection.class.isAssignableFrom(rClass))
                ((Collection) using).add(o);

            else if (Map.class.isAssignableFrom(rClass)) {

                if (key == null) {
                    LOG.debug("skipping value=" + o + " as no key was found.");
                    continue;
                }

                ((Map) using).put(key, o);

            } else
                throw new IllegalStateException("Unexpected class type class=" + vClass);

        }
        return using;
    }

    private DateTimeFormatter dateFormat(String date) {
        return (date.length() == YYYY_MM_DD.length()) ? shortDateTimeFormatter : dateTimeFormatter;
    }


    @Override
    public void removeAllExternal(final Set<K> keys) {

        if (keys.isEmpty())
            return;

        final StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(table);

        sql.append(" WHERE ");
        sql.append(fieldMapper.keyName());
        sql.append(" IN (");
        for (K key : keys) {
            sql.append(key.toString()).append(',');
        }

        sql.deleteCharAt(sql.length() - 1);

        sql.append(")");

        try {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            LOG.error("", e);
        }

    }


    @Override
    public void removeExternal(K k) {

        final StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(table);
        sql.append(" WHERE ");
        sql.append(fieldMapper.keyName());
        sql.append(" = ");
        sql.append(k.toString());

        try {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            LOG.error("", e);
        }

    }

}





