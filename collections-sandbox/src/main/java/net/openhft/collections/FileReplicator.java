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
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.*;

import static net.openhft.collections.ExternalReplicator.AbstractExternalReplicator;
import static net.openhft.collections.FieldMapper.ReflectionBasedFieldMapperBuilder;
import static net.openhft.collections.FieldMapper.ValueWithFieldName;

/**
 * Replicates the contents of the map into a directory, each file in the directory represents an entry in the
 * map
 *
 * @author Rob Austin.
 */
public class FileReplicator<K, V, M> extends
        AbstractExternalReplicator<K, V> {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());
    private static final String SEPARATOR = System.getProperty("line.separator");

    private final FieldMapper<V> fieldMapper;
    private final Class<V> vClass;
    private final String directory;
    private final String fileExt = ".value";
    private final Map<String, Field> fieldsByColumnsName;
    private final DateTimeFormatter dateTimeFormatter;
    private final DateTimeZone dateTimeZone;


    public FileReplicator(final Map map,
                          @NotNull final Class<V> vClass,
                          @NotNull final String directory,
                          final DateTimeZone dateTimeZone) {
        super(map);

        this.vClass = vClass;


        this.directory = directory;

        this.dateTimeZone = dateTimeZone;
        this.dateTimeFormatter = DEFAULT_DATE_TIME_FORMATTER.withZone(dateTimeZone);

        final ReflectionBasedFieldMapperBuilder builder = new ReflectionBasedFieldMapperBuilder();
        fieldMapper = builder.create(vClass, dateTimeFormatter);

        final Map<Field, String> fieldStringMap = fieldMapper.columnsNamesByField();
        fieldsByColumnsName = new HashMap<String, Field>(fieldStringMap.size());

        for (Map.Entry<Field, String> entry : fieldStringMap.entrySet()) {
            fieldsByColumnsName.put(entry.getValue().toUpperCase().trim(), entry.getKey());
        }

    }

    public String getFileExt() {
        return fileExt;
    }

    void onPut(M map, Bytes entry, int metaDataBytes, boolean added, K key, V value,
               long pos, SharedSegment segment) {

        putExternal(key, value, true);
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
        final File file = new File(directory + File.pathSeparator + key.toString());
        file.delete();
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
        final File file = new File(directory + File.pathSeparator + key.toString());
        file.delete();

    }


    /**
     * since we are on the only system read and writing this data to the database, we will know, if the record
     * has already been inserted or updated so can call the appropriate sql
     */
    public void putExternal(K key, V value, boolean added) {
        try {

            final File file = toFile(key);

            if (!file.exists()) {
                synchronized (this) {
                    if (!file.exists()) {
                        final boolean wasCreated = file.createNewFile();

                        if (!wasCreated) {
                            LOG.error("It was not possible to store the entry in file=" + file.getAbsoluteFile());
                            return;
                        }

                    }
                }

            }

            final StringBuilder stringBuilder = new StringBuilder();


            stringBuilder.append(fieldMapper.keyName()).append("=")
                    .append(fieldMapper.keyField().get(value).toString())
                    .append(SEPARATOR);

            for (final ValueWithFieldName field : fieldMapper.getFields(value, true)) {
                stringBuilder.append(field.name).append("=").append(field.value).append(SEPARATOR);
            }

            // remove the trailing new line
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);

            final FileWriter fw = new FileWriter(file);
            final BufferedWriter bw = new BufferedWriter(fw);
            bw.write(stringBuilder.toString());
            bw.close();
        } catch (Exception e) {
            LOG.error("", e);
        }

    }

    private File toFile(K key) {
        return new File(directory + key.toString() + fileExt);
    }


    /**
     * gets all the records from the database as a Map<K,V>
     *
     * @return
     * @throws java.sql.SQLException
     * @throws InstantiationException
     */
    public V getExternal(K key) {

        try {
            final File folder = new File(directory);

            if (!folder.isDirectory()) {
                throw new IllegalArgumentException("NOT A VALID DIRECTORY : directory=" + directory);
            }

            return get(new File(directory + key.toString() + fileExt));

        } catch (Exception e) {
            LOG.error("", e);
            return null;
        }
    }

    @Override
    public void removeAllExternal() {

        final File folder = new File(directory);

        if (!folder.isDirectory()) {
            throw new IllegalArgumentException("NOT A VALID DIRECTORY : directory=" + directory);
        }

        for (K key : map.keySet()) {
            final File file = toFile(key);
            file.delete();
        }


    }


    private V get(final File file) throws InstantiationException, FileNotFoundException {

        if (!file.exists())
            return null;

        final V o = (V) NativeBytes.UNSAFE.allocateInstance(this.vClass);

        final Scanner scanner = new Scanner(file);

        try {

            while (scanner.hasNextLine()) {

                final String s = scanner.nextLine();
                final int i = s.indexOf("=");

                if (i == -1) {
                    LOG.debug("Skipping line='" + s + "' from file='" + file.getAbsolutePath() +
                            " as its missing a  '='");
                    continue;
                }

                final String fieldKey = s.substring(0, i);
                final String fieldValue = (i == s.length()) ? "" : s.substring(i + 1, s.length());


                final Field field = fieldsByColumnsName.get(fieldKey);

                try {
                    if (field.getType().equals(boolean.class))
                        field.setBoolean(o, Boolean.parseBoolean(fieldValue));

                    else if (field.getType().equals(byte.class))
                        field.setByte(o, Byte.parseByte(fieldValue));

                    if (field.getType().equals(char.class))
                        field.setChar(o, fieldValue.length() > 0 ? fieldValue.charAt(0) : 0);

                    else if (field.getType().equals(short.class))
                        field.setShort(o, Short.parseShort(fieldValue));

                    else if (field.getType().equals(Float.class))
                        field.setFloat(o, Float.parseFloat(fieldValue));

                    else if (field.getType().equals(int.class))
                        field.setInt(o, Integer.parseInt(fieldValue));

                    else if (field.getType().equals(Long.class))
                        field.setLong(o, Long.parseLong(fieldValue));

                    else if (field.getType().equals(double.class))
                        field.setDouble(o, Double.parseDouble(fieldValue));

                    else if (field.getType().equals(String.class))
                        field.set(o, fieldValue);

                    else if (field.getType().equals(DateTime.class)) {
                        final DateTime dateTime = FileReplicator.this.dateTimeFormatter.parseDateTime
                                (fieldValue);

                        if (dateTime != null)
                            field.set(o, dateTime);
                    } else if (field.getType().equals(Date.class)) {
                        final DateTime dateTime = dateTimeFormatter.parseDateTime
                                (fieldValue);
                        if (dateTime != null)
                            field.set(o, dateTime.toDate());
                    }


                } catch (Exception e) {
                    continue;
                }
            }
        } finally {
            scanner.close();
        }

        return o;
    }


    @Override
    public DateTimeZone getZone() {
        return this.dateTimeZone;
    }

    /**
     * gets all the records from the database for a list of keys
     *
     * @return
     * @throws java.sql.SQLException
     * @throws InstantiationException
     */
    public Set<V> get(K... keys) throws SQLException, InstantiationException, IllegalAccessException, IOException {

        final HashSet<V> result = new HashSet<V>(keys.length);

        for (K k : keys) {
            result.add(getExternal(k));
        }
        return result;
    }

    /**
     * gets all the records from the database as a Map<K,V>
     *
     * @return
     * @throws java.sql.SQLException
     * @throws InstantiationException
     */
    public Map<K, V> getAll() throws SQLException, InstantiationException, IllegalAccessException {
        return aquireAllUsing(new HashMap<K, V>());
    }

    @Override
    public void putAllEntries() {
        aquireAllUsing(map);
    }

    class ExtFilter implements FilenameFilter {

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(fileExt);
        }
    }

    private final ExtFilter filenameFilter = new ExtFilter();

    private Map<K, V> aquireAllUsing(final Map<K, V> map) {

        final File folder = new File(directory);
        if (!folder.isDirectory()) {
            throw new IllegalArgumentException("NOT A VALID DIRECTORY : directory=" + directory);
        }

        for (final File fileEntry : folder.listFiles(filenameFilter)) {
            try {
                final V v = get(fileEntry);
                map.put((K) fieldMapper.keyField().get(v), v);
            } catch (Exception e) {
                LOG.error("fileEntry=" + fileEntry, e);
                continue;
            }
        }

        return map;

    }

}





