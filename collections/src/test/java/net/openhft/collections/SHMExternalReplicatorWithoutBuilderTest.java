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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static net.openhft.collections.ExternalReplicator.FieldMapper.Column;
import static net.openhft.collections.ExternalReplicator.FieldMapper.Key;
import static org.joda.time.DateTimeZone.UTC;


@RunWith(value = Parameterized.class)
public class SHMExternalReplicatorWithoutBuilderTest {

    private final ExternalReplicator.AbstractExternalReplicator<Integer, BeanClass> externalReplicator;
    private final Map map;


    static final ReplicatedSharedHashMap.EntryResolver NOP_ENTRY_RESOLVER = new
            ReplicatedSharedHashMap.EntryResolver() {

                @Override
                public Object key(@NotNull NativeBytes entry, Object usingKey) {
                    return null;
                }

                @Override
                public Object value(@NotNull NativeBytes entry, Object usingValue) {
                    return null;
                }

                @Override
                public boolean wasRemoved(@NotNull NativeBytes entry) {
                    return false;
                }
            };

    class BeanClass {

        @Key(name = "ID")
        int id;

        // testing without the name annotation , the field will be used instead
        @Column
        String name;


        // testing without the name annotation , the field will be used instead
        @Column
        String fullCamelCaseFieldName;


        @Column(name = "DOUBLE_VAL")
        double doubleValue;

        @Column(name = "TIMESTAMP_VAL")
        Date timeStamp;

        @Column(name = "DATE_VAL")
        Date dateValue;


        @Column(name = "CHAR_VAL")
        char charValue;

        @Column(name = "BOOL_VAL")
        boolean booleanValue;

        @Column(name = "SHORT_VAL")
        short shortVal;

        @Column(name = "DATETIME_VAL")
        DateTime dateTimeValue;

        BeanClass(int id, String name,
                  String fullCamelCaseFieldName,
                  double doubleValue,
                  Date timeStamp,
                  Date dateValue,
                  char charValue,
                  boolean booleanValue,
                  short shortVal,
                  DateTime dateTimeValue) {
            this.id = id;
            this.name = name;
            this.fullCamelCaseFieldName = fullCamelCaseFieldName;
            this.doubleValue = doubleValue;
            this.timeStamp = timeStamp;
            this.dateValue = dateValue;
            this.charValue = charValue;
            this.booleanValue = booleanValue;
            this.shortVal = shortVal;
            this.dateTimeValue = dateTimeValue;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws SQLException, InstantiationException {

        final String dbURL = "jdbc:derby:memory:openhft;create=true";

        Connection connection = DriverManager.getConnection(dbURL);
        connection.setAutoCommit(true);
        Statement stmt = connection.createStatement();

        String tableName = createUniqueTableName();

        stmt.executeUpdate("CREATE TABLE " + tableName + " (" +
                "ID integer NOT NULL, " +
                "NAME varchar(40) NOT NULL, " +
                "FULL_CAMEL_CASE_FIELD_NAME varchar(40) NOT NULL, " +
                "CHAR_VAL char(1) NOT NULL, " +
                "DOUBLE_VAL REAL," +
                "SHORT_VAL SMALLINT," +
                "DATE_VAL DATE," +
                "TIMESTAMP_VAL TIMESTAMP," +
                "DATETIME_VAL TIMESTAMP," +
                "BOOL_VAL BOOLEAN," +
                "PRIMARY KEY (ID))");


        final HashMap<Integer, BeanClass> map = new HashMap<Integer, BeanClass>();
        ReplicatedSharedHashMap.EntryResolver NOP_ENTRY_RESOLVER = new ReplicatedSharedHashMap.EntryResolver() {

            @Override
            public Object key(@NotNull NativeBytes entry, Object usingKey) {
                return null;
            }

            @Override
            public Object value(@NotNull NativeBytes entry, Object usingValue) {
                return null;
            }

            @Override
            public boolean wasRemoved(@NotNull NativeBytes entry) {
                return false;
            }
        };


        final ExternalFileReplicatorBuilder<Integer, BeanClass> fileReplicatorBuilder = new ExternalFileReplicatorBuilder
                (BeanClass.class);
        final ExternalJDBCReplicatorBuilder<BeanClass> jdbcReplicatorBuilder = new ExternalJDBCReplicatorBuilder(BeanClass.class, stmt, tableName);

        return Arrays.asList(new Object[][]{
                {
                        new ExternalFileReplicator<Integer, BeanClass>(
                                Integer.class, BeanClass.class,
                                fileReplicatorBuilder, NOP_ENTRY_RESOLVER), map
                },
                {
                        new ExternalJDBCReplicator<Integer, BeanClass>(
                                Integer.class, BeanClass.class,
                                jdbcReplicatorBuilder, NOP_ENTRY_RESOLVER), map
                },
                {
                        new ExternalJDBCReplicator<Integer, BeanClass>(
                                Integer.class, BeanClass.class,
                                jdbcReplicatorBuilder.dateTimeZone(DateTimeZone.getDefault()),
                                NOP_ENTRY_RESOLVER), map
                }
        });
    }

    @Before
    public void setup() {
        map.clear();
    }

    @After
    public void after() {
        // cleans up the files or removes the items from the database
        externalReplicator.removeAllExternal(map.keySet());
    }

    public SHMExternalReplicatorWithoutBuilderTest(ExternalReplicator.AbstractExternalReplicator externalReplicator, Map map) {
        this.externalReplicator = externalReplicator;
        this.map = map;
    }


    @Test
    public void test() throws ClassNotFoundException, SQLException, IOException, InstantiationException {

        final Date expectedDate = new Date(0);
        final DateTime expectedDateTime = new DateTime(0, UTC);
        final BeanClass bean = new BeanClass(1, "Rob", "camelCase", 1.234, expectedDate, expectedDate, 'c',
                false,
                (short) 1, expectedDateTime);


        externalReplicator.putExternal(bean.id, bean, true);

        final BeanClass result = externalReplicator.getExternal(bean.id);

        Assert.assertEquals("Rob", result.name);
        Assert.assertEquals("camelCase", result.fullCamelCaseFieldName);
        Assert.assertEquals(1.234, result.doubleValue, 0.001);
        Assert.assertEquals('c', result.charValue);
        Assert.assertEquals(false, result.booleanValue);
        Assert.assertEquals(1, result.shortVal);
        Assert.assertEquals(expectedDateTime.toDate().getTime(), result.dateTimeValue.toDate().getTime());
        Assert.assertEquals(expectedDate, result.timeStamp);

    }

    @Test
    @Ignore
    public void testPutAllEntries() {

        final Date expectedDate = new Date(0);
        final DateTime expectedDateTime = new DateTime(0, UTC);
        final BeanClass bean1 = new BeanClass(1,
                "Rob",
                "camelCase",
                1.111,
                expectedDate,
                expectedDate,
                'a',
                false,
                (short) 1, expectedDateTime);

        final BeanClass bean2 = new BeanClass(2,
                "Rob2",
                "camelCase",
                1.222,
                expectedDate,
                expectedDate,
                'b',
                false,
                (short) 1, expectedDateTime);


        final BeanClass bean3 = new BeanClass(3,
                "Rob3",
                "camelCase",
                1.333,
                expectedDate,
                expectedDate,
                'c',
                false,
                (short) 1, expectedDateTime);


        final BeanClass[] beanClasses = {bean1, bean2, bean3};
        for (BeanClass bean : beanClasses) {
            externalReplicator.putExternal(bean.id, bean, true);
        }

        externalReplicator.getAllExternal(map);

        for (BeanClass bean : beanClasses) {
            final int key = bean.id;
            Assert.assertEquals(bean.charValue, ((BeanClass) map.get(key)).charValue);
        }


    }


    @Test
    public void testPutEntry() {

        final Date expectedDate = new Date(0);
        final DateTime expectedDateTime = new DateTime(0, UTC);
        final BeanClass bean1 = new BeanClass(1,
                "Rob",
                "camelCase",
                1.111,
                expectedDate,
                expectedDate,
                'a',
                false,
                (short) 1, expectedDateTime);

        final BeanClass bean2 = new BeanClass(2,
                "Rob2",
                "camelCase",
                1.222,
                expectedDate,
                expectedDate,
                'b',
                false,
                (short) 1, expectedDateTime);


        final BeanClass bean3 = new BeanClass(3,
                "Rob3",
                "camelCase",
                1.333,
                expectedDate,
                expectedDate,
                'c',
                false,
                (short) 1, expectedDateTime);


        final BeanClass[] beanClasses = {bean1, bean2, bean3};
        for (BeanClass bean : beanClasses) {
            externalReplicator.putExternal(bean.id, bean, true);
        }

        final BeanClass external = externalReplicator.getExternal(3);
        map.put(3, external);

        Assert.assertEquals(bean3.charValue, external.charValue);

    }


    private static int sequenceNumber;

    private static String createUniqueTableName() {
        return "dbo.Test" + (sequenceNumber++);
    }


    public static boolean waitTimeEqual(long timeMs, Object expected,
                                        Object actual) throws InterruptedException {
        long endTime = System.currentTimeMillis() + timeMs;
        while (System.currentTimeMillis() < endTime) {
            if (expected.equals(actual))
                return true;
            Thread.sleep(1);
        }
        return false;
    }
}




