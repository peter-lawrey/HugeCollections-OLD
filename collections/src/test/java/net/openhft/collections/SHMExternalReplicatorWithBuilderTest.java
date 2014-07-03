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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static net.openhft.collections.ExternalReplicator.FieldMapper.Column;
import static net.openhft.collections.ExternalReplicator.FieldMapper.Key;
import static net.openhft.collections.SHMExternalReplicatorWithoutBuilderTest.waitTimeEqual;
import static org.joda.time.DateTimeZone.UTC;

/**
 * This test uses the modification iterator to update the map
 */
@RunWith(value = Parameterized.class)
public class SHMExternalReplicatorWithBuilderTest {


    private static Connection connection;

    static class BeanClass implements Serializable {

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


        @Override
        public String toString() {
            return "BeanClass{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", fullCamelCaseFieldName='" + fullCamelCaseFieldName + '\'' +
                    ", doubleValue=" + doubleValue +
                    ", timeStamp=" + timeStamp +
                    ", dateValue=" + dateValue +
                    ", charValue=" + charValue +
                    ", booleanValue=" + booleanValue +
                    ", shortVal=" + shortVal +
                    '}';
        }
    }


    private final ExternalReplicator<Integer, BeanClass> externalReplicator;

    private final Map<Integer, BeanClass> map;

    static int count;

    public static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime() + (count++));
        file.deleteOnExit();
        return file;
    }


    @Parameterized.Parameters
    public static Collection<Object[]> data() throws SQLException, InstantiationException, IOException {

        final String dbURL = "jdbc:derby:memory:openhft;create=true";

        connection = DriverManager.getConnection(dbURL);
        connection.setAutoCommit(true);

        Statement stmt = connection.createStatement();

        final String tableName = createUniqueTableName();

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


        return Arrays.asList(new Object[][]{
                {
                        new ExternalFileReplicatorBuilder(BeanClass.class)
                },
                {
                        new ExternalJDBCReplicatorBuilder(BeanClass.class, stmt, tableName)
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
        map.clear();

    }

    public SHMExternalReplicatorWithBuilderTest(ExternalReplicatorBuilder builder) throws
            IOException {


        final SharedHashMapBuilder sharedHashMapBuilder = new SharedHashMapBuilder()
                .entries(1000)
                .identifier((byte) 1)
                .canReplicate(true)
                .externalReplicatorBuilder(builder)
                .entries(20000);

        map = sharedHashMapBuilder.create(getPersistenceFile(), Integer.class, BeanClass.class);
        externalReplicator = sharedHashMapBuilder.externalReplicator();
    }

    @Test
    public void testModificationIterator() throws IOException, InterruptedException, TimeoutException, SQLException {

        final Date expectedDate = new Date(0);
        final DateTime expectedDateTime = new DateTime(0, UTC);
        final BeanClass bean1 = new BeanClass(1,
                "Rob",
                "camelCase",
                1.5,
                expectedDate,
                expectedDate,
                'a',
                false,
                (short) 1, expectedDateTime);

        final BeanClass bean2 = new BeanClass(2,
                "Rob2",
                "camelCase",
                2.5,
                expectedDate,
                expectedDate,
                'b',
                false,
                (short) 1, expectedDateTime);


        final BeanClass bean3 = new BeanClass(3,
                "Rob3",
                "camelCase",
                3.5,
                expectedDate,
                expectedDate,
                'c',
                false,
                (short) 1, expectedDateTime);


        final BeanClass[] beanClasses = {bean1, bean2, bean3};


        for (BeanClass bean : beanClasses) {
            map.put(bean.id, bean);
        }

        connection.commit();
        Thread.sleep(100);

        // we will now check, that what as got saved to the database is all 3 beans
        for (final BeanClass bean : beanClasses) {
            final BeanClass external = getExternal(bean.id, 1000);
            Assert.assertTrue(waitTimeEqual(10, bean.name, external.name));
        }

    }


    private static int sequenceNumber;

    private static String createUniqueTableName() {
        return "dbo.Test" + System.nanoTime() + sequenceNumber++;
    }

    public BeanClass getExternal(Integer id, long timeout) throws TimeoutException, InterruptedException {
        long endTime = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < endTime) {
            final BeanClass external = externalReplicator.getExternal(id);
            if (external != null)
                return external;
            Thread.sleep(1);
        }
        throw new TimeoutException("unable to get id=" + id);
    }

}


