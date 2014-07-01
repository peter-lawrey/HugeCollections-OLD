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
import org.joda.time.DateTimeZone;
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

import static net.openhft.collections.ExternalReplicator.AbstractExternalReplicator;
import static org.joda.time.DateTimeZone.UTC;

/**
 * This test uses the modification iterator to update the map
 */
@RunWith(value = Parameterized.class)
public class SHMExternalReplicatorWithModIterTest {

    private final AbstractExternalReplicator<Integer, BeanClass> externalReplicator;
    private final VanillaSharedReplicatedHashMap map;


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


    static int count;

    public static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime() + (count++));
        file.deleteOnExit();
        return file;
    }


    public static <T extends VanillaSharedReplicatedHashMap<Integer, ExternalReplicatorTest.BeanClass>>

    VanillaSharedReplicatedHashMap<Integer, BeanClass> newIntBeanSHM(
            final byte identifier) throws IOException {


        SharedHashMapBuilder b = new SharedHashMapBuilder()
                .entries(1000)
                .identifier(identifier)
                .canReplicate(true)
                .entries(20000);

        return (VanillaSharedReplicatedHashMap) b.create(getPersistenceFile(), Integer.class,
                BeanClass.class);
    }


    @Parameterized.Parameters
    public static Collection<Object[]> data() throws SQLException, InstantiationException, IOException {

        final String dbURL = "jdbc:derby:memory:openhft;create=true";

        Connection connection = DriverManager.getConnection(dbURL);
        connection.setAutoCommit(true);
        Statement stmt = connection.createStatement();

        final String tableName = createUniqueTableName();

        stmt.executeUpdate("create table " + tableName + " (" +
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


        final VanillaSharedReplicatedHashMap<Integer, BeanClass> map = newIntBeanSHM((byte) 1);

        return Arrays.asList(new Object[][]{
                {
                        new FileReplicator<Integer, BeanClass, SharedHashMap<Integer, BeanClass>>(
                                Integer.class, BeanClass.class,
                                System.getProperty("java.io.tmpdir"),
                                UTC, map), map
                },
                {
                        new JDBCReplicator<Integer, BeanClass, SharedHashMap<Integer, BeanClass>>(
                                Integer.class, BeanClass.class,
                                stmt, tableName, UTC, map), map
                },
                {
                        new FileReplicator<Integer, BeanClass, SharedHashMap<Integer, BeanClass>>(
                                Integer.class, BeanClass.class, System.getProperty("java.io.tmpdir"),
                                DateTimeZone.getDefault(), map), map

                },
                {
                        new JDBCReplicator<Integer, BeanClass, SharedHashMap<Integer, BeanClass>>(
                                Integer.class, BeanClass.class, stmt, tableName,
                                DateTimeZone.getDefault(), map), map
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

    public SHMExternalReplicatorWithModIterTest(AbstractExternalReplicator externalReplicator, VanillaSharedReplicatedHashMap map) throws
            IOException {
        this.externalReplicator = externalReplicator;
        this.map = map;
    }

    @Test
    public void testModificationIterator() throws IOException {

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


        ReplicatedSharedHashMap.ModificationNotifier notifier = new ReplicatedSharedHashMap
                .ModificationNotifier() {
            @Override
            public void onChange() {

            }
        };

        final VanillaSharedReplicatedHashMap.ModificationIterator modificationIterator
                = map.acquireModificationIterator((byte) 1, notifier, true);

        for (BeanClass bean : beanClasses) {
            map.put(bean.id, bean);
        }

        // this will update the database with the new values added to the map
        // ideally this will be run on its own thread
        while (modificationIterator.hasNext()) {
            modificationIterator.nextEntry(externalReplicator);
        }

        // we will now check, that what as got saved to the database is all 3 beans
        for (final BeanClass bean : beanClasses) {
            final BeanClass external = externalReplicator.getExternal(bean.id);
            Assert.assertEquals(bean.name, external.name);
        }

    }

    private static int sequenceNumber;

    private static String createUniqueTableName() {
        return "dbo.Test" + System.nanoTime() + sequenceNumber++;
    }


}




