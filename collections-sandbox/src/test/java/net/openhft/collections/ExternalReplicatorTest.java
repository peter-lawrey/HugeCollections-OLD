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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;


@RunWith(value = Parameterized.class)
public class ExternalReplicatorTest {

    private final ExternalReplicator<Integer, BeanClass> externalReplicator;


    private Connection connection;
    private Statement stmt;

    class BeanClass {

        @Key(name = "ID")
        int id;

        @Column(name = "NAME")
        String name;

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

        BeanClass(int id, String name, double doubleValue, Date timeStamp, Date dateValue, char charValue, boolean booleanValue, short shortVal, DateTime dateTimeValue) {
            this.id = id;
            this.name = name;
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
    public static Collection<Object[]> data() throws SQLException {

        final String dbURL = "jdbc:derby:memory:openhft;create=true";

        Connection connection = DriverManager.getConnection(dbURL);
        connection.setAutoCommit(true);
        Statement stmt = connection.createStatement();

        String tableName = createUniqueTableName();

        stmt.executeUpdate("create table " + tableName + " (" +
                "ID integer NOT NULL, " +
                "NAME varchar(40) NOT NULL, " +
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
                        new FileReplicator<Integer, BeanClass, SharedHashMap<Integer, BeanClass>>(
                                BeanClass.class, System.getProperty("java.io.tmpdir")), DateTimeZone.UTC
                },
                {
                        new JDBCReplicator<Object, BeanClass, SharedHashMap<Object, BeanClass>>(
                                BeanClass.class, stmt, tableName), DateTimeZone.UTC
                }
        });
    }


    public ExternalReplicatorTest(ExternalReplicator externalReplicator, DateTimeZone dateTimeZone) {
        this.externalReplicator = externalReplicator;
        externalReplicator.withZone(dateTimeZone);

    }


    @Test
    public void test() throws ClassNotFoundException, SQLException, IOException, InstantiationException {

        final Date expectedDate = new Date(0);
        final DateTime expectedDateTime = new DateTime(0, DateTimeZone.UTC);
        final BeanClass bean = new BeanClass(1, "Rob", 1.234, expectedDate, expectedDate, 'c', false,
                (short) 1, expectedDateTime);


        externalReplicator.put(bean.id, bean, true);

        final BeanClass result = externalReplicator.get(bean.id);

        Assert.assertEquals("Rob", result.name);
        Assert.assertEquals(1.234, result.doubleValue, 0.001);
        Assert.assertEquals('c', result.charValue);
        Assert.assertEquals(false, result.booleanValue);
        Assert.assertEquals(1, result.shortVal);


        Assert.assertEquals(expectedDateTime, result.dateTimeValue);
        Assert.assertEquals(expectedDate, result.timeStamp);


    }

    private static int sequenceNumber;

    private static String createUniqueTableName() {
        return "dbo.Test" + (sequenceNumber++);
    }


}



