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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

/**
 * @author Rob Austin.
 */
public class JDBCReplicatorTest {

    private Connection connection;
    private Statement stmt;

    @Before
    public void setup() throws SQLException {
        final String dbURL = "jdbc:derby:memory:openhft;create=true";
        connection = DriverManager.getConnection(dbURL);
        connection.setAutoCommit(true);
        stmt = connection.createStatement();
    }

    @After
    public void after() throws SQLException {

        if (stmt != null)
            stmt.close();

        if (connection != null)
            connection.close();
    }

    /**
     * an example of the embedded in memory JDBC database connectivity
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void testSimpleJDBCConnectivity() throws ClassNotFoundException, SQLException {

        String tableName = createUniqueTableName();

        stmt.executeUpdate("create table " + tableName +
                " (ID integer NOT NULL, " +
                "NAME varchar(40) NOT NULL, " +
                "PRIMARY KEY (ID))");

        stmt.execute("insert into " + tableName + " (ID,NAME) values (1,'rob')");

        ResultSet resultSets = stmt.executeQuery("select * from " + tableName);

        resultSets.next();

        Assert.assertEquals(1, resultSets.getInt("ID"));
        Assert.assertEquals("rob", resultSets.getString("NAME"));


    }


    @Test
    public void testJDBCWithCustomFieldMapper() throws ClassNotFoundException, SQLException {

        String tableName = createUniqueTableName();
        String createString =
                "create table " + tableName + " " +
                        "(ID integer NOT NULL, " +
                        "F1 varchar(40) NOT NULL, " +
                        "PRIMARY KEY (ID))";

        stmt.executeUpdate(createString);

        final JDBCReplicator jdbcCReplicator = new JDBCReplicator(new JDBCReplicator.Fields() {

            @Override
            public CharSequence getKeyName() {
                return "ID";
            }

            @Override
            public Set<Field> getFields(Object value) {
                return Collections.singleton(new Field("F1", "'Rob'"));
            }
        }, stmt, tableName);


        jdbcCReplicator.onUpdate("1", "F1");
        ResultSet resultSets = stmt.executeQuery("select * from " + tableName);

        resultSets.next();

        Assert.assertEquals("Rob", resultSets.getString("F1"));


    }


    @Test
    public void testJDBCWithAnnotationBasedFieldMapper() throws ClassNotFoundException, SQLException {

        class BeanClass {

            @Key(name = "ID")
            int id;

            @Column(name = "NAME")
            String name;

            @Column(name = "DOUBLE_VAL")
            double doubleValue;

            @Column(name = "DATE_VAL")
            Date dateValue;

            @Column(name = "CHAR_VAL")
            char c;

            @Column(name = "BOOL_VAL")
            boolean bool;

            @Column(name = "SHORT_VAL")
            short shortVal;


            BeanClass(int id, String name, double doubleValue, Date dateValue, char c, boolean bool, short shortVal) {
                this.id = id;
                this.name = name;
                this.doubleValue = doubleValue;
                this.dateValue = dateValue;
                this.c = c;
                this.bool = bool;
                this.shortVal = shortVal;
            }
        }

        final String tableName = createUniqueTableName();

        stmt.executeUpdate("create table " + tableName + " (" +
                "ID integer NOT NULL, " +
                "NAME varchar(40) NOT NULL, " +
                "CHAR_VAL char(1) NOT NULL, " +
                "DOUBLE_VAL REAL," +
                "SHORT_VAL SMALLINT," +
                "DATE_VAL DATE," +
                "BOOL_VAL BOOLEAN," +
                "PRIMARY KEY (ID))");

        final JDBCReplicator<Object, BeanClass, SharedHashMap<Object, BeanClass>> jdbcCReplicator = new JDBCReplicator<Object, BeanClass, SharedHashMap<Object, BeanClass>>(BeanClass.class, stmt, tableName);
        final Date expectedDate = new Date(0);
        final BeanClass bean = new BeanClass(1, "Rob", 1.234, expectedDate, 'c', false, (short) 1);

        jdbcCReplicator.onUpdate(bean.id, bean);

        final ResultSet resultSets = stmt.executeQuery("select * from " + tableName);

        resultSets.next();

        Assert.assertEquals("Rob", resultSets.getString("NAME"));
        Assert.assertEquals(1.234, resultSets.getDouble("DOUBLE_VAL"), 0.001);
        Assert.assertEquals("c", resultSets.getString("CHAR_VAL"));
        Assert.assertEquals(false, resultSets.getBoolean("BOOL_VAL"));
        Assert.assertEquals(1, resultSets.getShort("SHORT_VAL"));
        final java.sql.Date expected = new java.sql.Date(expectedDate.getTime());

        Assert.assertEquals(expected.toLocalDate(), resultSets.getDate("DATE_VAL").toLocalDate());

    }

    private static int sequenceNumber;

    private static String createUniqueTableName() {
        return "dbo.Test" + (sequenceNumber++);
    }
}



