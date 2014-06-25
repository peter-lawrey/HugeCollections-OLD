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

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

/**
 * @author Rob Austin.
 */
public class JDBCConnectivityTest {


    /**
     * an example of the embedded in memory JDBC database connectivity
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void testSimpleJDBC() throws ClassNotFoundException, SQLException {

        String dbURL = "jdbc:derby:memory:openhft;create=true";
        Connection conn1 = DriverManager.getConnection(dbURL);
        conn1.setAutoCommit(false);

        String createString =
                "create table dbo.Test " +
                        "(ID integer NOT NULL, " +
                        "NAME varchar(40) NOT NULL, " +
                        "PRIMARY KEY (ID))";

        Statement stmt = null;

        try {

            stmt = conn1.createStatement();
            stmt.executeUpdate(createString);
            stmt.execute("insert into dbo.Test (ID,NAME) values (1,'rob')");

            ResultSet resultSets = stmt.executeQuery("select * from dbo.Test ");

            resultSets.next();

            Assert.assertEquals(1, resultSets.getInt("ID"));
            Assert.assertEquals("rob", resultSets.getString("NAME"));

        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }


    }


}
