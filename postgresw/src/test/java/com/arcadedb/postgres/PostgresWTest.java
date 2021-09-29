/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arcadedb.postgres;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.Properties;

public class PostgresWTest extends BaseGraphServerTest {
    @Override
    public void setTestConfiguration() {
        super.setTestConfiguration();
        GlobalConfiguration.SERVER_PLUGINS.setValue("Postgres Protocol:com.arcadedb.postgres.PostgresProtocolPlugin");

    }

    @AfterEach
    @Override
    public void endTest() {
        GlobalConfiguration.SERVER_PLUGINS.setValue("");
        super.endTest();
    }

    @Test
    public void testTypeNotExistsErrorManagement() throws Exception {
        try (final Connection conn = getConnection()) {
            try (Statement st = conn.createStatement()) {
                try {
                    st.executeQuery("SELECT * FROM V");
                    Assertions.fail("The query should go in error");
                } catch (PSQLException e) {
                }
            }
        }
    }

    @Test
    public void queryVertices() throws Exception {
        try (final Connection conn = getConnection()) {
            try (Statement st = conn.createStatement()) {
                st.execute("create vertex type V");
                st.execute("create vertex V set name = 'Jay', lastName = 'Miner'");

                PreparedStatement pst = conn.prepareStatement("create vertex V set name = ?, lastName = ?");
                pst.setString(1, "Rocky");
                pst.setString(2, "Balboa");
                pst.execute();
                pst.close();

                ResultSet rs = st.executeQuery("SELECT * FROM V");

                Assertions.assertTrue(!rs.isAfterLast());

                int i = 0;
                while (rs.next()) {
                    if (rs.getString(1).equalsIgnoreCase("Jay")) {
                        Assertions.assertEquals("Jay", rs.getString(1));
                        Assertions.assertEquals("Miner", rs.getString(2));
                        ++i;
                    } else if (rs.getString(1).equalsIgnoreCase("Rocky")) {
                        Assertions.assertEquals("Rocky", rs.getString(1));
                        Assertions.assertEquals("Balboa", rs.getString(2));
                        ++i;
                    } else
                        Assertions.fail("Unknown value");
                }

                Assertions.assertEquals(2, i);

                rs.close();
            }
        }
    }

    @Test
    @Disabled
    public void queryTransaction() throws Exception {
        try (final Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("create vertex type V");
                st.execute("create vertex V set name = 'Jay', lastName = 'Miner'");

                PreparedStatement pst = conn.prepareStatement("create vertex V set name = ?, lastName = ?");
                pst.setString(1, "Rocky");
                pst.setString(2, "Balboa");
                pst.execute();
                pst.close();

                ResultSet rs = st.executeQuery("SELECT * FROM V");

                Assertions.assertTrue(!rs.isAfterLast());

                int i = 0;
                while (rs.next()) {
                    if (rs.getString(1).equalsIgnoreCase("Jay")) {
                        Assertions.assertEquals("Jay", rs.getString(1));
                        Assertions.assertEquals("Miner", rs.getString(2));
                        ++i;
                    } else if (rs.getString(1).equalsIgnoreCase("Rocky")) {
                        Assertions.assertEquals("Rocky", rs.getString(1));
                        Assertions.assertEquals("Balboa", rs.getString(2));
                        ++i;
                    } else
                        Assertions.fail("Unknown value");
                }

                Assertions.assertEquals(2, i);

                rs.close();
            }
        }
    }

    @Test
    @Disabled
    public void testWaitForConnectionFromExternal() throws InterruptedException {
        Thread.sleep(1000000);
    }

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost/" + getDatabaseName();
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
        props.setProperty("ssl", "false");
        Connection conn = DriverManager.getConnection(url, props);
        return conn;
    }

    protected String getDatabaseName() {
        return "postgresdb";
    }
}
