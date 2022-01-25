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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.e2e;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcQueriesTest extends ArcadeContainerTemplate {

    private Connection conn;

    @BeforeAll
    static void beforeAll() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
    }

    @BeforeEach
    void setUp() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "playwithdata");
        props.setProperty("ssl", "false");

        conn = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + pgsqlPort + "/beer", props);
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    void simpleSQLQuery() throws Exception {

        try (Statement st = conn.createStatement()) {

            try (java.sql.ResultSet rs = st.executeQuery("SELECT * FROM Beer limit 1")) {
                assertThat(rs.next()).isTrue();

                assertThat(rs.getString("name")).isNotBlank();

                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    void simpleGremlinQuery() throws Exception {

        try (Statement st = conn.createStatement()) {

            try (java.sql.ResultSet rs = st.executeQuery("{gremlin}g.V().limit(1)")) {
                assertThat(rs.next()).isTrue();

                assertThat(rs.getString("name")).isNotBlank();

                assertThat(rs.next()).isFalse();
            }
        }
    }
    @Test
    void simpleCypherQuery() throws Exception {

        try (Statement st = conn.createStatement()) {

            try (java.sql.ResultSet rs = st.executeQuery("{cypher}MATCH(p:Beer) RETURN * LIMIT 1")) {
                assertThat(rs.next()).isTrue();

                assertThat(rs.getString("name")).isNotBlank();

                assertThat(rs.next()).isFalse();
            }
        }
    }
}
