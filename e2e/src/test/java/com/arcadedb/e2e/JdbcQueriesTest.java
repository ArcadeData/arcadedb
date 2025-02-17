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
import java.sql.ResultSet;
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
    final Properties props = new Properties();
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

    try (final Statement st = conn.createStatement()) {

      try (final ResultSet rs = st.executeQuery("SELECT * FROM Beer limit 1")) {
        assertThat(rs.next()).isTrue();

        assertThat(rs.getString("name")).isNotBlank();

        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Test
  void bigResultSetSQLQuery() throws Exception {

    try (final Statement st = conn.createStatement()) {

      try (final ResultSet rs = st.executeQuery("SELECT * FROM Beer limit -1")) {
        while (rs.next()) {
          assertThat(rs.getString("name")).isNotBlank();
        }
      }
    }
  }

  @Test
  void simpleGremlinQuery() throws Exception {
    try (final Statement st = conn.createStatement()) {

      try (final ResultSet rs = st.executeQuery("{gremlin}g.V().hasLabel('Beer').limit(1)")) {
        assertThat(rs.next()).isTrue();

        assertThat(rs.getString("name")).isNotBlank();

        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Test
  void simpleCypherQuery() throws Exception {
    try (final Statement st = conn.createStatement()) {

      try (final ResultSet rs = st.executeQuery("{cypher}MATCH(p:Beer) RETURN * LIMIT 1")) {
        assertThat(rs.next()).isTrue();

        assertThat(rs.getString("name")).isNotBlank();

        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Test
  void createVertexCypherQuery() throws Exception {
    try (final Statement st = conn.createStatement()) {

      try (final ResultSet rs = st.executeQuery("{cypher} CREATE (n:City {id:'C1'}) RETURN n")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("id")).isEqualTo("C1");
        assertThat(rs.next()).isFalse();
      }

      try (final ResultSet rs = st.executeQuery("{cypher} MATCH (n:City) WHERE n.id = 'C1' RETURN n")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("id")).isEqualTo("C1");
        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Test
  void createSchemaWithSqlScript() throws SQLException {
    try (final Statement st = conn.createStatement()) {

      st.execute("""
          {sqlscript}
          CREATE DOCUMENT TYPE article IF NOT EXISTS;
          CREATE PROPERTY article.created IF NOT EXISTS DATETIME;
          CREATE PROPERTY article.updated IF NOT EXISTS DATETIME;
          CREATE PROPERTY article.title IF NOT EXISTS STRING;
          CREATE PROPERTY article.content IF NOT EXISTS STRING;
          CREATE PROPERTY article.author IF NOT EXISTS STRING;
          CREATE PROPERTY article.notes IF NOT EXISTS EMBEDDED;

          INSERT INTO article CONTENT {"created": "2021-01-01 00:00:00",
                  "updated": "2021-01-01 00:00:00",
                  "title": "My first article",
                  "content": "This is the content of my first article",
                  "author": "John Doe"};
          INSERT INTO article CONTENT {"created": "2021-01-02 00:00:00",
                  "updated": "2021-01-02 00:00:00",
                  "title": "My second article",
                  "content": "This is the content of my second article",
                  "author": "John Doe"};
          INSERT INTO article CONTENT {"created": "2021-01-03 00:00:00",
                  "updated": "2021-01-03 00:00:00",
                  "title": "My third article",
                  "content": "This is the content of my third article",
                  "author": "John Doe"};
          """);
    }

    try (final Statement st = conn.createStatement()) {
      try (final ResultSet rs = st.executeQuery("SELECT * FROM article")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("title")).isEqualTo("My first article");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("title")).isEqualTo("My second article");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("title")).isEqualTo("My third article");
        assertThat(rs.next()).isFalse();
      }

    }
  }
}
