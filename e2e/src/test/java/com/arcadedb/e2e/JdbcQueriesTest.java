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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
          CREATE DOCUMENT TYPE comment IF NOT EXISTS;
          CREATE PROPERTY comment.created IF NOT EXISTS DATETIME;
          CREATE PROPERTY comment.content IF NOT EXISTS STRING;


          CREATE DOCUMENT TYPE location IF NOT EXISTS;
          CREATE PROPERTY location.name IF NOT EXISTS STRING;
          CREATE PROPERTY location.timezone IF NOT EXISTS STRING;

          CREATE VERTEX TYPE article IF NOT EXISTS BUCKETS 8;
          CREATE PROPERTY article.id IF NOT EXISTS LONG;
          CREATE PROPERTY article.created IF NOT EXISTS DATETIME;
          CREATE PROPERTY article.updated IF NOT EXISTS DATETIME;
          CREATE PROPERTY article.title IF NOT EXISTS STRING;
          CREATE PROPERTY article.content IF NOT EXISTS STRING;
          CREATE PROPERTY article.author IF NOT EXISTS STRING;
          CREATE PROPERTY article.tags IF NOT EXISTS LIST OF STRING;
          CREATE PROPERTY article.comment IF NOT EXISTS LIST OF comment;
          CREATE PROPERTY article.location IF NOT EXISTS EMBEDDED OF location;

          CREATE INDEX IF NOT EXISTS on article(id) UNIQUE;
          """);

      st.execute("""
          {sqlscript}
          INSERT INTO article CONTENT {
                  "id": 1,
                  "created": "2021-01-01 00:00:00",
                  "updated": "2021-01-01 00:00:00",
                  "title": "My first article",
                  "content": "This is the content of my first article",
                  "author": "John Doe",
                  "tags": ["tag1", "tag2"],
                  "comment": [{
                    "@type": "comment",
                    "content": "This is a comment",
                    "created": "2021-01-01 00:00:00"
                    },
                    {
                    "@type": "comment",
                    "content": "This is a comment 2",
                    "created": "2021-01-01 00:00:00"
                    }],
                  "location": {
                    "@type": "location",
                    "name": "My location",
                    "timezone": "UTC"
                    }
                  };
          INSERT INTO article CONTENT {
                  "id": 2,
                  "created": "2021-01-02 00:00:00",
                  "updated": "2021-01-02 00:00:00",
                  "title": "My second article",
                  "content": "This is the content of my second article",
                  "author": "John Doe",
                  "tags": ["tag1", "tag3", "tag4"]
                  };
          INSERT INTO article CONTENT {
                  "id": 3,
                  "created": "2021-01-03 00:00:00",
                  "updated": "2021-01-03 00:00:00",
                  "title": "My third article",
                  "content": "This is the content of my third article",
                  "author": "John Doe",
                  "tags": ["tag2", "tag3"]
                  };
          """);
    }

    try (final Statement st = conn.createStatement()) {
      try (final ResultSet rs = st.executeQuery("SELECT * FROM article")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("title")).isEqualTo("My first article");
        //comments is an array of embedded docs on first row
        ResultSet comments = rs.getArray("comment").getResultSet();
        assertThat(comments.next()).isTrue();
        assertThat(new JSONObject(comments.getString("value")).getString("content")).isEqualTo("This is a comment");
        //location is an embedded doc
        assertThat(rs.getString("location")).isNotNull();
        assertThat(new JSONObject(rs.getString("location")).getString("name")).contains("My location");
        assertThat(new JSONObject(rs.getString("location")).getString("timezone")).contains("UTC");

        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("title")).isEqualTo("My second article");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("title")).isEqualTo("My third article");
        assertThat(rs.next()).isFalse();
      }

    }

  }

  @Test
  void testSelectSchemaTypes() throws SQLException, ClassNotFoundException {
      try (final Statement st = conn.createStatement()) {

        final ResultSet rs = st.executeQuery("{sql}select from schema:types");
        while (rs.next()) {
          if (rs.getArray("properties").getResultSet().next()) {
            ResultSet props = rs.getArray("properties").getResultSet();
            assertThat(props.next()).isTrue();
            assertThat(new JSONObject(props.getString("value")).getString("type")).isEqualTo("INTEGER");
          }
        }

    }
  }

  //use this to test the result set
  private static void printResultSet(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    // Print header
    StringBuilder header = new StringBuilder();
    StringBuilder separator = new StringBuilder();
    for (int i = 1; i <= columnCount; i++) {
      String columnName = metaData.getColumnName(i);
      header.append(String.format("| %-20s ", columnName));
      separator.append("+----------------------");
    }
    header.append("|");
    separator.append("+");

    System.out.println(separator);
    System.out.println(header);
    System.out.println(separator);

    // Print rows
    while (rs.next()) {
      StringBuilder row = new StringBuilder();
      for (int i = 1; i <= columnCount; i++) {
        String value = rs.getString(i);
        value = value != null ? value : "null";
        if (value.length() > 20)
          value = value.substring(0, 17) + "...";
        row.append(String.format("| %-20s ", value));
      }
      row.append("|");
      System.out.println(row);
    }

    System.out.println(separator);
  }
}
