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
package com.arcadedb.postgres;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.util.PSQLException;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.data.Offset.offset;

public class PostgresWJdbcIT extends BaseGraphServerTest {
  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "Postgres:com.arcadedb.postgres.PostgresProtocolPlugin,GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false"); // SET THIS TO TRUE TO DEBUG THE PROTOCOL
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");

    super.endTest();
  }

  @Test
  void backupDatabase() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        ResultSet backupDatabase = st.executeQuery("{sql}BACKUP DATABASE");
        while (backupDatabase.next()) {
          assertThat(backupDatabase.getString("result")).isEqualTo("OK");
          assertThat(backupDatabase.getString("backupFile")).startsWith("postgresdb-backup-");
        }
      }
      try (var st = conn.createStatement()) {
        ResultSet backupDatabase = st.executeQuery("{sqlscript}BACKUP DATABASE");
        while (backupDatabase.next()) {
          assertThat(backupDatabase.getString("result")).isEqualTo("OK");
          assertThat(backupDatabase.getString("backupFile")).startsWith("postgresdb-backup-");
        }
      }
    }
  }

  @Test
  void typeNotExistsErrorManagement() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        assertThatThrownBy(() -> st.executeQuery("SELECT * FROM V"))
            .isInstanceOf(PSQLException.class);
      }
    }
  }

  @Test
  void parsingErrorMgmt() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        assertThatThrownBy(() -> st.executeQuery("SELECT 'abc \\u30 def';"))
            .isInstanceOf(PSQLException.class)
            .hasMessageContaining("Syntax error");
      }
    }
  }

  @Test
  void gremlinQuery() throws Exception {
    try (var conn = getConnection()) {
      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("create vertex type V");
        for (int i = 0; i < 11; i++) {
          st.execute("create vertex V set id = " + i + ", name = 'Jay', lastName = 'Miner'");
        }

        var rs = st.executeQuery("{gremlin}g.V().hasLabel('V').order().by('id').limit(10)");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isEqualTo(0);
        assertThat(rs.getString("name")).isEqualTo("Jay");
        assertThat(rs.getString("lastName")).isEqualTo("Miner");
      }
    }
  }

  @Test
  void selectSchemaTypes() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {

        var rs = st.executeQuery("{sql}select from schema:types");
        while (rs.next()) {
          if (rs.getArray("properties").getResultSet().next()) {
            ResultSet props = rs.getArray("properties").getResultSet();
            assertThat(props.next()).isTrue();
            assertThat(new JSONObject(props.getString("value")).getString("type")).isEqualTo("LONG");
          }
        }

      }
    }
  }

  @Test
  void script() throws Exception {
    try (var conn = getConnection()) {
      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("""
            {sqlscript}create vertex type V IF NOT EXISTS;
            create vertex V set id = 0, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 1, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 2, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 3, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 4, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 5, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 6, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 7, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 8, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 9, name = 'Jay', lastName = 'Miner';
            create vertex V set id = 10, name = 'Jay', lastName = 'Miner';
            """);

        var rs = st.executeQuery("{gremlin}g.V().hasLabel('V').order().by('id').limit(10)");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isEqualTo(0);
        assertThat(rs.getString("name")).isEqualTo("Jay");
        assertThat(rs.getString("lastName")).isEqualTo("Miner");
      }
    }
  }

  @Test
  void queryVertices() throws Exception {
    final int TOTAL = 1;
    final long now = System.currentTimeMillis();

    try (final Connection conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("create vertex type V");
        for (int i = 0; i < TOTAL; i++) {
          st.execute("create vertex V set id = " + i + ", name = 'Jay', lastName = 'Miner'");
        }

        var pst = conn.prepareStatement(
            """
                create vertex V set name = ?,
                lastName = ?,
                short = ?,
                int = ?,
                long = ?,
                float = ?,
                double = ?,
                boolean = ?,
                date = ?,
                timestamp = ?
                """);
        pst.setString(1, "Rocky");
        pst.setString(2, "Balboa");
        pst.setShort(3, (short) 3);
        pst.setInt(4, 4);
        pst.setLong(5, 5L);
        pst.setFloat(6, 6F);
        pst.setDouble(7, 7D);
        pst.setBoolean(8, false);
        pst.setDate(9, new Date(now));
        pst.setTimestamp(10, new Timestamp(now));

        pst.execute();
        pst.close();

        ResultSet rs = st.executeQuery(
            "SELECT name, lastName, short, int, long, float, double, boolean, date, timestamp FROM V order by id");

        assertThat(rs.isAfterLast()).isFalse();

        int i = 0;
        while (rs.next()) {
          if (rs.getString(1).equalsIgnoreCase("Jay")) {
            assertThat(rs.getString(1)).isEqualTo("Jay");
            assertThat(rs.getString(2)).isEqualTo("Miner");
            ++i;
          } else if (rs.getString(1).equalsIgnoreCase("Rocky")) {
            assertThat(rs.getString(2)).isEqualTo("Balboa");
            assertThat(rs.getShort(3)).isEqualTo((short) 3);
            assertThat(rs.getInt(4)).isEqualTo(4);
            assertThat(rs.getLong(5)).isEqualTo(5L);
            assertThat(rs.getFloat(6)).isEqualTo(6F);
            assertThat(rs.getDouble(7)).isEqualTo(7D);
            assertThat(rs.getBoolean(8)).isFalse();
            assertThat(rs.getDate(9).toLocalDate()).isEqualTo(LocalDate.now());
            assertThat(rs.getTimestamp(10).getTime()).isEqualTo(now);
            ++i;
          } else
            fail("Unknown value");
        }

        assertThat(i).isEqualTo(TOTAL + 1);

        rs.close();

        rs = st.executeQuery("SELECT FROM V order by id");

        assertThat(rs.isAfterLast()).isFalse();

        i = 0;
        while (rs.next()) {
          assertThat(rs.findColumn(RID_PROPERTY) > -1).isTrue();
          assertThat(rs.findColumn(TYPE_PROPERTY) > -1).isTrue();
          assertThat(rs.findColumn(CAT_PROPERTY) > -1).isTrue();

          assertThat(rs.getString(rs.findColumn(RID_PROPERTY)).startsWith("#")).isTrue();
          assertThat(rs.getString(rs.findColumn(TYPE_PROPERTY))).isEqualTo("V");
          assertThat(rs.getString(rs.findColumn(CAT_PROPERTY))).isEqualTo("v");

          ++i;
        }

        assertThat(i).isEqualTo(TOTAL + 1);

      }
    }
  }

  @Test
  void queryTransaction() throws Exception {
    try (final Connection conn = getConnection()) {
      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("begin");
        st.execute("create vertex type V");
        st.execute("create vertex V set name = 'Jay', lastName = 'Miner'");

        final PreparedStatement pst = conn.prepareStatement("create vertex V set name = ?, lastName = ?");
        pst.setString(1, "Rocky");
        pst.setString(2, "Balboa");
        pst.execute();
        pst.close();

        var rs = st.executeQuery("SELECT * FROM V");

        assertThat(rs.isAfterLast()).isFalse();

        int i = 0;
        while (rs.next()) {
          if (rs.getString(1).equalsIgnoreCase("Jay")) {
            assertThat(rs.getString(1)).isEqualTo("Jay");
            assertThat(rs.getString(2)).isEqualTo("Miner");
            ++i;
          } else if (rs.getString(1).equalsIgnoreCase("Rocky")) {
            assertThat(rs.getString(1)).isEqualTo("Rocky");
            assertThat(rs.getString(2)).isEqualTo("Balboa");
            ++i;
          } else
            fail("Unknown value");
        }

        st.execute("commit");

        assertThat(i).isEqualTo(2);

        rs.close();
      }
    }
  }

  @Test
  void cypher() throws Exception {
    try (final Connection conn = getConnection()) {
      conn.setAutoCommit(false);

      try (var st = conn.createStatement()) {
        st.execute("CREATE VERTEX TYPE PersonVertex;");

        for (int i = 0; i < 100; i++) {
          st.execute("{cypher} MATCH (n) DETACH DELETE n;");
          st.execute("{cypher} CREATE (james:PersonVertex {name: \"James\", height: 1.9});");
          st.execute("{cypher} CREATE (henry:PersonVertex {name: \"Henry\"});");

          var rs = st.executeQuery("{cypher} MATCH (person:PersonVertex) RETURN person.name, person.height;");

          int numberOfPeople = 0;
          while (rs.next()) {
            assertThat(rs.getString(1)).isNotNull();

            if (rs.getString(1).equals("James"))
              assertThat(rs.getFloat(2)).isEqualTo(1.9F);
            else if (rs.getString(1).equals("Henry"))
              assertThat(rs.getString(2)).isNull();
            else
              fail("");

            ++numberOfPeople;
          }

          assertThat(numberOfPeople).isEqualTo(2);
          st.execute("commit");
        }
      }
    }
  }

  /**
   * Issue <a href="https://github.com/ArcadeData/arcadedb/issues/1325#issuecomment-1816145926">...</a>
   *
   * @throws Exception
   */
  @Test
  void showTxIsolationLevel() throws Exception {
    try (final Connection conn = getConnection()) {
      try (var st = conn.createStatement()) {
        try (var rs = st.executeQuery("SHOW TRANSACTION ISOLATION LEVEL")) {
          assertThat(rs.next()).isTrue();
        }
      }
    }
  }

  @Test
  void isoDateFormat() throws Exception {
    try (final Connection conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("SET datestyle TO 'ISO'");
      }
    }
  }

  @Test
  @Disabled
  void waitForConnectionFromExternal() throws Exception {
    Thread.sleep(1000000);
  }

  private Connection getConnection() throws ClassNotFoundException, SQLException {
    Class.forName("org.postgresql.Driver");

    var url = "jdbc:postgresql://localhost/" + getDatabaseName();
    var props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    return DriverManager.getConnection(url, props);
  }

  protected String getDatabaseName() {
    return "postgresdb";
  }

  @Test
  void createSchemaWithSqlScript() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {

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
      try (var st = conn.createStatement()) {
        try (var rs = st.executeQuery("SELECT * FROM article")) {
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
  }

  private static final int    DEFAULT_SIZE = 64;
  private static final Random RANDOM       = new Random();

  private static List<?> randomValues(Class<?> type) {
    if (type == Boolean.class) {
      return IntStream.range(0, PostgresWJdbcIT.DEFAULT_SIZE)
          .mapToObj(i -> RANDOM.nextBoolean())
          .toList();
    } else if (type == Double.class) {
      return IntStream.range(0, PostgresWJdbcIT.DEFAULT_SIZE)
          .mapToObj(i -> RANDOM.nextDouble() * 200 - 100)
          .toList();
    } else if (type == Float.class) {
      return IntStream.range(0, PostgresWJdbcIT.DEFAULT_SIZE)
          .mapToObj(i -> RANDOM.nextFloat())
          .toList();
    } else if (type == Integer.class) {
      return IntStream.range(0, PostgresWJdbcIT.DEFAULT_SIZE)
          .mapToObj(i -> RANDOM.nextInt(201) - 100)
          .toList();
    } else if (type == String.class) {
      return IntStream.range(0, PostgresWJdbcIT.DEFAULT_SIZE)
          .mapToObj(i -> generateRandomString())
          .toList();
    } else {
      throw new IllegalArgumentException("Unsupported type: " + type.getName());
    }
  }

  private static String generateRandomString() {
    int length = RANDOM.nextInt(11) + 5; // 5 to 15
    String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int index = RANDOM.nextInt(chars.length());
      sb.append(chars.charAt(index));
    }
    return sb.toString();
  }

  @ParameterizedTest
  @ValueSource(classes = { Boolean.class, Float.class, Integer.class, String.class })
  void returnArray(Class<?> typeToTest) throws Exception {
    try (var conn = getConnection()) {
      conn.setAutoCommit(true);

      String typeName = typeToTest.getSimpleName();
      String arcadeName = "TEXT_" + typeName;

      try (var st = conn.createStatement()) {
        st.execute("CREATE VERTEX TYPE `" + arcadeName + "` IF NOT EXISTS");
        st.execute("CREATE PROPERTY " + arcadeName + ".str IF NOT EXISTS STRING");
        st.execute("CREATE PROPERTY " + arcadeName + ".data IF NOT EXISTS LIST");

        List<?> randomData = randomValues(typeToTest);
        JSONArray jsonArray = new JSONArray(randomData);

        try (ResultSet rs = st.executeQuery(
            "INSERT INTO `" + arcadeName + "` SET str = \"meow\", data = " +
                jsonArray + " RETURN data")) {
        }

        try (ResultSet rs = st.executeQuery(
            "SELECT data FROM `" + arcadeName + "` WHERE str = \"meow\"")) {
          assertThat(rs.next()).isTrue();

          Array dataArray = rs.getArray("data");
          Object[] dataValues = (Object[]) dataArray.getArray();

          // Check if it's a list (array in Java)
          assertThat(dataValues)
              .as("For " + typeName + ": Type LIST is returned as null")
              .isNotNull();
          // Check if all items are of the expected type
          for (Object item : dataValues) {
            assertThat(item.getClass().getTypeName())//.isInstance(item)
                .as("For " + typeName + ": Not all items are of type " + typeName)
                .isEqualTo(typeToTest.getTypeName());
          }
        }
      }
    }
  }

  @Test
  void floatMapping() throws Exception {
    try (Connection conn = getConnection()) {
      Statement stmt = conn.createStatement();

      stmt.execute("CREATE DOCUMENT TYPE TestProduct");

      stmt.execute("INSERT INTO TestProduct (name, price) VALUES ('TestItem', 29.99)");

      ResultSet rs = stmt.executeQuery("SELECT * FROM TestProduct");

      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("TestItem");
      assertThat(rs.getFloat("price")).isEqualTo(29.99f);

    }
  }

  @Test
  void nullValuesMapping() throws Exception {
    try (var conn = getConnection()) {
      var stmt = conn.createStatement();

      stmt.execute("""
          {sqlscript}
          CREATE DOCUMENT TYPE TestProduct;
          CREATE PROPERTY TestProduct.id INTEGER;
          CREATE PROPERTY TestProduct.name STRING;
          CREATE PROPERTY TestProduct.price FLOAT;
          CREATE INDEX ON TestProduct(id) UNIQUE;

          INSERT INTO TestProduct (id, name, price) VALUES (1, 'TestItem', 29.99);
          INSERT INTO TestProduct (id, price) VALUES (2, 38.99);
          """);

      ResultSet rs = stmt.executeQuery("SELECT  FROM TestProduct ");

      while (rs.next()) {
        int id = rs.getInt("id");
        String name = rs.getString("name");
        Double price = rs.getDouble("price");

        if (id == 1) {
          assertThat(name).isEqualTo("TestItem");
          assertThat(price).isEqualTo(29.99);
        } else if (id == 2) {
          assertThat(name).isNull();
          assertThat(price).isEqualTo(38.99);
        }
      }

    }
  }

  @Test
  void createVertexCypherQueryParams() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("create vertex type City");
      }

      try (var pst = conn.prepareStatement("create vertex City set id = ? ;")) {
        pst.setString(1, "C1");
        pst.execute();
      }

      try (var st = conn.prepareStatement("{cypher} CREATE (n:City {id: ? }) RETURN n")) {
        st.setString(1, "C2");
        boolean execute = st.execute();
        assertThat(execute).isTrue();

      }
      try (var st = conn.prepareStatement("{cypher} MATCH (n:City) WHERE n.id = ? RETURN n")) {
        st.setString(1, "C2");
        try (var rs = st.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString("id")).isEqualTo("C2");
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Disabled("Pending fix verification")
  @Test
  void cypherWithArrayParameterInClause() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("create vertex type CHUNK");
        // Create test vertices directly
        st.execute("{cypher} CREATE (n:CHUNK {text: 'chunk1'})");
        st.execute("{cypher} CREATE (n:CHUNK {text: 'chunk2'})");
        st.execute("{cypher} CREATE (n:CHUNK {text: 'chunk3'})");
      }

      // Get all RIDs
      String[] rids = new String[3];
      try (var st = conn.createStatement()) {
        try (var rs = st.executeQuery("{cypher} MATCH (n:CHUNK) RETURN ID(n) ORDER BY n.text")) {
          int idx = 0;
          while (rs.next() && idx < 3) {
            rids[idx++] = rs.getString(1);
          }
        }
      }

      // Now query with IN clause using array parameter - this should reproduce the ClassCastException
      try (var pst = conn.prepareStatement("{cypher} MATCH (n:CHUNK) WHERE ID(n) IN ? RETURN n.text as text ORDER BY n.text")) {
        Array array = conn.createArrayOf("text", rids);
        pst.setArray(1, array);

        try (var rs = pst.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString("text")).isEqualTo("chunk1");
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString("text")).isEqualTo("chunk2");
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString("text")).isEqualTo("chunk3");
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  void floatArrayPropertyRoundTrip() throws SQLException, ClassNotFoundException {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("create vertex type `TEXT_EMBEDDING` if not exists;");
        st.execute("create property TEXT_EMBEDDING.str if not exists STRING;");
        st.execute("create property TEXT_EMBEDDING.embedding if not exists LIST;");

        // Use explicit float casting to ensure values are stored as floats, not doubles
        st.execute("INSERT INTO `TEXT_EMBEDDING` SET str = \"meow\", embedding = [0.1f, 0.2f, 0.3f]");
        ResultSet resultSet = st.executeQuery("SELECT embedding FROM `TEXT_EMBEDDING` WHERE str = \"meow\"");

        assertThat(resultSet.next()).isTrue();
        Array embeddingArray = resultSet.getArray("embedding");
        assertThat(embeddingArray).isNotNull();

        Object[] embeddings = (Object[]) embeddingArray.getArray();
        assertThat(embeddings).isNotNull();
        assertThat(embeddings).hasSize(3);

        // Verify all elements are Float instances (not Double)
        for (Object item : embeddings) {
          assertThat(item).isInstanceOf(Float.class);
        }

        // Verify the actual values
        assertThat((Float) embeddings[0]).isEqualTo(0.1f, offset(0.0001f));
        assertThat((Float) embeddings[1]).isEqualTo(0.2f, offset(0.0001f));
        assertThat((Float) embeddings[2]).isEqualTo(0.3f, offset(0.0001f));
      }
    }
  }

  @Test
  void arrayOfFloatsPropertyRoundTrip() throws SQLException, ClassNotFoundException {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("create vertex type `TEXT_EMBEDDING_2` if not exists;");
        st.execute("create property TEXT_EMBEDDING_2.str if not exists STRING;");
        st.execute("create property TEXT_EMBEDDING_2.embedding if not exists ARRAY_OF_FLOATS;");

        // Test INSERT with RETURN - this matches the Python e2e test scenario
        ResultSet resultSet = st.executeQuery("INSERT INTO `TEXT_EMBEDDING_2` SET str = \"meow\", embedding = [0.1,0.2,0.3] RETURN embedding");

        assertThat(resultSet.next()).isTrue();
        Array embeddingArray = resultSet.getArray("embedding");
        assertThat(embeddingArray).isNotNull();

        Object[] embeddings = (Object[]) embeddingArray.getArray();
        assertThat(embeddings).isNotNull();
        assertThat(embeddings).hasSize(3);

        // Verify all elements are Float instances (not Double)
        for (Object item : embeddings) {
          assertThat(item).isInstanceOf(Float.class);
        }

        // Verify the actual values
        assertThat((Float) embeddings[0]).isEqualTo(0.1f, offset(0.0001f));
        assertThat((Float) embeddings[1]).isEqualTo(0.2f, offset(0.0001f));
        assertThat((Float) embeddings[2]).isEqualTo(0.3f, offset(0.0001f));

        // Also test regular SELECT query
        resultSet = st.executeQuery("SELECT embedding FROM `TEXT_EMBEDDING_2` WHERE str = \"meow\"");
        assertThat(resultSet.next()).isTrue();
        embeddingArray = resultSet.getArray("embedding");
        assertThat(embeddingArray).isNotNull();

        embeddings = (Object[]) embeddingArray.getArray();
        assertThat(embeddings).hasSize(3);
        assertThat((Float) embeddings[0]).isEqualTo(0.1f, offset(0.0001f));
      }
    }
  }
}
