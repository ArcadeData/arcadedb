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

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11957: {@code out()}, {@code outE()} and {@code in()} answer a lazy engine iterable, which the Postgres wire typed as
 * a json column and then sent as the iterable's identity string ({@code com.arcadedb.graph.GraphEngine$10@72d8f43f}), so a
 * client decoding the json column (psycopg does it for every row) failed the whole fetch. The column now holds the records
 * as a JSON array, the neighbours HTTP and gRPC send.
 */
class Issue11957GraphTraversalColumnIT extends PostgresWireProtocolTestBase {
  private static final String QUERY = "SELECT k, out('E') AS o, outE('E') AS oe, in() AS i FROM V WHERE k = 1";

  private RID target;
  private RID edge;

  @BeforeEach
  void createGraph() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });
    database.transaction(() -> {
      final MutableVertex source = database.newVertex("V").set("k", 1).save();
      final MutableVertex destination = database.newVertex("V").set("k", 2).save();
      target = destination.getIdentity();
      edge = source.newEdge("E", destination).set("w", 1.5).save().getIdentity();
    });
  }

  @Test
  void simpleQuerySendsTheNeighboursAsJson() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("preferQueryMode", "simple");
    try (final Connection connection = connect(properties); final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery(QUERY)) {
      assertNeighboursAsJson(rs);
    }
  }

  @Test
  void extendedQuerySendsTheNeighboursAsJson() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("prepareThreshold", "0");
    try (final Connection connection = connect(properties); final PreparedStatement statement = connection.prepareStatement(QUERY)) {
      // twice: pgjdbc switches to a named statement on a later execution
      for (int i = 0; i < 2; i++)
        try (final ResultSet rs = statement.executeQuery()) {
          assertNeighboursAsJson(rs);
        }
    }
  }

  /** A server-prepared statement in binary: the values never carry the identity string of the iterable. */
  @Test
  void serverPreparedBinaryQueryNeverSendsTheIterableIdentity() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("prepareThreshold", "-1");
    try (final Connection connection = connect(properties); final PreparedStatement statement = connection.prepareStatement(QUERY)) {
      for (int i = 0; i < 2; i++)
        try (final ResultSet rs = statement.executeQuery()) {
          assertThat(rs.next()).isTrue();
          for (final String column : new String[] { "o", "oe", "i" })
            assertThat(rs.getString(column)).as(column).doesNotContain("GraphEngine");
          assertThat(rs.getString("o")).contains(target.toString());
          assertThat(rs.getString("oe")).contains(edge.toString());
        }
    }
  }

  private void assertNeighboursAsJson(final ResultSet rs) throws Exception {
    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt("k")).isEqualTo(1);

    // what a json-decoding client does with the column: it must parse
    assertThat(rs.getMetaData().getColumnTypeName(rs.findColumn("o"))).isEqualTo("json");
    final JSONArray neighbours = new JSONArray(rs.getString("o"));
    assertThat(neighbours.length()).isEqualTo(1);
    final JSONObject neighbour = neighbours.getJSONObject(0);
    assertThat(neighbour.getString("@rid")).isEqualTo(target.toString());
    assertThat(neighbour.getInt("k")).isEqualTo(2);

    assertThat(rs.getMetaData().getColumnTypeName(rs.findColumn("oe"))).isEqualTo("json");
    final JSONArray edges = new JSONArray(rs.getString("oe"));
    assertThat(edges.length()).isEqualTo(1);
    assertThat(edges.getJSONObject(0).getString("@rid")).isEqualTo(edge.toString());
    assertThat(edges.getJSONObject(0).getDouble("w")).isEqualTo(1.5);

    // no incoming edge: an empty array, not the identity string of an empty iterable
    assertThat(rs.getString("i")).doesNotContain("GraphEngine");
    assertThat((Object[]) rs.getArray("i").getArray()).isEmpty();

    assertThat(rs.next()).isFalse();
  }

  private Connection connect(final Properties properties) throws Exception {
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("sslmode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
