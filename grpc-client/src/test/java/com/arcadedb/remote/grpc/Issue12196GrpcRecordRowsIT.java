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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12196: over gRPC, Cypher {@code MATCH (n) RETURN n} answered a vertex whose only property was {@code n}, and a column
 * that holds a record ({@code RETURN a, r, b}, SQL {@code @this}) reached the caller as a raw map. A row returning a node reads as
 * over HTTP, the node with its own properties, and a row with record columns reads as with the embedded API, records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue12196GrpcRecordRowsIT extends BaseGrpcClientServerTest {

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private RemoteDatabase     http;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    http = new RemoteDatabase("localhost", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    http.command("sql", "CREATE VERTEX TYPE P12196");
    http.command("sql", "CREATE EDGE TYPE K12196");
    http.command("opencypher", "CREATE (:P12196 {name: 'a', age: 1})-[:K12196 {since: 2020}]->(:P12196 {name: 'b', age: 2})");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (http != null)
      http.close();
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void cypherReturnOfANodeIsTheNode() {
    final String query = "MATCH (n:P12196) RETURN n ORDER BY n.name";
    final List<String> rows = rows(http.query("opencypher", query));
    assertThat(rows).hasSize(2);
    assertThat(rows.getFirst()).contains("name=a").contains("age=1");
    assertThat(rows(grpc.query("opencypher", query))).isEqualTo(rows);
  }

  @Test
  void cypherReturnOfARelationshipIsTheRelationship() {
    final String query = "MATCH (:P12196)-[r:K12196]->(:P12196) RETURN r";
    final List<String> rows = rows(http.query("opencypher", query));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).contains("since=2020");
    assertThat(rows(grpc.query("opencypher", query))).isEqualTo(rows);

    try (final ResultSet rs = grpc.query("opencypher", query)) {
      final Edge edge = rs.next().getEdge().orElseThrow();
      assertThat(edge.getTypeName()).isEqualTo("K12196");
      assertThat(edge.getOutVertex().getString("name")).isEqualTo("a");
      assertThat(edge.getInVertex().getString("name")).isEqualTo("b");
    }
  }

  @Test
  void cypherColumnsHoldingElementsAreRecords() {
    // THE HTTP CLIENT HAS ITS OWN WRONG SHAPE FOR THESE COLUMNS (#12184): THE EMBEDDED API IS THE REFERENCE
    final String query = "MATCH (a:P12196)-[r:K12196]->(b:P12196) RETURN a, r, b";
    final List<String> rows = rows(getServerDatabase(0, getDatabaseName()).query("opencypher", query));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).contains("a=record:").contains("r=record:").contains("b=record:");
    assertThat(rows(grpc.query("opencypher", query))).isEqualTo(rows);

    // the columns are the record classes, not their lookalike maps
    try (final ResultSet rs = grpc.query("opencypher", query)) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("a")).isInstanceOf(Vertex.class);
      assertThat(row.<Object>getProperty("b")).isInstanceOf(Vertex.class);
      assertThat(row.<Object>getProperty("r")).isInstanceOf(Edge.class);
      final Edge edge = row.getProperty("r");
      assertThat(edge.getOut()).isEqualTo(row.<Vertex>getProperty("a").getIdentity());
      assertThat(edge.getIn()).isEqualTo(row.<Vertex>getProperty("b").getIdentity());
      assertThat(row.<Vertex>getProperty("a").getString("name")).isEqualTo("a");
    }
  }

  @Test
  void cypherCollectedNodesAreAListOfRecords() {
    final String query = "MATCH (n:P12196) WITH n ORDER BY n.name RETURN collect(n) AS nodes";
    final List<String> rows = rows(getServerDatabase(0, getDatabaseName()).query("opencypher", query));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).contains("record:").contains("name=a").contains("name=b");
    assertThat(rows(grpc.query("opencypher", query))).isEqualTo(rows);

    try (final ResultSet rs = grpc.query("opencypher", query)) {
      final List<?> nodes = rs.next().getProperty("nodes");
      assertThat(nodes).hasSize(2).allSatisfy(node -> assertThat(node).isInstanceOf(Vertex.class));
      assertThat(((Vertex) nodes.getFirst()).getString("name")).isEqualTo("a");
    }
  }

  @Test
  void cypherNullColumnStaysNull() {
    final String query = "MATCH (n:P12196) OPTIONAL MATCH (n)-[:K12196]->(b:P12196) RETURN n, b ORDER BY n.name";
    final List<String> rows = rows(getServerDatabase(0, getDatabaseName()).query("opencypher", query));
    assertThat(rows).hasSize(2);
    assertThat(rows.getFirst()).contains("b=record:");
    assertThat(rows.getLast()).contains("b=null");
    assertThat(rows(grpc.query("opencypher", query))).isEqualTo(rows);

    try (final ResultSet rs = grpc.query("opencypher", query)) {
      rs.next();
      final Result second = rs.next();
      assertThat(second.<Object>getProperty("n")).isInstanceOf(Vertex.class);
      assertThat(second.hasProperty("b")).isTrue();
      assertThat(second.<Object>getProperty("b")).isNull();
    }
  }

  @Test
  void sqlThisColumnIsARecord() {
    final String query = "SELECT @rid, @type, @this FROM P12196 ORDER BY name";
    final List<String> rows = rows(getServerDatabase(0, getDatabaseName()).query("sql", query));
    assertThat(rows).hasSize(2);
    assertThat(rows.getFirst()).startsWith("projection{").contains("@this=record:");
    assertThat(rows(grpc.query("sql", query))).isEqualTo(rows);
  }

  @Test
  void aRecordColumnKeepsThePropertyOrderOfTheRecord() {
    // "p1".."p9" hash to consecutive buckets, so a re-hashed copy of the record lists them 1..9, not in the order they were set
    http.command("sql", "INSERT INTO P12196 SET name = 'c', p9 = 9, p8 = 8, p7 = 7, p6 = 6, p5 = 5, p4 = 4, p3 = 3, p2 = 2, p1 = 1");
    final String query = "MATCH (n:P12196 {name: 'c'}) RETURN n, 1 AS one";

    final List<String> embedded = propertyNamesOfColumn(getServerDatabase(0, getDatabaseName()).query("opencypher", query), "n");
    assertThat(embedded).startsWith("name", "p9", "p8");
    assertThat(propertyNamesOfColumn(grpc.query("opencypher", query), "n")).isEqualTo(embedded);
  }

  @Test
  void aProjectionRowListsItsColumnsInTheOrderTheyWereProjected() {
    // "z1".."z3" hash to consecutive buckets, so a row built on a HashMap lists them 1..3, not in the order they were projected
    final String query = "MATCH (n:P12196 {name: 'a'}) RETURN 3 AS z3, 2 AS z2, 1 AS z1, n";

    final List<String> embedded = columnNames(getServerDatabase(0, getDatabaseName()).query("opencypher", query));
    assertThat(embedded).startsWith("z3", "z2", "z1");
    assertThat(columnNames(grpc.query("opencypher", query))).isEqualTo(embedded);
  }

  @Test
  void streamedRowsAreTheSameShapes() {
    final String nodes = "MATCH (n:P12196) RETURN n ORDER BY n.name";
    assertThat(rows(grpc.queryStream("opencypher", nodes))).isEqualTo(rows(http.query("opencypher", nodes)));

    final String columns = "MATCH (a:P12196)-[r:K12196]->(b:P12196) RETURN a, r, b";
    assertThat(rows(grpc.queryStream("opencypher", columns)))
        .isEqualTo(rows(getServerDatabase(0, getDatabaseName()).query("opencypher", columns)));
  }

  @Test
  void mapEncodingNeverFailsAndKeepsWhatItCannotBuild() {
    // THE MAP ENCODING SENDS A RECORD'S @rid, @type AND PROPERTIES: AN EDGE HAS NO @out/@in IN IT, SO IT CANNOT BE BUILT AND STAYS
    // WHAT THE SERVER SENT, WHERE IT USED TO FAIL THE WHOLE QUERY
    final String query = "MATCH (a:P12196)-[r:K12196]->(b:P12196) RETURN a, r, b";
    try (final ResultSet rs = grpc.query("opencypher", query, new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_MAP, 0),
        Map.<String, Object>of())) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("a")).isInstanceOf(Vertex.class);
      assertThat(row.<Object>getProperty("b")).isInstanceOf(Vertex.class);
      assertThat(row.<Vertex>getProperty("a").getString("name")).isEqualTo("a");
      assertThat(row.<Object>getProperty("r")).isInstanceOf(Map.class);
      assertThat(row.<Map<String, Object>>getProperty("r")).containsEntry("@type", "K12196").containsEntry("since", 2020);
    }
  }

  @Test
  void aMapThatOnlyLooksLikeARecordStaysAMap() {
    // A MAP A QUERY BUILDS MAY HOLD ANY KEYS (A STORED MAP PROPERTY CANNOT: A MAP WITH AN @type IS AN EMBEDDED DOCUMENT): ONE NAMING A
    // TYPE THE SCHEMA DOES NOT HAVE, OR AN @rid THAT IS NOT ONE, IS NOT A RECORD AND STAYS WHAT IT IS, IN EVERY ROW
    final String query = "UNWIND range(1, 3) AS i RETURN i, {`@rid`: '#1:1', `@type`: 'NoSuchType12196'} AS unknownType, "
        + "{`@rid`: 'nope', `@type`: 'P12196'} AS badRid";

    final Database database = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.next().<Object>getProperty("unknownType")).isInstanceOf(Map.class);
    }

    int rows = 0;
    try (final ResultSet rs = grpc.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("unknownType")).isInstanceOf(Map.class);
        assertThat(row.<Map<String, Object>>getProperty("unknownType")).containsEntry("@type", "NoSuchType12196");
        assertThat(row.<Object>getProperty("badRid")).isInstanceOf(Map.class);
        assertThat(row.<Map<String, Object>>getProperty("badRid")).containsEntry("@rid", "nope");
        ++rows;
      }
    }
    assertThat(rows).isEqualTo(3);
  }

  @Test
  void aMapShapedLikeARecordIsReadAsThatRecord() {
    // THE SHAPE IS THE ONLY MARK THE WIRE GIVES A RECORD COLUMN, SO A MAP A QUERY BUILDS WITH THE RID OF A RECORD AND THE NAME OF A
    // TYPE OF THE SCHEMA IS READ AS A RECORD, WHERE THE EMBEDDED API KEEPS THE MAP. PINNED ON PURPOSE, AS THE LIMIT OF THE RULE
    final String rid;
    try (final ResultSet rs = grpc.query("sql", "SELECT @rid AS rid FROM P12196 WHERE name = 'a'")) {
      rid = String.valueOf(rs.next().<Object>getProperty("rid"));
    }

    try (final ResultSet rs = grpc.query("opencypher", "RETURN {`@rid`: '" + rid + "', `@type`: 'P12196', name: 'lookalike'} AS m")) {
      final Document record = rs.next().getProperty("m");
      assertThat(record).isInstanceOf(Vertex.class);
      assertThat(record.getIdentity().toString()).isEqualTo(rid);
      assertThat(record.getString("name")).isEqualTo("lookalike");
    }
  }

  private static List<String> columnNames(final ResultSet resultSet) {
    try (final ResultSet rs = resultSet) {
      return new ArrayList<>(rs.next().getPropertyNames());
    }
  }

  private static List<String> propertyNamesOfColumn(final ResultSet resultSet, final String column) {
    try (final ResultSet rs = resultSet) {
      final Document document = rs.next().getProperty(column);
      return new ArrayList<>(document.getPropertyNames());
    }
  }

  /**
   * One line per row: the element it is with its properties, or the projection with every column, record values shown as
   * records.
   */
  private static List<String> rows(final ResultSet resultSet) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = resultSet) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        if (row.isElement()) {
          rows.add(describe(row.getElement().get()));
          continue;
        }
        final Map<String, String> columns = new TreeMap<>();
        for (final String name : row.getPropertyNames())
          columns.put(name, describe(row.getProperty(name)));
        rows.add("projection" + columns);
      }
    }
    return rows;
  }

  private static String describe(final Object value) {
    if (value instanceof Document document) {
      final Map<String, Object> properties = new TreeMap<>(document.toMap(false));
      return "record:" + document.getIdentity() + ":" + document.getTypeName() + properties;
    }
    if (value instanceof Record record)
      return "record:" + record.getIdentity();
    if (value instanceof List<?> list) {
      final List<String> items = new ArrayList<>(list.size());
      for (final Object item : list)
        items.add(describe(item));
      return items.toString();
    }
    return String.valueOf(value);
  }
}
