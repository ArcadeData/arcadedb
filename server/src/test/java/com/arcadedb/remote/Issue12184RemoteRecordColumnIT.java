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
package com.arcadedb.remote;

import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12184: a record returned as a column of a projection row (Cypher {@code RETURN a, r, b}, SQL {@code @this},
 * {@code first(out())}) reached the remote client as a list of RIDs, every property lost, because the column's LINK type
 * hint converted the record's JSON to an Identifiable.
 */
class Issue12184RemoteRecordColumnIT extends BaseGraphServerTest {
  private RemoteDatabase database;

  @Override
  protected String getDatabaseName() {
    return "issue12184";
  }

  @BeforeEach
  void openAndPopulate() {
    database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    database.command("sql", "create vertex type Person12184");
    database.command("sql", "create edge type Knows12184");
    database.command("sql", "create vertex Person12184 set name = 'alice', age = 30");
    database.command("sql", "create vertex Person12184 set name = 'bob', age = 40");
    database.command("sql", "create edge Knows12184 from (select from Person12184 where name = 'alice') "
        + "to (select from Person12184 where name = 'bob') set since = 2020");
  }

  @AfterEach
  void closeDatabase() {
    if (database != null)
      database.close();
  }

  @Test
  void cypherRecordColumnsAreRecords() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Person12184)-[r:Knows12184]->(b:Person12184) RETURN a, r, b")) {
      final Result row = rs.next();

      assertThat(row.<Object>getProperty("a")).isInstanceOf(Vertex.class);
      final Vertex a = row.getProperty("a");
      assertThat(a.getString("name")).isEqualTo("alice");
      assertThat(a.getInteger("age")).isEqualTo(30);

      assertThat(row.<Object>getProperty("r")).isInstanceOf(Edge.class);
      final Edge r = row.getProperty("r");
      assertThat(r.getInteger("since")).isEqualTo(2020);
      assertThat(r.getOut()).isEqualTo(a.getIdentity());

      assertThat(row.<Object>getProperty("b")).isInstanceOf(Vertex.class);
      final Vertex b = row.getProperty("b");
      assertThat(b.getString("name")).isEqualTo("bob");
      assertThat(r.getIn()).isEqualTo(b.getIdentity());
    }
  }

  @Test
  void cypherRecordNextToScalarColumn() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Person12184 {name:'alice'}) RETURN a, a.name AS n")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("a")).isInstanceOf(Vertex.class);
      assertThat(row.<Vertex>getProperty("a").getInteger("age")).isEqualTo(30);
      assertThat(row.<String>getProperty("n")).isEqualTo("alice");
    }
  }

  @Test
  void sqlThisAndFunctionColumnsAreRecords() {
    try (final ResultSet rs = database.query("sql", "SELECT @this AS me, name FROM Person12184 WHERE name = 'alice'")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("me")).isInstanceOf(Vertex.class);
      assertThat(row.<Vertex>getProperty("me").getInteger("age")).isEqualTo(30);
      assertThat(row.<String>getProperty("name")).isEqualTo("alice");
    }

    try (final ResultSet rs = database.query("sql",
        "SELECT first(out('Knows12184')) AS friend FROM Person12184 WHERE name = 'alice'")) {
      final Object friend = rs.next().getProperty("friend");
      assertThat(friend).isInstanceOf(Vertex.class);
      assertThat(((Vertex) friend).getString("name")).isEqualTo("bob");
    }
  }

  @Test
  void cypherListOfRecordsColumn() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Person12184) RETURN collect(a) AS people")) {
      final List<?> people = rs.next().getProperty("people");
      assertThat(people).hasSize(2).allSatisfy(p -> assertThat(p).isInstanceOf(Vertex.class));
      assertThat(people.stream().map(p -> ((Vertex) p).getString("name")).toList()).containsExactlyInAnyOrder("alice", "bob");
    }
  }

  @Test
  void cypherListWhoseFirstItemIsNotARecord() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Person12184 {name:'alice'}) RETURN [1, a] AS mixed, [null, a] AS nullFirst")) {
      final Result row = rs.next();
      for (final String column : new String[] { "mixed", "nullFirst" }) {
        final List<?> items = row.getProperty(column);
        assertThat(items).as(column).hasSize(2);
        assertThat(items.get(1)).as(column).isInstanceOf(Vertex.class);
        assertThat(((Vertex) items.get(1)).getString("name")).as(column).isEqualTo("alice");
      }
    }
  }

  @Test
  void cypherMapLiteralAndNestedListHoldRecords() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person12184 {name:'alice'}) RETURN {who: a, n: a.name} AS m, [[a]] AS nested")) {
      final Result row = rs.next();

      final Map<?, ?> m = row.getProperty("m");
      assertThat(m.get("who")).isInstanceOf(Vertex.class);
      assertThat(((Vertex) m.get("who")).getInteger("age")).isEqualTo(30);
      assertThat(m.get("n")).isEqualTo("alice");

      final List<?> nested = row.getProperty("nested");
      assertThat(((List<?>) nested.getFirst()).getFirst()).isInstanceOf(Vertex.class);
    }
  }

  @Test
  void cypherPathColumnHoldsRecords() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH path = (a:Person12184)-[:Knows12184]->(b:Person12184) RETURN path")) {
      final Map<?, ?> path = rs.next().getProperty("path");
      final List<?> nodes = (List<?>) path.get("nodes");
      assertThat(nodes).hasSize(2).allSatisfy(n -> assertThat(n).isInstanceOf(Vertex.class));
      assertThat(((Vertex) nodes.getFirst()).getString("name")).isEqualTo("alice");
      final List<?> relationships = (List<?>) path.get("relationships");
      assertThat(relationships).hasSize(1);
      assertThat(relationships.getFirst()).isInstanceOf(Edge.class);
      assertThat(((Edge) relationships.getFirst()).getInteger("since")).isEqualTo(2020);
    }
  }

  @Test
  void embeddedDocumentColumnIsADocument() {
    database.command("sql", "create document type Address12184");
    database.command("sql", "create property Person12184.address embedded of Address12184");
    database.command("sql", "update Person12184 set address = {'@type':'Address12184','city':'Rome'} where name = 'alice'");

    try (final ResultSet rs = database.query("sql", "SELECT address, name FROM Person12184 WHERE name = 'alice'")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("address")).isInstanceOf(Document.class);
      assertThat(row.<Document>getProperty("address").getString("city")).isEqualTo("Rome");
      assertThat(row.<String>getProperty("name")).isEqualTo("alice");
    }
  }
}
