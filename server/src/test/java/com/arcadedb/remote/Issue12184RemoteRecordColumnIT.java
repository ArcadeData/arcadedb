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

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

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
}
