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

import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4689 via the RemoteDatabase client (HTTP "record" serializer path,
 * parsed through json2Result/json2Record) - the path a remote driver actually uses.
 */
class Issue4689MatchReturnVertexRemoteIT extends BaseGraphServerTest {

  @Override
  protected String getDatabaseName() {
    return "issue4689";
  }

  @Test
  void cypherMatchReturnWholeVertexOverRemote() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("opencypher", "CREATE (u:IssueUser {name: 'Alice', age: 30})");
    database.command("opencypher", "CREATE (u:IssueUser {name: 'Bob', age: 25})");

    // The reported failing query: returning the whole vertex
    final ResultSet rs = database.query("opencypher", "MATCH (u:IssueUser) RETURN u");

    int count = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(row.isVertex()).as("RETURN u should produce a vertex element over RemoteDatabase").isTrue();
      final Vertex v = row.getVertex().get();
      assertThat(v.getString("name")).isNotNull();
      count++;
    }
    assertThat(count).as("Should return 2 User vertices over RemoteDatabase").isEqualTo(2);
  }

  @Test
  void cypherMatchReturnVertexProjectionOverRemote() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("opencypher", "CREATE (u:IssueUserProj {name: 'Charlie', age: 35})");

    // The workaround the reporter says works
    final ResultSet rs = database.query("opencypher", "MATCH (u:IssueUserProj) RETURN u{.*} as user");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Object>getProperty("user")).as("Projection should expose the 'user' property").isNotNull();
  }

  @Test
  void sqlSelectWholeRecordOverRemote() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE SqlRemoteAll");
    database.command("sql", "INSERT INTO SqlRemoteAll SET name = 'Alice', age = 30");
    database.command("sql", "INSERT INTO SqlRemoteAll SET name = 'Bob', age = 25");

    // SQL: returning the whole record (same class of issue as Cypher)
    final ResultSet rs = database.query("sql", "SELECT FROM SqlRemoteAll");

    int count = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(row.isVertex()).as("SELECT FROM should produce a vertex element over RemoteDatabase").isTrue();
      count++;
    }
    assertThat(count).as("Should return 2 records over RemoteDatabase").isEqualTo(2);
  }

  @Test
  void sqlSelectFieldOverRemote() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE SqlRemoteFields");
    database.command("sql", "INSERT INTO SqlRemoteFields SET name = 'Dave', age = 40");

    // SQL workaround: selecting an individual field
    final ResultSet rs = database.query("sql", "SELECT name FROM SqlRemoteFields");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Dave");
  }

  @Test
  void cypherMatchReturnVertexWithEdgesOverRemote() {
    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("opencypher", "CREATE (u:UserEdgeRemote {name: 'Src', role: 'admin'})");
    database.command("opencypher", "CREATE (u:UserEdgeRemote {name: 'Dst', role: 'user'})");
    database.command("sql",
        "CREATE EDGE E1 FROM (SELECT FROM UserEdgeRemote WHERE name = 'Src') TO (SELECT FROM UserEdgeRemote WHERE name = 'Dst')");

    // Vertices with edges - exercises the vertex serialization with edge metadata over the wire
    final ResultSet rs = database.query("opencypher", "MATCH (u:UserEdgeRemote) RETURN u");

    int count = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(row.isVertex()).isTrue();
      count++;
    }
    assertThat(count).as("Should return 2 vertices with edges over RemoteDatabase").isEqualTo(2);
  }
}
