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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12519: over gRPC, a projection row with a String column named {@code @type} reached the client as a record of that type
 * (the service recovered the type of the rows a follower rebuilds from its leader's JSON, issue #4688, from that very column), so
 * {@code SELECT @type, name FROM P} answered a vertex without the {@code @type} column. It is a projection row, with the columns it
 * was given, as with the embedded API.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class Issue12519GrpcProjectionTypeColumnIT extends BaseGrpcClientServerTest {

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
    http.command("sql", "CREATE VERTEX TYPE P12519");
    http.command("sql", "CREATE EDGE TYPE K12519");
    http.command("opencypher", "CREATE (:P12519 {name: 'a'})-[:K12519]->(:P12519 {name: 'b'})");
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
  void aTypeColumnIsAColumnNotARecord() {
    final String query = "SELECT @type, name FROM P12519 ORDER BY name";
    final Database embedded = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = embedded.query("sql", query)) {
      final Result row = rs.next();
      assertThat(row.isElement()).isFalse();
      assertThat(row.getPropertyNames()).containsExactly("@type", "name");
    }

    try (final ResultSet rs = grpc.query("sql", query)) {
      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());
      assertThat(rows).hasSize(2);
      for (final Result row : rows) {
        assertThat(row.isElement()).isFalse();
        assertThat(row.getPropertyNames()).containsExactly("@type", "name");
        assertThat(row.<String>getProperty("@type")).isEqualTo("P12519");
      }
      assertThat(rows.getFirst().<String>getProperty("name")).isEqualTo("a");
      assertThat(rows.getLast().<String>getProperty("name")).isEqualTo("b");
    }
  }

  @Test
  void aTypeColumnWithAListColumnKeepsBothColumns() {
    final String query = "SELECT @type, out() AS friends FROM P12519 WHERE name = 'a'";
    try (final ResultSet rs = grpc.query("sql", query)) {
      final Result row = rs.next();
      assertThat(row.isElement()).isFalse();
      assertThat(row.getPropertyNames()).containsExactly("@type", "friends");
      assertThat(row.<String>getProperty("@type")).isEqualTo("P12519");
      assertThat(row.<Object>getProperty("friends")).isNotNull();
    }
  }

  @Test
  void aCypherTypeColumnWithCollectedNodesKeepsBothColumns() {
    final String query = "MATCH (n:P12519) WITH n ORDER BY n.name RETURN 'P12519' AS `@type`, collect(n) AS nodes";
    try (final ResultSet rs = grpc.query("opencypher", query)) {
      final Result row = rs.next();
      assertThat(row.isElement()).isFalse();
      assertThat(row.getPropertyNames()).containsExactly("@type", "nodes");
      assertThat(row.<String>getProperty("@type")).isEqualTo("P12519");
      final List<?> nodes = row.getProperty("nodes");
      assertThat(nodes).hasSize(2).allSatisfy(node -> assertThat(node).isInstanceOf(Vertex.class));
    }
  }

  @Test
  void aStreamedTypeColumnIsAColumnToo() {
    try (final ResultSet rs = grpc.queryStream("sql", "SELECT @type, name FROM P12519 ORDER BY name")) {
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.isElement()).isFalse();
        assertThat(row.getPropertyNames()).containsExactly("@type", "name");
        ++rows;
      }
      assertThat(rows).isEqualTo(2);
    }
  }

  @Test
  void aWholeRecordIsStillARecord() {
    try (final ResultSet rs = grpc.query("sql", "SELECT FROM P12519 WHERE name = 'a'")) {
      final Result row = rs.next();
      assertThat(row.isElement()).isTrue();
      assertThat(row.getElement().get().getTypeName()).isEqualTo("P12519");
      assertThat(row.<String>getProperty("name")).isEqualTo("a");
    }
  }
}
