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
package com.arcadedb.bench.workload.protocol;

import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.RemoteClients;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The four openCypher clients against one server with every protocol plugin enabled: the same read gives the same rows as the
 * HTTP client, each parameter shape crosses, and a transaction commits (and a failing one leaves nothing behind).
 */
class ProtocolClientIT {
  private static final String IMAGE    = System.getProperty("arcadedb.bench.image", "arcadedata/arcadedb:latest");
  private static final String DATABASE = "protocols";

  private static final Map<String, String> SETTINGS = Map.of(
      "arcadedb.server.plugins", "PostgresProtocolPlugin,GrpcServerPlugin,BoltProtocolPlugin,PrometheusMetricsPlugin",
      "arcadedb.grpc.enabled", "true",
      "arcadedb.grpc.port", "50051",
      "arcadedb.grpc.mode", "standard");

  private static final String READ =
      "MATCH (a:P)-[:K]->(b:P) RETURN a.id AS a, b.id AS b, a.name AS name, a.score AS score, "
          + "[a.score, b.score] AS scores, {x: a.id, y: b.name} AS m ORDER BY a, b";

  private static ServerUnderTest server;

  @BeforeAll
  static void startServer() {
    server = ServerUnderTest.start(IMAGE, "", 4, "2g", SETTINGS);
    RemoteClients.ensureDatabase(server, DATABASE);
    try (final RemoteDatabase db = RemoteClients.open(server, DATABASE)) {
      db.command("sql", "CREATE VERTEX TYPE P IF NOT EXISTS");
      db.command("sql", "CREATE VERTEX TYPE T IF NOT EXISTS");
      db.command("sql", "CREATE EDGE TYPE K IF NOT EXISTS");
      // FLOAT-typed values: Bolt widens a float to a double (0.1f -> 0.10000000149011612), the text protocols say 0.1
      db.command("sql", "CREATE VERTEX TYPE F IF NOT EXISTS");
      db.command("sql", "CREATE PROPERTY F.f IF NOT EXISTS FLOAT");
      db.command("sql", "CREATE PROPERTY F.v IF NOT EXISTS ARRAY_OF_FLOATS");
      db.command("sql", "INSERT INTO F SET id = 1, f = 0.1, v = [0.1, 0.7, -2.3]");
      db.command("opencypher", "CREATE (:P {id: 1, name: 'one', score: 0.5}), (:P {id: 2, name: 'two', score: 1.25}), "
          + "(:P {id: 3, name: 'three', score: 2.0})");
      db.command("opencypher", "MATCH (a:P {id: 1}), (b:P {id: 2}) CREATE (a)-[:K]->(b)");
      db.command("opencypher", "MATCH (a:P {id: 1}), (b:P {id: 3}) CREATE (a)-[:K]->(b)");
      db.command("opencypher", "MATCH (a:P {id: 2}), (b:P {id: 3}) CREATE (a)-[:K]->(b)");
    }
  }

  @AfterAll
  static void stopServer() {
    if (server != null)
      server.close();
  }

  private static List<Map<String, Object>> overHttp(final String cypher, final Map<String, Object> params) {
    try (final ProtocolClient http = ProtocolClient.open(Protocol.HTTP, server, DATABASE)) {
      return http.query(cypher, params);
    }
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void readGivesTheRowsOfHttp(final Protocol protocol) {
    final List<Map<String, Object>> expected = overHttp(READ, Map.of());
    assertThat(expected).hasSize(3);
    try (final ProtocolClient client = ProtocolClient.open(protocol, server, DATABASE)) {
      assertThat(Parity.diff(expected, client.query(READ, Map.of()), true)).isNull();
    }
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void floatTypedValuesGiveTheRowsOfHttp(final Protocol protocol) {
    final String cypher = "MATCH (x:F) RETURN x.f AS f, x.v AS v, x.id AS id";
    final List<Map<String, Object>> expected = overHttp(cypher, Map.of());
    assertThat(expected).hasSize(1);
    try (final ProtocolClient client = ProtocolClient.open(protocol, server, DATABASE)) {
      assertThat(Parity.diff(expected, client.query(cypher, Map.of()), true)).isNull();
    }
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void scalarParameter(final Protocol protocol) {
    final String cypher = "MATCH (p:P) WHERE p.id = $id OR p.name = $name RETURN p.id AS id ORDER BY id";
    final Map<String, Object> params = Map.of("id", 1L, "name", "three");
    final List<Map<String, Object>> expected = overHttp(cypher, params);
    assertThat(expected).hasSize(2);
    try (final ProtocolClient client = ProtocolClient.open(protocol, server, DATABASE)) {
      assertThat(Parity.diff(expected, client.query(cypher, params), true)).isNull();
    }
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void listOfMapsParameter(final Protocol protocol) {
    final String cypher = "UNWIND $rows AS r RETURN r.id AS id, r.name AS name";
    final Map<String, Object> params = Map.of("rows", List.of(Map.of("id", 10L, "name", "x"), Map.of("id", 11L, "name", "y")));
    final List<Map<String, Object>> expected = overHttp(cypher, params);
    assertThat(expected).hasSize(2);
    try (final ProtocolClient client = ProtocolClient.open(protocol, server, DATABASE)) {
      assertThat(Parity.diff(expected, client.query(cypher, params), false)).isNull();
    }
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void listOfFloatListsParameter(final Protocol protocol) {
    final String cypher = "UNWIND $vs AS v RETURN size(v) AS n, v[0] AS first, v[1] AS second";
    final Map<String, Object> params = Map.of("vs", List.of(List.of(0.5d, 1.25d), List.of(2.5d, -1.0d)));
    final List<Map<String, Object>> expected = overHttp(cypher, params);
    assertThat(expected).hasSize(2);
    try (final ProtocolClient client = ProtocolClient.open(protocol, server, DATABASE)) {
      assertThat(Parity.diff(expected, client.query(cypher, params), false)).isNull();
    }
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void transactionCommits(final Protocol protocol) {
    final String proto = protocol.caseName();
    try (final ProtocolClient client = ProtocolClient.open(protocol, server, DATABASE)) {
      client.transaction(List.of("CREATE (:T {proto: $p, i: 1})", "CREATE (:T {proto: $p, i: $i})"),
          List.of(Map.of("p", proto), Map.of("p", proto, "i", 2L)));
    }
    final List<Map<String, Object>> rows = overHttp("MATCH (t:T) WHERE t.proto = $p RETURN t.i AS i ORDER BY i", Map.of("p", proto));
    assertThat(Parity.diff(List.of(Map.of("i", 1L), Map.of("i", 2L)), rows, true)).isNull();
  }

  @ParameterizedTest
  @EnumSource(Protocol.class)
  void failingTransactionLeavesNothing(final Protocol protocol) {
    final String proto = protocol.caseName() + "-failed";
    try (final ProtocolClient client = ProtocolClient.open(protocol, server, DATABASE)) {
      assertThatThrownBy(() -> client.transaction(List.of("CREATE (:T {proto: $p, i: 1})", "RETURN noSuchFunction(1) AS x"),
          List.of(Map.of("p", proto), Map.of()))).isInstanceOf(RuntimeException.class);
      // the client is still usable after the rollback
      assertThat(client.query("MATCH (t:T) WHERE t.proto = $p RETURN count(t) AS n", Map.of("p", proto))).hasSize(1);
    }
    final List<Map<String, Object>> rows = overHttp("MATCH (t:T) WHERE t.proto = $p RETURN count(t) AS n", Map.of("p", proto));
    assertThat(Parity.diff(List.of(Map.of("n", 0L)), rows, true)).isNull();
  }
}
