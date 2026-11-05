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
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11056: {@code getStats()} is the same public API, with the same keys, on the embedded and on the remote databases, so the
 * same calls must move the same counters on all of them. The HTTP driver filed {@code query(language, query, Map)} - and the
 * two-argument form that delegates to it - under {@code commands}, and its own read helpers ({@code lookupByRID},
 * {@code iterateType}, the walks of a remote vertex) went through that overload, so a read-only workload grew {@code commands}.
 * Found on the way, the gRPC driver counted {@code query(language, query, Object...)} twice and filed its count helpers as
 * queries, and both drivers counted {@code existsRecord} as a read too, and never counted {@code iterateType} /
 * {@code iterateBucket}.
 * <p>
 * The embedded database is the oracle: every scenario runs on it and on both drivers, and the counters must agree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11056RemoteStatisticsContractTest extends BaseGrpcClientServerTest {
  private static final String       VERTEX_TYPE = "V11056";
  private static final String       EDGE_TYPE   = "E11056";
  private static final List<String> COUNTERS    = List.of("queries", "commands", "readRecord", "existsRecord", "iterateType",
      "iterateBucket", "countType", "countBucket", "scanType", "scanBucket");

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private RemoteDatabase     http;
  private RID                fromRid;
  private RID                edgeRid;
  private String             bucketName;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeEach
  void open() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    http = new RemoteDatabase("localhost", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    http.command("sql", "CREATE VERTEX TYPE " + VERTEX_TYPE + " IF NOT EXISTS");
    http.command("sql", "CREATE EDGE TYPE " + EDGE_TYPE + " IF NOT EXISTS");
    try (final ResultSet rs = http.command("sql", "CREATE VERTEX " + VERTEX_TYPE + " SET name = 'from'")) {
      fromRid = rs.next().getIdentity().get();
    }
    final RID toRid;
    try (final ResultSet rs = http.command("sql", "CREATE VERTEX " + VERTEX_TYPE + " SET name = 'to'")) {
      toRid = rs.next().getIdentity().get();
    }
    try (final ResultSet rs = http.command("sql", "CREATE EDGE " + EDGE_TYPE + " FROM " + fromRid + " TO " + toRid)) {
      edgeRid = rs.next().getIdentity().get();
    }
    bucketName = embedded().getSchema().getType(VERTEX_TYPE).getBuckets(false).get(0).getName();
  }

  @AfterEach
  void close() {
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    if (http != null)
      http.close();
  }

  /** The reported shape: one call of each {@code query} overload is three queries and no command, on every implementation. */
  @ParameterizedTest
  @ValueSource(strings = { "embedded", "http", "grpc" })
  void everyQueryOverloadIsCountedOnceAsAQuery(final String implementation) {
    final Map<String, Long> delta = measure(database(implementation), db -> {
      db.query("sql", "select 1 as one").close();
      db.query("sql", "select ? as one", 1).close();
      db.query("sql", "select :p as one", Map.of("p", 1)).close();
    });

    assertThat(delta.get("queries")).as("three query() calls on " + implementation).isEqualTo(3L);
    assertThat(delta.get("commands")).as("no command() call on " + implementation).isZero();
  }

  /** The same scenario on the three implementations moves the same counters by the same amount. */
  @Test
  void theDriversCountTheReadHelpersAsTheEmbeddedEngineDoes() {
    final Map<String, Long> oracle = measure(embedded(), this::readHelpers);
    assertThat(oracle.get("readRecord")).as("the oracle must have counted the reads, or this proves nothing").isPositive();

    assertThat(measure(http, this::readHelpers)).as("HTTP driver").isEqualTo(oracle);
    assertThat(measure(grpc, this::readHelpers)).as("gRPC driver").isEqualTo(oracle);
  }

  /**
   * The walks of a remote vertex and the endpoints of a remote edge are queries on the wire, but not a {@code query()} of the
   * application: an endpoint is a read, as the embedded edge's {@code lookupByRID} is, and a walk is no counter at all.
   */
  @ParameterizedTest
  @ValueSource(strings = { "http", "grpc" })
  void theWalksOfARemoteRecordAreNotFiledAsQueriesOrCommands(final String implementation) {
    final RemoteDatabase db = (RemoteDatabase) database(implementation);
    final Vertex from = db.lookupByRID(fromRid, true).asVertex();
    final Edge edge = db.lookupByRID(edgeRid, true).asEdge();

    final Map<String, Long> delta = measure(db, d -> {
      assertThat(from.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE)).isEqualTo(1L);
      assertThat(from.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext()).isTrue();
      assertThat(from.isConnectedTo(edge.getIn())).isTrue();
      assertThat(edge.getInVertex()).isNotNull();
    });

    assertThat(delta.get("queries")).as("queries on " + implementation).isZero();
    assertThat(delta.get("commands")).as("commands on " + implementation).isZero();
    assertThat(delta.get("readRecord")).as("the edge endpoint read on " + implementation).isEqualTo(1L);
  }

  /**
   * The embedded schema answers from memory and counts nothing; the drivers read {@code schema:*} with a command, which filed
   * every metadata read - including the one a record conversion triggers - as a {@code command()} of the application.
   */
  @ParameterizedTest
  @ValueSource(strings = { "embedded", "http", "grpc" })
  void schemaMetadataReadsAreNotCommands(final String implementation) {
    final BasicDatabase db = database(implementation);
    final Map<String, Long> delta = measure(db, d -> {
      assertThat(d.getSchema().existsType(VERTEX_TYPE)).isTrue();
      assertThat(d.getSchema().getType(VERTEX_TYPE)).isNotNull();
      assertThat(d.getSchema().existsBucket(bucketName)).isTrue();
      assertThat(d.getSchema().getTypes()).isNotEmpty();
    });

    assertThat(delta.get("commands")).as("commands on " + implementation).isZero();
    assertThat(delta.get("queries")).as("queries on " + implementation).isZero();
  }

  /** {@code RemoteDatabase.lookupByRID(RID)} counted the read itself and then again in the overload it delegates to. */
  @ParameterizedTest
  @ValueSource(strings = { "http", "grpc" })
  void aLookupByRidIsOneRead(final String implementation) {
    final RemoteDatabase db = (RemoteDatabase) database(implementation);
    final Map<String, Long> delta = measure(db, d -> assertThat(db.lookupByRID(fromRid)).isNotNull());

    assertThat(delta.get("readRecord")).as("readRecord on " + implementation).isEqualTo(1L);
    assertThat(delta.get("queries")).as("queries on " + implementation).isZero();
  }

  private void readHelpers(final BasicDatabase db) {
    assertThat(db.lookupByRID(fromRid, true)).isNotNull();
    assertThat(db.existsRecord(fromRid)).isTrue();
    consume(db.iterateType(VERTEX_TYPE, true));
    consume(db.iterateType(VERTEX_TYPE, false));
    consume(db.iterateBucket(bucketName));
    assertThat(db.countType(VERTEX_TYPE, true)).isEqualTo(2L);
    assertThat(db.countBucket(bucketName)).isPositive();
  }

  private static void consume(final Iterator<?> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      ++count;
    }
    assertThat(count).isPositive();
  }

  private BasicDatabase database(final String implementation) {
    return switch (implementation) {
      case "embedded" -> embedded();
      case "http" -> http;
      case "grpc" -> grpc;
      default -> throw new IllegalArgumentException(implementation);
    };
  }

  private Database embedded() {
    return getServerDatabase(0, getDatabaseName());
  }

  private static Map<String, Long> measure(final BasicDatabase db, final Scenario scenario) {
    final Map<String, Object> before = db.getStats();
    scenario.run(db);
    final Map<String, Object> after = db.getStats();

    final Map<String, Long> delta = new LinkedHashMap<>();
    for (final String counter : COUNTERS)
      delta.put(counter, ((Number) after.get(counter)).longValue() - ((Number) before.get(counter)).longValue());
    return delta;
  }

  @FunctionalInterface
  private interface Scenario {
    void run(BasicDatabase db);
  }
}
