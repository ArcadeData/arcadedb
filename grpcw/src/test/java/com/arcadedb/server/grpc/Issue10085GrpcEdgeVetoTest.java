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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import com.arcadedb.query.sql.executor.ResultSet;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issues #10085 and #10084 on the gRPC surface: a before-create listener that refuses an edge was a {@link NullPointerException} out of
 * the adjacency append, answered {@code INTERNAL} as if the server were broken. The streaming insert counts the refused edge as
 * {@code ignored} like a refused document or vertex; the unary calls, which must answer a record and cannot, fail with
 * {@code INVALID_ARGUMENT} naming the refusal instead of replying a record that was never created, and an update a listener
 * vetoed is not replied as {@code updated}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10085GrpcEdgeVetoTest extends BaseGrpcServerTest {
  private static final String VERTEX = "VtxVeto10085";
  private static final String EDGE   = "EdgeVeto10085";
  private static final String REFUSING_EDGE = "EdgeRefusing10085";
  private static final String META          = "Meta10085";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;
  private String                                          from;
  private String                                          to;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions,
          final Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
          @Override
          public void start(final Listener<RespT> responseListener, final Metadata headers) {
            headers.put(USER_HEADER, "root");
            headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
            headers.put(DATABASE_HEADER, getDatabaseName());
            super.start(responseListener, headers);
          }
        };
      }
    }));

    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createVertexType(VERTEX).getEvents().registerListener(
        (BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("name")));
    database.getSchema().createEdgeType(EDGE).getEvents().registerListener(
        (BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("name")));
    database.getSchema().getType(EDGE).getEvents().registerListener(
        (BeforeRecordUpdateListener) record -> !"refused".equals(record.asDocument().getString("name")));

    final String[] rids = new String[2];
    database.transaction(() -> {
      final var a = database.newVertex(VERTEX).set("name", "a").save();
      final var b = database.newVertex(VERTEX).set("name", "b").save();
      rids[0] = a.getIdentity().toString();
      rids[1] = b.getIdentity().toString();
      // the source owns a committed segment in the next transaction: the ordinary case, where the append dereferenced the null RID
    });
    from = rids[0];
    to = rids[1];
    database.transaction(() -> database.lookupByRID(database.newRID(from), false).asVertex().newEdge(EDGE, database.newRID(to), "name", "first"));
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aRefusedEdgeInTheStreamingInsertIsCountedAsIgnored() {
    final InsertSummary summary = stub.bulkInsert(BulkInsertRequest.newBuilder().setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName()).setCredentials(credentials()).setTargetClass(EDGE)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH).build())
        .addAllRows(List.of(edgeRow("refused"), edgeRow("kept"))).build());

    assertThat(summary.getReceived()).isEqualTo(2);
    assertThat(summary.getInserted()).isEqualTo(1);
    assertThat(summary.getIgnored()).isEqualTo(1);
    assertThat(summary.getFailed()).isZero();
    assertThat(summary.getErrorsList()).isEmpty();
    assertThat(count(EDGE)).isEqualTo(2L);
  }

  /**
   * The properties are part of the create, so the create listener sees the name it refuses: the call fails as a client error and
   * nothing is stored or linked.
   */
  /**
   * The streaming insert hands the edge properties to the create as name/value pairs instead of setting them one by one afterwards:
   * a null, an int and a decimal property land on the edge exactly as before, and nothing but the row's own properties is stored.
   */
  @Test
  void theStreamingInsertStoresNullIntegerAndDecimalPropertiesOnTheEdge() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getType(EDGE).createProperty("count", Type.INTEGER);
    database.getSchema().getType(EDGE).createProperty("amount", Type.DECIMAL);

    final GrpcRecord row = edgeRow("typed").toBuilder().putProperties("count", GrpcValue.newBuilder().setInt32Value(7).build())
        .putProperties("amount", GrpcValue.newBuilder().setDecimalValue(GrpcDecimal.newBuilder().setUnscaled(1234).setScale(2)).build())
        .putProperties("note", GrpcValue.getDefaultInstance()).build();

    final InsertSummary summary = stub.bulkInsert(BulkInsertRequest.newBuilder().setOptions(InsertOptions.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setTargetClass(EDGE)
        .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH).build()).addRows(row).build());
    assertThat(summary.getInserted()).isEqualTo(1);
    assertThat(summary.getFailed()).isZero();

    try (final ResultSet rs = database.query("sql", "select from " + EDGE + " where name = 'typed'")) {
      final Edge edge = rs.next().getEdge().orElseThrow();
      assertThat(edge.get("count")).isEqualTo(7);
      assertThat(edge.get("amount")).isEqualTo(new BigDecimal("12.34"));
      assertThat(edge.getPropertyNames()).contains("name", "count", "amount", "note");
      assertThat(edge.get("note")).isNull();
    }
  }

  @Test
  void aRefusedEdgeInAUnaryCreateIsAClientErrorAndStoresNothing() {
    assertThatThrownBy(() -> stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(EDGE).setRecord(edgeRow("refused")).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
          assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
          assertThat(e.getStatus().getDescription()).contains("vetoed");
        });

    assertThat(count(EDGE)).isEqualTo(1L);
    assertThat(edgeCountOf(from)).isEqualTo(1L);
  }

  /**
   * Under an externally managed transaction nothing of the refused edge is left in it: the create is atomic, so the client's commit
   * afterwards does not carry a half-initialised edge wired to the vertices.
   */
  @Test
  void aRefusedEdgeLeavesNothingInAnExternalTransaction() {
    final String txId = stub.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext transaction = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();

    assertThatThrownBy(() -> stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(EDGE).setRecord(edgeRow("refused")).setTransaction(transaction).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

    assertThat(stub.commitTransaction(CommitTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(transaction).build())
        .getCommitted()).isTrue();

    assertThat(count(EDGE)).isEqualTo(1L);
    assertThat(edgeCountOf(from)).isEqualTo(1L);
    assertThat(edgeCountOfIn(to)).isEqualTo(1L);
  }

  /**
   * An initializer that fails part way (a property the schema type cannot convert) leaves nothing in an externally managed
   * transaction either: the edge is neither saved nor wired, so the client's commit afterwards carries nothing of it.
   */
  @Test
  void aFailingPropertyLeavesNothingInAnExternalTransaction() {
    getServerDatabase(0, getDatabaseName()).getSchema().getType(EDGE).createProperty("count", Type.INTEGER);

    final String txId = stub.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext transaction = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();

    final GrpcRecord row = edgeRow("kept").toBuilder().putProperties("count", string("not-a-number")).build();
    assertThatThrownBy(() -> stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(EDGE).setRecord(row).setTransaction(transaction).build())).isInstanceOf(StatusRuntimeException.class);

    assertThat(stub.commitTransaction(CommitTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(transaction).build())
        .getCommitted()).isTrue();

    assertThat(count(EDGE)).isEqualTo(1L);
    assertThat(edgeCountOf(from)).isEqualTo(1L);
    assertThat(edgeCountOfIn(to)).isEqualTo(1L);
  }

  /**
   * The properties are converted on the edge BEFORE it is saved: a LINK needs nothing of it, an EMBEDDED document is created on its
   * owner, which has no identity yet. Both must land on the edge the call creates.
   */
  @Test
  void embeddedAndLinkPropertiesAreSetOnTheUnsavedEdge() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createDocumentType(META);
    database.getSchema().getType(EDGE).createProperty("meta", Type.EMBEDDED).setOfType(META);
    database.getSchema().getType(EDGE).createProperty("ref", Type.LINK);

    final GrpcRecord row = edgeRow("kept").toBuilder()
        .putProperties("meta", GrpcValue.newBuilder().setEmbeddedValue(GrpcEmbedded.newBuilder().setType(META).putFields("note", string("hello"))).build())
        .putProperties("ref", GrpcValue.newBuilder().setLinkValue(GrpcLink.newBuilder().setRid(to)).build()).build();

    final String rid = stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(EDGE).setRecord(row).build()).getRid();

    final Edge edge = database.lookupByRID(database.newRID(rid), true).asEdge();
    assertThat(((Document) edge.get("meta")).getString("note")).isEqualTo("hello");
    assertThat(edge.get("ref").toString()).isEqualTo(to);
  }

  /** A listener that refuses the create itself: newEdge raises the refusal, which reaches the client as INVALID_ARGUMENT. */
  @Test
  void aVetoedCreateOfTheEdgeIsAClientErrorAndStoresNothing() {
    getServerDatabase(0, getDatabaseName()).getSchema().createEdgeType(REFUSING_EDGE).getEvents()
        .registerListener((BeforeRecordCreateListener) record -> false);

    assertThatThrownBy(() -> stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(REFUSING_EDGE).setRecord(edgeRow("kept").toBuilder().setType(REFUSING_EDGE).build()).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
          assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
          assertThat(e.getStatus().getDescription()).contains("vetoed");
        });

    assertThat(count(REFUSING_EDGE)).isZero();
  }

  @Test
  void aRefusedVertexInAUnaryCreateIsAClientErrorAndNotAnInternalOne() {
    assertThatThrownBy(() -> stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(VERTEX).setRecord(GrpcRecord.newBuilder().setType(VERTEX).putProperties("name", string("refused")).build()).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

    assertThat(count(VERTEX)).isEqualTo(2L);
  }

  @Test
  void aKeptEdgeInAUnaryCreateStillAnswersItsRid() {
    final CreateRecordResponse response = stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setType(EDGE).setRecord(edgeRow("kept")).build());

    assertThat(response.getRid()).startsWith("#");
    assertThat(count(EDGE)).isEqualTo(2L);
  }

  @Test
  void anUpdateAListenerVetoedIsNotRepliedAsUpdated() {
    final String rid = firstEdgeRid();

    final UpdateRecordResponse refused = stub.updateRecord(UpdateRecordRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setRid(rid).setPartial(PropertiesUpdate.newBuilder().putProperties("name", string("refused")).build())
        .build());
    assertThat(refused.getUpdated()).isFalse();
    // the clients read success as "the record was updated" (RemoteGrpcDatabase.updateRecord() returns it)
    assertThat(refused.getSuccess()).isFalse();

    final UpdateRecordResponse kept = stub.updateRecord(UpdateRecordRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setRid(rid).setPartial(PropertiesUpdate.newBuilder().putProperties("name", string("renamed")).build())
        .build());
    assertThat(kept.getUpdated()).isTrue();
    assertThat(kept.getSuccess()).isTrue();

    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "select name from " + EDGE)) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("renamed");
    }
  }

  // --- helpers ---

  private GrpcRecord edgeRow(final String name) {
    return GrpcRecord.newBuilder().setType(EDGE).putProperties("out", string(from)).putProperties("in", string(to))
        .putProperties("name", string(name)).build();
  }

  private static GrpcValue string(final String value) {
    return GrpcValue.newBuilder().setStringValue(value).build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private String firstEdgeRid() {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "select from " + EDGE + " limit 1")) {
      return rs.next().getIdentity().orElseThrow().toString();
    }
  }

  private long edgeCountOf(final String vertexRid) {
    final Database database = getServerDatabase(0, getDatabaseName());
    return database.lookupByRID(database.newRID(vertexRid), true).asVertex().countEdges(Vertex.DIRECTION.OUT, EDGE);
  }

  private long edgeCountOfIn(final String vertexRid) {
    final Database database = getServerDatabase(0, getDatabaseName());
    return database.lookupByRID(database.newRID(vertexRid), true).asVertex().countEdges(Vertex.DIRECTION.IN, EDGE);
  }

  private long count(final String type) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count() AS c FROM " + type)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
