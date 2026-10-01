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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.InsertOptions;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8525, end to end: a write that commits on its own reaches the server and lands, then the channel drops before
 * the answer is read ({@code UNAVAILABLE}). Every such entry point must report an unknown outcome, never the
 * {@link NeedRetryException} a caller would honour by applying the write a second time. A write inside a client
 * transaction keeps the retryable mapping, because nothing it did is durable until the commit.
 */
class Issue8525GrpcAutoCommitLostResponseIT extends BaseGrpcClientServerTest {
  private static final String TYPE    = "Issue8525Row";
  private static final String TS_TYPE = "Issue8525Sensor";

  private RemoteGrpcServer server;
  private DroppingDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    server = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new DroppingDatabase(server, getServerGrpcPort(), getServerHttpPort(), getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS").close();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null)
      database.close();
    if (server != null)
      server.close();
    super.endTest();
  }

  @Test
  void autoCommittedCommand() {
    assertUnknownOutcome(ArcadeDbServiceGrpc.getExecuteCommandMethod(),
        () -> database.command("sql", "INSERT INTO " + TYPE + " SET name = 'Alice'").close());
    assertThat(count()).as("the command landed exactly once").isEqualTo(1);
  }

  @Test
  void execSqlBeginCommit() {
    // Before #8525 this path threw a bare IllegalStateException("unreachable") for every gRPC failure
    assertUnknownOutcome(ArcadeDbServiceGrpc.getExecuteCommandMethod(),
        () -> database.execSql("INSERT INTO " + TYPE + " SET name = 'Alice'", Map.of(), 30_000));
    assertThat(count()).isEqualTo(1);
  }

  @Test
  void saveNewDocument() {
    assertUnknownOutcome(ArcadeDbServiceGrpc.getCreateRecordMethod(),
        () -> database.newDocument(TYPE).set("name", "Alice").save());
    assertThat(count()).isEqualTo(1);
  }

  @Test
  void saveExistingDocument() {
    final MutableDocument doc = database.newDocument(TYPE).set("name", "Alice");
    doc.save();

    assertUnknownOutcome(ArcadeDbServiceGrpc.getUpdateRecordMethod(), () -> doc.set("name", "Bob").save());
    assertThat(names()).containsExactly("Bob");
  }

  @Test
  void deleteRecordObject() {
    final MutableDocument doc = database.newDocument(TYPE).set("name", "Alice");
    doc.save();

    assertUnknownOutcome(ArcadeDbServiceGrpc.getDeleteRecordMethod(), () -> database.deleteRecord(doc));
    assertThat(count()).isZero();
  }

  @Test
  void deleteRecordByRid() {
    final MutableDocument doc = database.newDocument(TYPE).set("name", "Alice");
    doc.save();
    final RID rid = doc.getIdentity();

    assertUnknownOutcome(ArcadeDbServiceGrpc.getDeleteRecordMethod(), () -> database.deleteRecord(rid.toString(), 30_000));
    assertThat(count()).isZero();
  }

  @Test
  void createRecord() {
    assertUnknownOutcome(ArcadeDbServiceGrpc.getCreateRecordMethod(),
        () -> database.createRecord(TYPE, Map.of("name", "Alice"), 30_000));
    assertThat(count()).isEqualTo(1);
  }

  @Test
  void createRecordTx() {
    assertUnknownOutcome(ArcadeDbServiceGrpc.getCreateRecordMethod(),
        () -> database.createRecordTx(TYPE, Map.of("name", "Alice"), 30_000));
    assertThat(count()).isEqualTo(1);
  }

  @Test
  void updateRecordPartial() {
    final String rid = database.createRecord(TYPE, Map.of("name", "Alice"), 30_000);

    assertUnknownOutcome(ArcadeDbServiceGrpc.getUpdateRecordMethod(),
        () -> database.updateRecord(rid, Map.<String, Object>of("name", "Bob"), 30_000));
    assertThat(names()).containsExactly("Bob");
  }

  @Test
  void bulkInsert() {
    final InsertOptions options = InsertOptions.newBuilder().setTargetClass(TYPE).build();

    assertUnknownOutcome(ArcadeDbServiceGrpc.getBulkInsertMethod(),
        () -> database.insertBulkAsListOfMaps(options, List.of(Map.of("name", "Alice")), 30_000));
    assertThat(count()).isEqualTo(1);
  }

  @Test
  void timeSeriesWrite() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TS_TYPE + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)")
        .close();

    assertUnknownOutcome(ArcadeDbServiceGrpc.getTimeSeriesWriteMethod(), () -> database.timeSeriesWrite(
        List.of(new TimeSeriesPoint(TS_TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 22.5)))));
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TS_TYPE)) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).as("the points landed exactly once").isEqualTo(1);
    }
  }

  /**
   * The negative space: inside a client transaction a lost response on a command is still retryable, and
   * {@code transaction()} re-running the scope applies nothing twice, because no attempt ever reached its commit.
   */
  @Test
  void commandInsideATransactionStaysRetryable() {
    database.dropping.add(ArcadeDbServiceGrpc.getExecuteCommandMethod().getFullMethodName());
    final AtomicInteger executions = new AtomicInteger();

    assertThatThrownBy(() -> database.transaction(() -> {
      executions.incrementAndGet();
      database.command("sql", "INSERT INTO " + TYPE + " SET name = 'Alice'").close();
    }, false, 3)).isInstanceOf(NeedRetryException.class);

    database.dropping.clear();
    assertThat(executions.get()).as("the scope is retried").isEqualTo(3);
    assertThat(count()).as("no attempt reached its commit").isZero();
  }

  private void assertUnknownOutcome(final MethodDescriptor<?, ?> method, final ThrowingCallable call) {
    database.dropping.add(method.getFullMethodName());
    final int droppedBefore = database.dropped.get();
    try {
      assertThatThrownBy(call)
          .isInstanceOf(RemoteException.class)
          .isNotInstanceOf(NeedRetryException.class)
          .hasMessageContaining("may already have applied it");
    } finally {
      database.dropping.clear();
    }
    assertThat(database.dropped.get()).as("the response was dropped exactly once").isEqualTo(droppedBefore + 1);
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private List<String> names() {
    try (final ResultSet rs = database.query("sql", "SELECT name FROM " + TYPE)) {
      return rs.stream().map(r -> r.<String>getProperty("name")).toList();
    }
  }

  /**
   * Lets every call to a method named in {@link #dropping} reach the server, then closes it with {@code UNAVAILABLE}
   * in place of the server's answer.
   */
  private static final class DroppingDatabase extends RemoteGrpcDatabase {
    final Set<String>   dropping = ConcurrentHashMap.newKeySet();
    final AtomicInteger dropped  = new AtomicInteger();

    DroppingDatabase(final RemoteGrpcServer server, final int grpcPort, final int httpPort, final String databaseName) {
      super(server, "localhost", grpcPort, httpPort, databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
    }

    @Override
    protected ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub createBlockingStub() {
      return super.createBlockingStub().withInterceptors(new ClientInterceptor() {
        @Override
        public <Q, A> ClientCall<Q, A> interceptCall(final MethodDescriptor<Q, A> method, final CallOptions callOptions,
            final Channel next) {
          final ClientCall<Q, A> call = next.newCall(method, callOptions);
          if (!dropping.contains(method.getFullMethodName()))
            return call;

          return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
            @Override
            public void start(final Listener<A> responseListener, final Metadata headers) {
              super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
                @Override
                public void onClose(final Status status, final Metadata trailers) {
                  dropped.incrementAndGet();
                  super.onClose(Status.UNAVAILABLE.withDescription("simulated: connection dropped after the request went out"),
                      new Metadata());
                }
              }, headers);
            }
          };
        }
      });
    }
  }
}
