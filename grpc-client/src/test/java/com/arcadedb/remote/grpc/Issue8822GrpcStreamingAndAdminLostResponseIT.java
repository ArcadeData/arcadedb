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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteGraphBatch;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.server.grpc.ArcadeDbAdminServiceGrpc;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.UserInfo;
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8822, end to end: the follow-up of #8525 for the streaming writes and the admin writes. The request reaches
 * the server and is applied, then the call is closed with a status-only {@code UNAVAILABLE} in place of the server's
 * answer. Each of these entry points must report an unknown outcome - "possibly partially applied" for a stream -
 * never the {@link NeedRetryException} a caller would honour by sending the write a second time. Reads keep the
 * retryable mapping.
 */
class Issue8822GrpcStreamingAndAdminLostResponseIT extends BaseGrpcClientServerTest {
  private static final String TYPE    = "Issue8822Row";
  private static final String VERTEX  = "Issue8822Vertex";
  private static final String TS_TYPE = "Issue8822Sensor";

  /** Full method name to the status the call is closed with in place of the server's answer. */
  private final Map<String, Status> replacing = new ConcurrentHashMap<>();
  private final Map<String, Metadata> replacingTrailers = new ConcurrentHashMap<>();
  private final AtomicInteger replaced = new AtomicInteger();

  private RemoteGrpcServer   server;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    server = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true,
        List.of(new ReplacingInterceptor()));
    database = new RemoteGrpcDatabase(server, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS").close();
    database.command("sql", "CREATE VERTEX TYPE " + VERTEX + " IF NOT EXISTS").close();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    replacing.clear();
    if (database != null)
      database.close();
    if (server != null)
      server.close();
    super.endTest();
  }

  // ----------------------------------------------------------------------------------------------------------------
  // Streaming writes (RemoteGrpcDatabase)
  // ----------------------------------------------------------------------------------------------------------------

  @Test
  void ingestStream() {
    assertPartiallyApplied(ArcadeDbServiceGrpc.getInsertStreamMethod(),
        () -> database.ingestStreamAsListOfMaps(options(), rows(3), 1, 30_000));
    assertThat(count()).as("the rows landed exactly once").isEqualTo(3);
  }

  /** A dry run commits nothing, so replaying it is safe and a lost response keeps the retryable mapping. */
  @Test
  void validateOnlyIngestStreamStaysRetryable() {
    drop(ArcadeDbServiceGrpc.getInsertStreamMethod(), Status.UNAVAILABLE, new Metadata());
    assertThatThrownBy(() -> database.ingestStreamAsListOfMaps(options().toBuilder().setValidateOnly(true).build(), rows(3), 1,
        30_000)).isInstanceOf(NeedRetryException.class);
    assertThat(count()).isZero();
  }

  /**
   * {@code ingestStream} handed callAsyncDuplex an observer it had already wrapped, so every failure was mapped twice
   * and the second pass, finding no status on the engine exception, flattened it to "gRPC error: UNKNOWN". A
   * server-classified error must reach the caller with the type the server named.
   */
  @Test
  void ingestStreamKeepsAServerClassifiedType() {
    final Metadata trailers = new Metadata();
    trailers.put(GrpcClientErrorMapper.EXCEPTION_CLASS_KEY, "com.arcadedb.exception.ConcurrentModificationException");
    drop(ArcadeDbServiceGrpc.getInsertStreamMethod(), Status.ABORTED.withDescription("simulated conflict"), trailers);

    assertThatThrownBy(() -> database.ingestStreamAsListOfMaps(options(), rows(1), 1, 30_000))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("simulated conflict");
  }

  @Test
  void ingestBidi() {
    assertPartiallyApplied(ArcadeDbServiceGrpc.getInsertBidirectionalMethod(),
        () -> database.ingestBidi(options(), rows(3), 1, 2, 30_000));
    assertThat(count()).isEqualTo(3);
  }

  @Test
  void validateOnlyIngestBidiStaysRetryable() {
    drop(ArcadeDbServiceGrpc.getInsertBidirectionalMethod(), Status.UNAVAILABLE, new Metadata());
    assertThatThrownBy(() -> database.ingestBidi(options().toBuilder().setValidateOnly(true).build(), rows(3), 1, 2, 30_000))
        .isInstanceOf(NeedRetryException.class);
    assertThat(count()).isZero();
  }

  @Test
  void timeSeriesWriteStream() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TS_TYPE
        + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)").close();

    assertPartiallyApplied(ArcadeDbServiceGrpc.getTimeSeriesWriteStreamMethod(), () -> database.timeSeriesWriteStream(
        List.of(new TimeSeriesPoint(TS_TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 22.5)),
            new TimeSeriesPoint(TS_TYPE, 2_000L, Map.of("location", "us-east"), Map.of("temperature", 23.5))), 1));
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TS_TYPE)) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2);
    }
  }

  @Test
  void graphBatchLoad() {
    assertPartiallyApplied(ArcadeDbServiceGrpc.getGraphBatchLoadMethod(), () -> {
      try (final RemoteGraphBatch batch = database.batch().build()) {
        batch.createVertex(VERTEX, "name", "Alice");
        batch.createVertex(VERTEX, "name", "Bob");
      }
    });
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + VERTEX)) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2);
    }
  }

  /** The #8823 review: a client deadline on a self-committing write is not retryable, and now says it may have landed. */
  @Test
  void deadlineOnAnAutoCommittedCommand() {
    drop(ArcadeDbServiceGrpc.getExecuteCommandMethod(), Status.DEADLINE_EXCEEDED, new Metadata());
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO " + TYPE + " SET name = 'Alice'").close())
        .isInstanceOf(TimeoutException.class)
        .hasMessageContaining("may already have applied");
    replacing.clear();
    assertThat(count()).isEqualTo(1);
  }

  // ----------------------------------------------------------------------------------------------------------------
  // Admin writes (RemoteGrpcServer)
  // ----------------------------------------------------------------------------------------------------------------

  @Test
  void createUser() {
    final String user = "client8822user";
    try {
      assertUnknownOutcome(ArcadeDbAdminServiceGrpc.getCreateUserMethod(),
          () -> server.createUser(user, "client8822password", Map.of(getDatabaseName(), List.of("admin"))));
      assertThat(getServer(0).getSecurity().existsUser(user)).isTrue();
    } finally {
      if (getServer(0).getSecurity().existsUser(user))
        getServer(0).getSecurity().dropUser(user);
    }
  }

  @Test
  void setServerSetting() {
    final GlobalConfiguration setting = GlobalConfiguration.TX_RETRIES;
    final Object previous = getServer(0).getConfiguration().getValue(setting);
    try {
      assertUnknownOutcome(ArcadeDbAdminServiceGrpc.getSetServerSettingMethod(),
          () -> server.setServerSetting(setting.getKey(), "9"));
      assertThat(getServer(0).getConfiguration().getValueAsInteger(setting)).isEqualTo(9);
    } finally {
      getServer(0).getConfiguration().setValue(setting.getKey(), previous);
    }
  }

  /** createDatabase / dropDatabase do not go through call(): they wrap the failure themselves. */
  @Test
  void createAndDropDatabase() {
    final String name = "client8822_db";
    try {
      assertUnknownOutcome(ArcadeDbAdminServiceGrpc.getCreateDatabaseMethod(), () -> server.createDatabase(name));
      assertThat(getServer(0).existsDatabase(name)).isTrue();

      assertUnknownOutcome(ArcadeDbAdminServiceGrpc.getDropDatabaseMethod(), () -> server.dropDatabase(name));
      assertThat(getServer(0).existsDatabase(name)).isFalse();
    } finally {
      if (getServer(0).existsDatabase(name))
        getServer(0).getDatabase(name).getEmbedded().drop();
    }
  }

  /** The server-streaming admin calls go through drain(), not call(). */
  @Test
  void importDatabase() throws IOException {
    final String name = "client8822_import";
    final File source = new File("./target/8822-client-import.csv");
    try (final FileWriter writer = new FileWriter(source)) {
      writer.write("id,name\n1,one\n2,two\n");
    }
    try {
      assertUnknownOutcome(ArcadeDbAdminServiceGrpc.getImportDatabaseMethod(),
          () -> server.importDatabase(name, "file://" + source.getAbsolutePath(), null));
      assertThat(getServer(0).existsDatabase(name)).isTrue();
    } finally {
      if (getServer(0).existsDatabase(name))
        getServer(0).getDatabase(name).getEmbedded().drop();
      source.delete();
    }
  }

  /** A read changes nothing on the server, so a lost response on it stays retryable. */
  @Test
  void adminReadStaysRetryable() {
    drop(ArcadeDbAdminServiceGrpc.getListUsersMethod(), Status.UNAVAILABLE, new Metadata());
    assertThatThrownBy(() -> server.listUsers()).isInstanceOf(NeedRetryException.class);
    replacing.clear();
    assertThat(server.listUsers()).extracting(UserInfo::getName).contains("root");
  }

  // ----------------------------------------------------------------------------------------------------------------

  private void assertPartiallyApplied(final MethodDescriptor<?, ?> method, final ThrowingCallable call) {
    assertUnknownOutcome(method, call, "partially");
  }

  private void assertUnknownOutcome(final MethodDescriptor<?, ?> method, final ThrowingCallable call) {
    assertUnknownOutcome(method, call, "may already have applied");
  }

  private void assertUnknownOutcome(final MethodDescriptor<?, ?> method, final ThrowingCallable call, final String message) {
    drop(method, Status.UNAVAILABLE.withDescription("simulated: connection dropped after the request went out"), new Metadata());
    final int before = replaced.get();
    try {
      assertThatThrownBy(call)
          .isInstanceOf(RemoteException.class)
          .isNotInstanceOf(NeedRetryException.class)
          .hasMessageContaining("may already have applied")
          .hasMessageContaining(message);
    } finally {
      replacing.clear();
    }
    assertThat(replaced.get()).as("the answer was replaced exactly once").isEqualTo(before + 1);
  }

  private void drop(final MethodDescriptor<?, ?> method, final Status status, final Metadata trailers) {
    replacing.put(method.getFullMethodName(), status);
    replacingTrailers.put(method.getFullMethodName(), trailers);
  }

  private InsertOptions options() {
    return InsertOptions.newBuilder().setTargetClass(TYPE).setTransactionMode(TransactionMode.PER_BATCH).setServerBatchSize(1)
        .build();
  }

  private static List<Map<String, Object>> rows(final int count) {
    return IntStream.range(0, count).mapToObj(i -> Map.<String, Object>of("name", "row-" + i)).toList();
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /**
   * Lets every call to a method named in {@link #replacing} run to its end on the server, then closes it with the
   * configured status in place of the server's own.
   */
  private final class ReplacingInterceptor implements ClientInterceptor {
    @Override
    public <Q, A> ClientCall<Q, A> interceptCall(final MethodDescriptor<Q, A> method, final CallOptions callOptions,
        final Channel next) {
      final ClientCall<Q, A> call = next.newCall(method, callOptions);
      final String name = method.getFullMethodName();
      if (!replacing.containsKey(name))
        return call;

      return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
        @Override
        public void start(final Listener<A> responseListener, final Metadata headers) {
          super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
            @Override
            public void onClose(final Status status, final Metadata trailers) {
              final Status replacement = replacing.get(name);
              if (replacement == null) {
                super.onClose(status, trailers);
                return;
              }
              replaced.incrementAndGet();
              super.onClose(replacement, replacingTrailers.getOrDefault(name, new Metadata()));
            }
          }, headers);
        }
      };
    }
  }
}
