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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7780, gRPC arm: {@link RemoteGrpcDatabase#begin} wrapped every failed {@code BeginTransaction} call in a
 * {@link TransactionException}, including an {@code UNAVAILABLE} that the client's own error mapper turns into a
 * {@link NeedRetryException} everywhere else. The inherited {@code transaction()} loop therefore gave up on the first
 * attempt although no transaction existed and nothing had run.
 * <p>
 * The refusal is injected on the client channel and never reaches the server, so a refused attempt leaves no
 * server-side transaction behind: what the test observes is only the client's handling of the status.
 */
class Issue7780GrpcBeginRetryIT extends BaseGrpcClientServerTest {
  private static final String TYPE = "Issue7780Row";

  private RemoteGrpcServer server;

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
    try (final RemoteGrpcDatabase db = newDatabase(0)) {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS").close();
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (server != null)
      server.close();
    super.endTest();
  }

  @Test
  void anUnavailableBeginIsRetriedByTransaction() {
    try (final RefusingDatabase database = newDatabase(1)) {
      final AtomicInteger executions = new AtomicInteger();
      database.transaction(() -> {
        executions.incrementAndGet();
        database.command("sql", "INSERT INTO " + TYPE + " SET name = 'Alice'").close();
      }, false, 3);

      assertThat(database.refused.get()).as("the first BeginTransaction must have been refused").isEqualTo(1);
      assertThat(executions.get()).isEqualTo(1);
      assertThat(count(database)).isEqualTo(1);
    }
  }

  @Test
  void anUnavailableBeginSurfacesAsNeedRetryException() {
    try (final RefusingDatabase database = newDatabase(1)) {
      assertThatThrownBy(database::begin).isInstanceOf(NeedRetryException.class)
          .isNotInstanceOf(TransactionException.class);
      assertThat(database.isTransactionActive()).isFalse();
    }
  }

  @Test
  void aServerThatKeepsRefusingExhaustsTheBudget() {
    try (final RefusingDatabase database = newDatabase(Integer.MAX_VALUE)) {
      final AtomicInteger executions = new AtomicInteger();
      assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 3))
          .isInstanceOf(NeedRetryException.class);

      assertThat(database.refused.get()).isEqualTo(3);
      assertThat(executions.get()).isZero();
    }
  }

  private long count(final RemoteGrpcDatabase database) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private RefusingDatabase newDatabase(final int refusals) {
    return new RefusingDatabase(server, getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), refusals);
  }

  /**
   * Answers the first {@code refusals} {@code BeginTransaction} calls with {@code UNAVAILABLE} without sending them.
   */
  private static final class RefusingDatabase extends RemoteGrpcDatabase {
    final         AtomicInteger refused = new AtomicInteger();
    private final AtomicInteger remaining;

    RefusingDatabase(final RemoteGrpcServer server, final int grpcPort, final int httpPort, final String databaseName,
        final int refusals) {
      super(server, "localhost", grpcPort, httpPort, databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
      this.remaining = new AtomicInteger(refusals);
    }

    @Override
    protected ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub createBlockingStub() {
      return super.createBlockingStub().withInterceptors(new ClientInterceptor() {
        @Override
        public <Q, A> ClientCall<Q, A> interceptCall(final MethodDescriptor<Q, A> method, final CallOptions callOptions,
            final Channel next) {
          if (!ArcadeDbServiceGrpc.getBeginTransactionMethod().getFullMethodName().equals(method.getFullMethodName())
              || remaining.getAndDecrement() <= 0)
            return next.newCall(method, callOptions);

          refused.incrementAndGet();
          return new RefusedCall<>();
        }
      });
    }
  }

  /** A call that is never sent and closes with {@code UNAVAILABLE} as soon as it starts. */
  private static final class RefusedCall<Q, A> extends ClientCall<Q, A> {
    @Override
    public void start(final Listener<A> listener, final Metadata headers) {
      listener.onClose(Status.UNAVAILABLE.withDescription("simulated: server installing a snapshot"), new Metadata());
    }

    @Override
    public void request(final int numMessages) {
    }

    @Override
    public void cancel(final String message, final Throwable cause) {
    }

    @Override
    public void halfClose() {
    }

    @Override
    public void sendMessage(final Q message) {
    }
  }
}
