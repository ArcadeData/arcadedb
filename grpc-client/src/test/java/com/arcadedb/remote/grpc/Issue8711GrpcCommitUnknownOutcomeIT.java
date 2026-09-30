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
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
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
 * Issue #8711, end to end: the commit reaches the server and lands, then the channel drops before the answer is read
 * ({@code UNAVAILABLE}). The client must report an unknown outcome as a {@link TransactionException}, never retry the
 * scope, which would apply it a second time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8711GrpcCommitUnknownOutcomeIT extends BaseGrpcClientServerTest {
  private static final String TYPE = "Issue8711Row";

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
    try (final DroppingDatabase db = newDatabase()) {
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
  void anUnavailableCommitIsNotRetried() {
    try (final DroppingDatabase database = newDatabase()) {
      final AtomicInteger executions = new AtomicInteger();
      assertThatThrownBy(() -> database.transaction(() -> {
        executions.incrementAndGet();
        database.command("sql", "INSERT INTO " + TYPE + " SET name = 'Alice'").close();
      }, false, 3)).isInstanceOf(TransactionException.class).isNotInstanceOf(NeedRetryException.class);

      assertThat(database.dropped.get()).isEqualTo(1);
      assertThat(executions.get()).as("the scope must not run again").isEqualTo(1);
      assertThat(count(database)).as("the commit landed exactly once").isEqualTo(1);
    }
  }

  private long count(final RemoteGrpcDatabase database) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private DroppingDatabase newDatabase() {
    return new DroppingDatabase(server, getServerGrpcPort(), getServerHttpPort(), getDatabaseName());
  }

  /**
   * Lets every {@code CommitTransaction} reach the server, then closes the call with {@code UNAVAILABLE} in place of
   * the server's answer.
   */
  private static final class DroppingDatabase extends RemoteGrpcDatabase {
    final AtomicInteger dropped = new AtomicInteger();

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
          if (!ArcadeDbServiceGrpc.getCommitTransactionMethod().getFullMethodName().equals(method.getFullMethodName()))
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
