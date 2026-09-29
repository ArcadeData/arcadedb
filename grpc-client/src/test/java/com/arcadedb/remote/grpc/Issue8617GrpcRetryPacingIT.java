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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.utility.RetryBackoff;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8617, gRPC arm: an {@code UNAVAILABLE} on {@code BeginTransaction} reaches the inherited
 * {@code RemoteDatabase.transaction()} retry loop as a {@link NeedRetryException} (issue #7780), and {@code UNAVAILABLE}
 * also covers transport failures - a refused connection, a dead channel. The loop re-issued those attempts back to back;
 * they are now paced by the same backoff window {@code LocalDatabase.transaction()} uses.
 * <p>
 * The refusal is injected on the client channel and never reaches the server. The pauses are recorded instead of slept.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8617GrpcRetryPacingIT extends BaseGrpcClientServerTest {
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
  void unavailableAttemptsArePacedByTheBackoff() {
    try (final RefusingDatabase database = new RefusingDatabase(server, getServerGrpcPort(), getServerHttpPort(),
        getDatabaseName(), 2)) {
      final AtomicInteger executions = new AtomicInteger();
      database.transaction(executions::incrementAndGet, false, 3);

      assertThat(database.refused.get()).isEqualTo(2);
      assertThat(executions.get()).isEqualTo(1);
      assertThat(database.pauses).hasSize(2);
      final ContextConfiguration defaults = new ContextConfiguration();
      for (int attempt = 0; attempt < database.pauses.size(); attempt++)
        assertThat(database.pauses.get(attempt)).as("pause after attempt %d", attempt + 1)
            .isBetween(1L, RetryBackoff.windowMs(attempt, defaults.getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY_BASE),
                defaults.getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY)));
    }
  }

  @Test
  void theLastAttemptIsNotFollowedByAPause() {
    try (final RefusingDatabase database = new RefusingDatabase(server, getServerGrpcPort(), getServerHttpPort(),
        getDatabaseName(), Integer.MAX_VALUE)) {
      assertThatThrownBy(() -> database.transaction(() -> {
      }, false, 3)).isInstanceOf(NeedRetryException.class);

      assertThat(database.refused.get()).isEqualTo(3);
      assertThat(database.pauses).hasSize(2);
    }
  }

  /**
   * Answers the first {@code refusals} {@code BeginTransaction} calls with {@code UNAVAILABLE} without sending them, and
   * records the pauses the retry loop asks for.
   */
  private static final class RefusingDatabase extends RemoteGrpcDatabase {
    final         AtomicInteger refused = new AtomicInteger();
    final         List<Long>    pauses  = new ArrayList<>();
    private final AtomicInteger remaining;

    RefusingDatabase(final RemoteGrpcServer server, final int grpcPort, final int httpPort, final String databaseName,
        final int refusals) {
      super(server, "localhost", grpcPort, httpPort, databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
      this.remaining = new AtomicInteger(refusals);
    }

    @Override
    protected void sleepBeforeRetry(final long delayMs) {
      pauses.add(delayMs);
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
      listener.onClose(Status.UNAVAILABLE.withDescription("simulated: connection refused"), new Metadata());
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
