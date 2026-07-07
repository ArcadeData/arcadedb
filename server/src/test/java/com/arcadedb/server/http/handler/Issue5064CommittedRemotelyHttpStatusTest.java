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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;

import io.micrometer.observation.ObservationRegistry;
import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Methods;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for the wire-facing half of issue #5064 (PR #5075): when a transaction is durably
 * committed cluster-wide but the LOCAL apply fails, {@link TransactionCommittedRemotelyException} must
 * surface over HTTP as 409 Conflict with an explicit do-not-retry detail - never as a retryable 5xx.
 * A 503 (or a load-balancer-retried 500) would invite the client to re-drive the write, inserting
 * duplicates of records the cluster already committed: the exact hazard the distinct exception type
 * exists to prevent. Same rationale as the DuplicatedKeyException 409 mapping from issue #4350.
 * <p>
 * The exception can only be produced by the Raft HA layer (ha-raft module), which the server module
 * cannot depend on, so this test drives the REAL {@code handleRequest} catch chain directly with a
 * handler whose {@code execute()} throws - the same seam the production exception propagates through
 * (it is thrown from {@code database.commit()} inside the handler's execute path). The full
 * cluster-side path, including this 409 over a real wire, is covered end-to-end by
 * {@code Issue5064CommittedRemotelyContractIT} in the ha-raft module.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5064CommittedRemotelyHttpStatusTest {

  @Test
  void committedRemotelyMapsTo409WithDoNotRetryDetail() {
    final HandledResponse response = handle(new TransactionCommittedRemotelyException(
        "Transaction TX(1) is committed cluster-wide but the local apply failed"
            + " (local pages reconciled from the replicated payload). Do NOT retry: reload the records and continue",
        new IllegalStateException("simulated local apply failure")));

    assertThat(response.statusCode)
        .as("committed-remotely must map to 409 Conflict, not a retry-worthy 5xx (body=%s)", response.body)
        .isEqualTo(409);

    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("error")).isEqualTo("Transaction committed cluster-wide but the local apply failed - do not retry");
    assertThat(json.getString("exception")).isEqualTo(TransactionCommittedRemotelyException.class.getName());
    assertThat(json.getString("detail")).contains("committed cluster-wide").contains("Do NOT retry");
  }

  @Test
  void committedRemotelyWrappedInCommandExecutionExceptionKeeps409() {
    // #5075 round 6 (defense in depth): script execution and command planners wrap exceptions in
    // CommandExecutionException. The do-not-retry 409 must survive the wrapping - falling through to the
    // generic 500 would re-invite the duplicate-insert retry the distinct type exists to prevent.
    final HandledResponse response = handle(new CommandExecutionException("Error on command execution",
        new TransactionCommittedRemotelyException("Transaction TX(1) is committed cluster-wide but the local apply failed."
            + " Do NOT retry: reload the records and continue", new IllegalStateException("simulated"))));

    assertThat(response.statusCode)
        .as("a WRAPPED committed-remotely outcome must keep the non-retryable 409 (body=%s)", response.body)
        .isEqualTo(409);
    assertThat(new JSONObject(response.body).getString("exception"))
        .isEqualTo(TransactionCommittedRemotelyException.class.getName());
  }

  @Test
  void committedRemotelyWrappedInTransactionExceptionKeeps409() {
    // Same guard for the auto-commit wrapper in DatabaseAbstractHandler, which wraps any Exception thrown
    // by execute() in a plain TransactionException.
    final HandledResponse response = handle(new TransactionException("Error on transaction commit",
        new TransactionCommittedRemotelyException("Transaction TX(1) is committed cluster-wide but the local apply failed."
            + " Do NOT retry: reload the records and continue", new IllegalStateException("simulated"))));

    assertThat(response.statusCode)
        .as("the auto-commit wrapper must not degrade the committed-remotely 409 (body=%s)", response.body)
        .isEqualTo(409);
    assertThat(new JSONObject(response.body).getString("exception"))
        .isEqualTo(TransactionCommittedRemotelyException.class.getName());
  }

  @Test
  void plainTransactionExceptionStillMapsTo500() {
    // Guards the catch ORDER: the committed-remotely arm precedes the generic TransactionException arm.
    // If a refactor reordered them, the specific type would fall into this generic 500 mapping instead.
    final HandledResponse response = handle(new TransactionException("Error on commit"));

    assertThat(response.statusCode).isEqualTo(500);
    assertThat(new JSONObject(response.body).getString("error")).isEqualTo("Error on transaction commit");
  }

  @Test
  void needRetryExceptionStillMapsTo503() {
    // The retryable/non-retryable split: a NeedRetryException subtype keeps the retry-worthy 503,
    // while TransactionCommittedRemotelyException (deliberately NOT a NeedRetryException) gets 409.
    final HandledResponse response = handle(new ConcurrentModificationException("Record modified by another transaction"));

    assertThat(response.statusCode).isEqualTo(503);
    assertThat(new JSONObject(response.body).getString("error")).isEqualTo("Cannot execute command");
  }

  private record HandledResponse(int statusCode, String body) {
  }

  /**
   * Runs the real {@link AbstractServerHttpHandler#handleRequest} against a handler whose
   * {@code execute()} throws the given exception, and captures the status code and JSON body the
   * catch chain produces.
   */
  private HandledResponse handle(final RuntimeException toThrow) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getObservationRegistry()).thenReturn(ObservationRegistry.create());
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getServerName()).thenReturn("test");

    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);

    final Sender sender = mock(Sender.class);
    final HttpServerExchange exchange = mock(HttpServerExchange.class);
    final int[] statusCode = { 200 };
    when(exchange.setStatusCode(anyInt())).thenAnswer(invocation -> {
      statusCode[0] = invocation.getArgument(0);
      return exchange;
    });
    when(exchange.getStatusCode()).thenAnswer(invocation -> statusCode[0]);
    when(exchange.getRequestHeaders()).thenReturn(new HeaderMap());
    when(exchange.getResponseHeaders()).thenReturn(new HeaderMap());
    when(exchange.getRequestMethod()).thenReturn(Methods.POST);
    when(exchange.getRelativePath()).thenReturn("/command/graph");
    when(exchange.getResponseSender()).thenReturn(sender);

    new ThrowingHandler(httpServer, toThrow).handleRequest(exchange);

    final ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
    verify(sender).send(body.capture());
    return new HandledResponse(statusCode[0], body.getValue());
  }

  /** Handler whose execute() throws, standing in for a command execution that fails at commit time. */
  private static final class ThrowingHandler extends AbstractServerHttpHandler {
    private final RuntimeException toThrow;

    private ThrowingHandler(final HttpServer httpServer, final RuntimeException toThrow) {
      super(httpServer);
      this.toThrow = toThrow;
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
        final JSONObject payload) {
      throw toThrow;
    }

    @Override
    public boolean isRequireAuthentication() {
      // Skip the Authorization machinery: this test targets the error-mapping catch chain only.
      return false;
    }
  }
}
