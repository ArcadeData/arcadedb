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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;

import io.micrometer.observation.ObservationRegistry;
import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Methods;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #6201: the HTTP status a failure is answered with must not depend on whether the
 * failure happened to be raised inside a transaction wrapper.
 * <p>
 * {@code DatabaseAbstractHandler} runs {@code execute()} inside the engine's retrying transaction, and the error
 * mapping in {@link AbstractServerHttpHandler} used to be three hand-written {@code instanceof} chains - one for
 * an exception that arrived as itself, one inside the {@code CommandExecutionException} arm and one inside the
 * {@code TransactionException} arm. They were mirrors maintained by hand and were never complete: every new
 * mapping (#4350, #5064, #5191, #5602, #5935, #6191) had to be added to each chain separately, and the half that
 * was missed took a second issue to notice. {@code NeedRetryException} (503) and {@code RecordNotFoundException}
 * (404) were the pair still missing when #6201 was filed, so a retryable conflict raised while a command ran was
 * answered as an opaque 500 "Error on transaction commit" - and a client whose retry policy keys on 503 gave up
 * on a write that would have succeeded on retry.
 * <p>
 * This test pins the property rather than the individual arms: for every mapped exception, the bare exception and
 * the same exception wrapped in each of the two generic wrappers must produce the SAME status. A future mapping
 * added to only one branch of the classification fails here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6201ErrorStatusParityTest {

  /** One mapped failure: how to build it, and the status the contract says it is answered with. */
  private record MappedFailure(String name, int expectedStatus, Supplier<RuntimeException> factory) {
  }

  private static final List<MappedFailure> MAPPED_FAILURES = List.of(
      // The pair #6201 was filed for: both existed only on the un-wrapped chain.
      new MappedFailure("ConcurrentModificationException", 503,
          () -> new ConcurrentModificationException("Record modified by another transaction")),
      new MappedFailure("RecordNotFoundException", 404,
          () -> new RecordNotFoundException("Record not found", new RID(1, 1))),
      // The mappings each earlier issue had to add to two or three chains by hand.
      new MappedFailure("DuplicatedKeyException", 409, () -> new DuplicatedKeyException("Idx", "[1]", new RID(1, 1))),
      new MappedFailure("TransactionCommittedRemotelyException", 409,
          () -> new TransactionCommittedRemotelyException("committed cluster-wide, do not retry",
              new IllegalStateException("simulated local apply failure"))),
      new MappedFailure("ServerIsNotTheLeaderException", 400,
          () -> new ServerIsNotTheLeaderException("Not the leader", "192.168.0.1:2480")),
      new MappedFailure("SecurityException", 403, () -> new SecurityException("Not allowed")),
      new MappedFailure("QueryNotIdempotentException", 400,
          () -> new QueryNotIdempotentException("Query is not idempotent")),
      new MappedFailure("JSONException", 400, () -> new JSONException("Missing property 'command'")),
      new MappedFailure("IllegalArgumentException", 400, () -> new IllegalArgumentException("Unparseable limit")),
      new MappedFailure("CommandParsingException", 400, () -> new CommandParsingException("Unknown variable 'x'")));

  /**
   * The property itself: bare, wrapped in {@code TransactionException} (what the auto-commit wrapper produced),
   * and wrapped in {@code CommandExecutionException} (what a script or a command planner produces) must all
   * answer the same status.
   */
  @TestFactory
  Stream<DynamicTest> statusIsTheSameWrappedAndUnwrapped() {
    return MAPPED_FAILURES.stream().map(failure -> DynamicTest.dynamicTest(failure.name(), () -> {
      final HandledResponse bare = handle(failure.factory().get());
      assertThat(bare.statusCode)
          .as("%s raised directly must answer %d (body=%s)", failure.name(), failure.expectedStatus(), bare.body)
          .isEqualTo(failure.expectedStatus());

      final HandledResponse inTransaction = handle(
          new TransactionException("Error on executing command", failure.factory().get()));
      assertThat(inTransaction.statusCode)
          .as("%s raised INSIDE the auto-commit transaction wrapper must answer the same %d, not a generic 500"
              + " (body=%s)", failure.name(), failure.expectedStatus(), inTransaction.body)
          .isEqualTo(failure.expectedStatus());

      final HandledResponse inCommand = handle(
          new CommandExecutionException("Error on command execution", failure.factory().get()));
      assertThat(inCommand.statusCode)
          .as("%s wrapped by a command planner must answer the same %d (body=%s)", failure.name(),
              failure.expectedStatus(), inCommand.body)
          .isEqualTo(failure.expectedStatus());
    }));
  }

  /**
   * The concrete defect reported in #6201: a lost MVCC race inside the auto-commit wrapper answered 500
   * "Error on transaction commit", which tells a client neither that the write can be re-driven nor why it
   * failed. {@code PostBatchHandler} documents the contract as "NeedRetryException -> 503".
   */
  @Test
  void aConflictInsideTheAutoCommitWrapperIsReportedAsRetryable() {
    final HandledResponse response = handle(new TransactionException("Error on executing command",
        new ConcurrentModificationException("Record #1:1 modified by another transaction")));

    assertThat(response.statusCode).isEqualTo(503);
    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("error")).isEqualTo("Cannot execute command");
    // The typed exception survives too: the remote Java driver and the HA leader-exception reconstruction both
    // rebuild the retryable type from this field, and a generic TransactionException is not retryable.
    assertThat(json.getString("exception")).isEqualTo(ConcurrentModificationException.class.getName());
  }

  /**
   * The un-wrapped fallbacks the classification still has to distinguish once nothing more specific matched, so
   * collapsing the three chains into one did not collapse the three generic 500s into one message.
   */
  @Test
  void theGenericWrappersKeepTheirOwnUnrecognisedFailureMessage() {
    assertThat(new JSONObject(handle(new TransactionException("Error on commit")).body).getString("error"))
        .isEqualTo("Error on transaction commit");
    assertThat(new JSONObject(handle(new CommandExecutionException("boom", new IllegalStateException("x"))).body)
        .getString("error")).isEqualTo("Cannot execute command");
    assertThat(new JSONObject(handle(new IllegalStateException("unexpected internal state")).body).getString("error"))
        .isEqualTo("Internal error");
  }

  private record HandledResponse(int statusCode, String body) {
  }

  /**
   * Runs the real {@link AbstractServerHttpHandler#handleRequest} against a handler whose {@code execute()}
   * throws the given exception, and captures the status code and JSON body the classification produces.
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

  /** Handler whose execute() throws, standing in for a command that fails while running. */
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
      // Skip the Authorization machinery: this test targets the error classification only.
      return false;
    }
  }
}
