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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
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

import java.nio.BufferUnderflowException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #600 (Locstat): a read-only command
 * (<pre>SELECT number, both().size() FROM Account ORDER BY both().size() DESC LIMIT 5</pre>) failed with
 * {@code java.nio.BufferUnderflowException}, but the server logged only
 * <pre>Error on transaction execution (PostCommandHandler): Error on executing command</pre>
 * with NO stack trace, even with logging at DEBUG. The exception could not be located.
 * <p>
 * Root cause: the unexpected-internal-error {@code else} arm of the catch chain in
 * {@link AbstractServerHttpHandler#handleRequest} logged the message only - it never passed the
 * {@link Throwable} to the logger, so a stack trace was never emitted at any log level. This test drives the
 * REAL catch chain with an internal exception wrapped exactly as the auto-commit wrapper wraps it
 * ({@code TransactionException("Error on executing command", cause)}) and asserts the logger receives the
 * throwable (the real cause), so the trace is actually written. Before the fix, the captured throwable is null
 * and this test fails.
 */
class Issue600InternalErrorStackTraceTest {

  @Test
  void internalErrorDuringCommandLogsFullStackTrace() {
    // The client's exact shape: a BufferUnderflowException (null message) wrapped by the auto-commit
    // transaction wrapper as TransactionException("Error on executing command", cause).
    final BufferUnderflowException cause = new BufferUnderflowException();
    final TransactionException wrapped = new TransactionException("Error on executing command", cause);

    final AtomicReference<Throwable> loggedThrowable = new AtomicReference<>();
    final boolean[] sawExecutionLog = { false };

    final Logger original = installCapturingLogger(loggedThrowable, sawExecutionLog);
    final HandledResponse response;
    try {
      response = handle(wrapped);
    } finally {
      LogManager.instance().setLogger(original);
    }

    // 1) THE WIRE CONTRACT STILL MATCHES THE CLIENT REPORT: 500 + BufferUnderflowException in the body.
    assertThat(response.statusCode).isEqualTo(500);
    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("error")).isEqualTo("Error on transaction commit");
    assertThat(json.getString("exception")).isEqualTo(BufferUnderflowException.class.getName());

    // 2) THE FIX: the internal error is logged WITH the throwable, so a real stack trace is emitted.
    assertThat(sawExecutionLog[0]).as("the 'Error on transaction execution' line must be logged").isTrue();
    assertThat(loggedThrowable.get())
        .as("the internal error must be logged WITH its throwable so the stack trace is printed (issue #600)")
        .isNotNull();
    // And it must be the REAL cause (BufferUnderflowException), not the opaque wrapper, so the trace is useful.
    assertThat(loggedThrowable.get()).isInstanceOf(BufferUnderflowException.class);
  }

  /**
   * Installs a {@link Logger} that captures the {@link Throwable} passed alongside the
   * "Error on transaction execution" line, returning the previous logger for restore.
   */
  private Logger installCapturingLogger(final AtomicReference<Throwable> loggedThrowable, final boolean[] sawExecutionLog) {
    final Logger capturing = new Logger() {
      private void record(final String message, final Throwable throwable) {
        if (message != null && message.startsWith("Error on transaction execution")) {
          sawExecutionLog[0] = true;
          if (throwable != null)
            loggedThrowable.set(throwable);
        }
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable throwable,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        record(message, throwable);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable throwable,
          final String context, final Object... args) {
        record(message, throwable);
      }

      @Override
      public void flush() {
      }
    };
    // Restore a fresh DefaultLogger in the caller's finally (the sanctioned test pattern, see DefaultLogger).
    final Logger previous = new DefaultLogger();
    LogManager.instance().setLogger(capturing);
    return previous;
  }

  private record HandledResponse(int statusCode, String body) {
  }

  /** Runs the real catch chain against a handler whose execute() throws, capturing status and body. */
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

  /** Handler whose execute() throws, standing in for a command execution that fails internally. */
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
      return false;
    }
  }
}
