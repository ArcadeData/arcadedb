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

import com.arcadedb.exception.QueryAdmissionException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.server.ArcadeDBServer;
import io.grpc.Context;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.StatusRuntimeException;

import java.util.Set;

/**
 * Makes the RPCs of {@code ArcadeDbService} wait for the query admission gate (issue #9518) like the requests of every
 * other protocol: in arrival order, when a slot is free and the running queries leave enough of the heap budget.
 * <p>
 * A unary or server-streaming RPC runs its handler in {@code onHalfClose}, on a thread of the gRPC executor, so that is
 * where it is admitted, and the slot goes back when the handler returns. A refused RPC is closed with the status the
 * service gives every retryable failure ({@code ABORTED}, through {@link GrpcErrorMapper}) before its handler ever ran,
 * so it leaves nothing behind.
 * <p>
 * The same RPCs are registered in the server's running statements once admitted (issue #9689), so {@code list queries}
 * and {@code SHOW TRANSACTIONS} list them and a terminate stops them, and the entry goes once the handler returns. A
 * client that gives up on the call - it cancels it, its deadline expires, its connection drops - terminates the entry:
 * nobody is left to read the answer of the work.
 * <p>
 * Not gated: the admin service, health and reflection, which must stay answerable on a server busy with queries;
 * {@code BeginTransaction} and {@code RollbackTransaction}, which only manage a transaction and must never wait behind the
 * queries a rollback would release; and the client-streaming loads ({@code InsertStream}, {@code InsertBidirectional},
 * {@code GraphBatchLoad}, {@code TimeSeriesWriteStream}), whose handler holds per-stream state from the first chunk on:
 * a refusal between two chunks would have to go through each stream's own error path rather than close the call
 * under it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GrpcAdmissionInterceptor implements ServerInterceptor {
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);
  private static final Set<String>          NOT_GATED       = Set.of(//
      ArcadeDbServiceGrpc.getBeginTransactionMethod().getFullMethodName(),//
      ArcadeDbServiceGrpc.getRollbackTransactionMethod().getFullMethodName());

  private final ArcadeDBServer server;

  GrpcAdmissionInterceptor(final ArcadeDBServer server) {
    this.server = server;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call, final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    final MethodDescriptor<ReqT, RespT> method = call.getMethodDescriptor();
    if (!isGated(method))
      return next.startCall(call, headers);

    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
      // The request, which says what the statement is: a gated RPC sends exactly one
      private Object request;

      @Override
      public void onMessage(final ReqT message) {
        request = message;
        super.onMessage(message);
      }

      @Override
      public void onHalfClose() {
        final QueryAdmissionGate.Ticket admission;
        try {
          admission = QueryAdmissionGate.getInstance().admit();
        } catch (final QueryAdmissionException e) {
          final StatusRuntimeException refusal = GrpcErrorMapper.toStatusRuntimeException(e, null, null, concealErrors());
          call.close(refusal.getStatus(), refusal.getTrailers() != null ? refusal.getTrailers() : new Metadata());
          return;
        }
        final RunningQuery runningQuery = register(method, headers, request);
        // A cancelled call cancels its Context at once, on a thread of its own, while the handler still runs here
        final Context context = Context.current();
        final Context.CancellationListener onCancel = cancelled -> runningQuery.terminate("client (call cancelled)");
        context.addListener(onCancel, Runnable::run);
        try {
          super.onHalfClose();
        } finally {
          context.removeListener(onCancel);
          try {
            admission.close();
          } finally {
            // LAST: THE ENTRY LEAVING THE LIST IS THE PROOF THE WORK IS OVER
            runningQuery.close();
          }
        }
      }
    };
  }

  /**
   * Opens the registry entry of an admitted RPC on the calling thread, described by its request when it carries a
   * statement (issue #9689). A transaction's statements run on the transaction's own thread, where the service binds it.
   */
  private RunningQuery register(final MethodDescriptor<?, ?> method, final Metadata headers, final Object request) {
    String database = headers.get(DATABASE_HEADER);
    String user = GrpcAuthInterceptor.USER_CONTEXT_KEY.get();
    String language = null;
    String text = method.getBareMethodName();
    DatabaseCredentials credentials = null;
    if (request instanceof ExecuteQueryRequest query) {
      database = query.getDatabase();
      language = query.getLanguage();
      text = query.getQuery();
      credentials = query.hasCredentials() ? query.getCredentials() : null;
    } else if (request instanceof ExecuteCommandRequest command) {
      database = command.getDatabase();
      language = command.getLanguage();
      text = command.getCommand();
      credentials = command.hasCredentials() ? command.getCredentials() : null;
    } else if (request instanceof StreamQueryRequest stream) {
      database = stream.getDatabase();
      language = stream.getLanguage();
      text = stream.getQuery();
      credentials = stream.hasCredentials() ? stream.getCredentials() : null;
    }
    if (user == null && credentials != null && !credentials.getUsername().isEmpty())
      // Authenticated by the credentials the request carries rather than by its headers
      user = credentials.getUsername();
    if (language != null && language.isEmpty())
      language = "sql";

    final RunningQuery runningQuery = server.getRunningQueries()
        .register(database != null && !database.isEmpty() ? database : null, user, "grpc", null, null);
    runningQuery.setStatement(language, text);
    return runningQuery;
  }

  /** Like the service: in production the refusal is answered without the exception's own text (issue #7472). */
  private boolean concealErrors() {
    return server != null && server.isProductionMode();
  }

  static boolean isGated(final MethodDescriptor<?, ?> method) {
    return ArcadeDbServiceGrpc.SERVICE_NAME.equals(method.getServiceName())//
        && method.getType().clientSendsOneMessage()//
        && !NOT_GATED.contains(method.getFullMethodName());
  }
}
