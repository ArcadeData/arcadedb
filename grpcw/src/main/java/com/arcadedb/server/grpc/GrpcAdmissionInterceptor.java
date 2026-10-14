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
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Set;

/**
 * Makes the RPCs of {@code ArcadeDbService} wait for the query admission gate (issue #9518) like the requests of every
 * other protocol: in arrival order, when a slot is free and the running queries leave enough of the heap budget.
 * <p>
 * A unary or server-streaming RPC runs its handler in {@code onHalfClose}, on a thread of the gRPC executor, so that is
 * where it is admitted. The slot goes back BEFORE the call is closed towards the client, which is what a blocking client
 * waits for, and at the latest when the handler returns: a client that has just read the answer never finds its own slot
 * still held, so one slot and no queue serve a sequential client without refusing it (issue #10053). A refused RPC is
 * closed with the status the service gives every retryable failure ({@code ABORTED}, through {@link GrpcErrorMapper})
 * before its handler ever ran, so it leaves nothing behind.
 * <p>
 * The same RPCs are registered in the server's running statements once admitted (issue #9689), so {@code list queries}
 * and {@code SHOW TRANSACTIONS} list them and a terminate stops them. Unlike the slot, the entry goes only once the handler
 * has returned, after whatever it still cleans up behind its answer. A client that gives up on the call - it cancels it,
 * its deadline expires, its connection drops - terminates the entry: nobody is left to read the answer of the work.
 * <p>
 * Not gated: the admin service, health and reflection, which must stay answerable on a server busy with queries;
 * {@code BeginTransaction} and {@code RollbackTransaction}, which only manage a transaction and must never wait behind the
 * queries a rollback would release; and the client-streaming loads ({@code InsertStream}, {@code InsertBidirectional},
 * {@code GraphBatchLoad}, {@code TimeSeriesWriteStream}), whose handler holds per-stream state from the first chunk on:
 * a refusal between two chunks would have to go through each stream's own error path rather than close the call
 * under it. Not gated is not unlisted: those four open their registry entry themselves, for the whole stream, once the
 * first chunk names the database ({@link StreamingLoadEntry}, issue #9875).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GrpcAdmissionInterceptor implements ServerInterceptor {
  /** The peer of a client-streaming load, for the entry its handler opens ({@link StreamingLoadEntry}, issue #9962). */
  static final Context.Key<RunningQuery.Client> CLIENT_KEY = Context.key("arcadedb-client");
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
    if (!isGated(method)) {
      // A client-streaming load opens its entry itself, from the handler, where the call is out of reach: it reads its
      // client from the Context (issue #9962)
      if (isStreamingLoad(method))
        return Contexts.interceptCall(Context.current().withValue(CLIENT_KEY, client(call)), call, headers, next);
      return next.startCall(call, headers);
    }

    // The handler closes THIS call, so the slot can go back at the moment the answer leaves the server (issue #10053)
    final AdmittedCall<ReqT, RespT> admittedCall = new AdmittedCall<>(call);

    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(admittedCall, headers)) {
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
        admittedCall.admission = admission;
        final RunningQuery runningQuery = register(method, headers, request, client(call));
        // A cancelled call cancels its Context at once, on a thread of its own, while the handler still runs here
        final Context context = Context.current();
        final Context.CancellationListener onCancel = cancelled -> runningQuery.terminate("client (call cancelled)");
        context.addListener(onCancel, Runnable::run);
        try {
          super.onHalfClose();
        } finally {
          context.removeListener(onCancel);
          try {
            // A NO-OP WHEN THE CALL WAS CLOSED BEFORE: THE TICKET GIVES THE SLOT BACK ONCE
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
   * The call the handler answers on. Its {@code close} - the status a blocking client or a stream iterator waits for - gives
   * the admission ticket back first, so a client that has read the end of a call can start the next one at once (issue
   * #10053). The ticket is closed again, to no effect, when the handler returns; the call may be closed from any thread, and
   * a ticket is safe to close from one that did not take it.
   */
  private static final class AdmittedCall<ReqT, RespT> extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> {
    // SET BY THE LISTENER ON THE THREAD THAT ADMITS THE CALL, BEFORE THE HANDLER THAT CLOSES IT RUNS
    private volatile QueryAdmissionGate.Ticket admission;

    private AdmittedCall(final ServerCall<ReqT, RespT> delegate) {
      super(delegate);
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
      final QueryAdmissionGate.Ticket ticket = admission;
      if (ticket != null)
        ticket.close();
      super.close(status, trailers);
    }
  }

  /**
   * Opens the registry entry of an admitted RPC on the calling thread, described by its request when it carries a
   * statement (issue #9689). A transaction's statements run on the transaction's own thread, where the service binds it.
   */
  private RunningQuery register(final MethodDescriptor<?, ?> method, final Metadata headers, final Object request,
      final RunningQuery.Client client) {
    // Who the call was authenticated as, by its headers or its session: never a name the payload carries
    final String user = GrpcAuthInterceptor.USER_CONTEXT_KEY.get();
    String database = requestDatabase(request);
    if (database == null || database.isEmpty())
      database = headers.get(DATABASE_HEADER);
    String language = null;
    String text = method.getBareMethodName();
    if (request instanceof ExecuteQueryRequest query) {
      language = query.getLanguage();
      text = query.getQuery();
    } else if (request instanceof ExecuteCommandRequest command) {
      language = command.getLanguage();
      text = command.getCommand();
    } else if (request instanceof StreamQueryRequest stream) {
      language = stream.getLanguage();
      text = stream.getQuery();
    }
    if (language != null && language.isEmpty())
      language = "sql";

    final RunningQuery runningQuery = server.getRunningQueries()
        .register(database != null && !database.isEmpty() ? database : null, user, "grpc", null, null, client);
    runningQuery.setStatement(language, text);
    return runningQuery;
  }

  /**
   * The database a request names in its own {@code database} field - every data RPC of the service carries one - or
   * {@code null}. Read through the message descriptor, so an RPC added later is covered without being listed here.
   */
  private static String requestDatabase(final Object request) {
    if (!(request instanceof Message message))
      return null;
    final Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByName("database");
    return field != null && field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING ?
        (String) message.getField(field) :
        null;
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

  /** The client-streaming loads of the service, which are not gated but open their own entry ({@link StreamingLoadEntry}). */
  static boolean isStreamingLoad(final MethodDescriptor<?, ?> method) {
    return ArcadeDbServiceGrpc.SERVICE_NAME.equals(method.getServiceName()) && !method.getType().clientSendsOneMessage();
  }

  /**
   * The client of {@code call}, as {@code list queries} names it beside the user (issue #9962): its peer, when the
   * transport reports one.
   */
  static RunningQuery.Client client(final ServerCall<?, ?> call) {
    return RunningQuery.Client.of(null, call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
  }
}
