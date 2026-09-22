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

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Puts {@link TransactionProtocol#SESSION_PARTIAL_COMMIT_TRAILER} on the calls that earned it (issue #8134).
 * <p>
 * The interceptor owns only the plumbing: it gives each call a one-shot flag and, at close, turns a raised flag
 * into the trailer. What raises it is {@code ArcadeDbGrpcService}, which samples the commit counter of the
 * caller's transaction around the work it dispatches onto that transaction's dedicated thread - the same
 * {@code TransactionContext.isPartiallyCommitted(...)} predicate {@code DatabaseAbstractHandler} evaluates for
 * the HTTP header of the same name.
 * <p>
 * Only a call that actually ran work inside a CLIENT-MANAGED transaction can raise the flag. An auto-commit
 * command has no caller transaction to report on, exactly as a session-less HTTP request carries no header.
 * <p>
 * <b>Cost.</b> The flag lives ON the forwarding {@link ServerCall} this interceptor has to allocate anyway,
 * rather than in a separate holder next to it, so a call adds one field and no second object - the same concern
 * that put the HTTP side's listener registration behind its session check (code review on PR #8138). It cannot
 * be made conditional the way that one is: whether the call will touch a caller transaction is not known until
 * the service runs, which is after the wrapper must already exist.
 */
class GrpcSessionPartialCommitInterceptor implements ServerInterceptor {

  /**
   * The call currently in scope, or {@code null} when there is none. Read on the thread the RPC is dispatched
   * ON (where the Context is attached) and handed to the work that runs on the transaction's own thread, which
   * has no Context of its own.
   */
  private static final Context.Key<Verdict> CURRENT_CALL = Context.key("arcadedb-session-partial-commit");

  /**
   * The one thing the service needs from the call it is running under: somewhere to say "this call published a
   * commit under the caller's transaction". A non-generic view of {@link PartialCommitCall}, so the service can
   * hold one without naming the call's request and response types.
   */
  interface Verdict {
    void raisePartialCommit();
  }

  /**
   * The forwarding call, carrying its own verdict.
   * <p>
   * {@code volatile} because the verdict is raised on the transaction's dedicated executor thread and read here
   * on the gRPC thread: for a unary RPC the {@code Future.get()} that collects the result already orders the
   * two, but a streaming RPC raises it from work whose completion the closing thread does not join.
   */
  private static final class PartialCommitCall<ReqT, RespT>
      extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> implements Verdict {

    private volatile boolean partiallyCommitted;

    private PartialCommitCall(final ServerCall<ReqT, RespT> delegate) {
      super(delegate);
    }

    @Override
    public void raisePartialCommit() {
      partiallyCommitted = true;
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
      // On the failing close as much as on the successful one: the guard this feeds is consulted exactly when
      // something went wrong, and the commit that made half the block durable may well be the one the failing
      // statement itself published before raising.
      if (partiallyCommitted)
        trailers.put(TransactionProtocol.SESSION_PARTIAL_COMMIT_TRAILER,
            TransactionProtocol.SESSION_PARTIAL_COMMIT_VALUE);

      super.close(status, trailers);
    }
  }

  /**
   * The current call's verdict, or {@code null} when there is no call in scope - which is not a production case
   * to guard against elsewhere, but keeps the service's sampling harmless in a unit test that drives it off a
   * live call.
   */
  static Verdict currentVerdict() {
    return CURRENT_CALL.get();
  }

  /**
   * Raises a verdict obtained from {@link #currentVerdict()}. A no-op for a call that had none, so callers need
   * no null check of their own.
   */
  static void raise(final Verdict verdict) {
    if (verdict != null)
      verdict.raisePartialCommit();
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers, final ServerCallHandler<ReqT, RespT> next) {

    final PartialCommitCall<ReqT, RespT> wrappedCall = new PartialCommitCall<>(call);

    return Contexts.interceptCall(Context.current().withValue(CURRENT_CALL, wrappedCall), wrappedCall, headers, next);
  }
}
