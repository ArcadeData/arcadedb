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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.grpc.LeaderRedirectProtocol;
import io.grpc.Metadata;
import io.grpc.Status;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Rebuilds the original ArcadeDB engine exception from a gRPC error returned by the server.
 * <p>
 * A server that maps its exceptions through {@code GrpcErrorMapper} ships the fully-qualified exception
 * class name in a metadata trailer. When that trailer is present the exact type is reconstructed - this is
 * what lets {@code RemoteDatabase.transaction()}'s retry-on-{@link NeedRetryException} behave over gRPC as
 * it does over HTTP, and what keeps a permanent {@link DuplicatedKeyException} from being mis-typed as a
 * retryable conflict. When the trailer is absent (an older server) the legacy status-code mapping is used,
 * so this is backward compatible.
 * <p>
 * The trailer key strings must stay in sync with the server-side {@code GrpcErrorMapper}.
 */
final class GrpcClientErrorMapper {
  static final Metadata.Key<String> EXCEPTION_CLASS_KEY = Metadata.Key.of("arcadedb-exception-class",
      Metadata.ASCII_STRING_MARSHALLER);
  static final Metadata.Key<String> DUP_INDEX_KEY        = Metadata.Key.of("arcadedb-dup-index",
      Metadata.ASCII_STRING_MARSHALLER);
  static final Metadata.Key<String> DUP_KEYS_KEY         = Metadata.Key.of("arcadedb-dup-keys",
      Metadata.ASCII_STRING_MARSHALLER);

  private static final int MAX_CAUSE_DEPTH = 32;

  private GrpcClientErrorMapper() {
  }

  /**
   * Converts a gRPC failure into the matching ArcadeDB runtime exception. Never returns {@code null}.
   */
  static RuntimeException toException(final Throwable e) {
    final Status status = Status.fromThrowable(e);
    final Metadata trailers = Status.trailersFromThrowable(e);
    final String msg = status.getDescription() != null ? status.getDescription() : status.getCode().name();

    final String exceptionClass = trailers != null ? trailers.get(EXCEPTION_CLASS_KEY) : null;
    if (exceptionClass != null) {
      final RuntimeException reconstructed = reconstructFromClassName(exceptionClass, msg, trailers);
      if (reconstructed != null)
        return reconstructed;
    }

    // Legacy / trailer-less fallback: map by status code only.
    return switch (status.getCode()) {
      case NOT_FOUND -> new RecordNotFoundException(msg, null);
      case ALREADY_EXISTS -> new DuplicatedKeyException(dupIndex(trailers, msg), dupKeys(trailers, msg), null);
      case ABORTED -> new ConcurrentModificationException(msg);
      case DEADLINE_EXCEEDED -> new TimeoutException(msg);
      case PERMISSION_DENIED -> new SecurityException(msg);
      case UNAVAILABLE -> new NeedRetryException(msg);
      default -> new RemoteException("gRPC error: " + msg, e);
    };
  }

  /**
   * {@link #toException} for a failed {@code CommitTransaction}. A status-only {@code UNAVAILABLE} does not prove the
   * commit never landed, because the channel can drop after the request went out, so it is an unknown outcome and a
   * non-retryable {@link TransactionException}: {@code transaction()} must not re-run a scope whose first run may
   * already be durable (issues #8711, #8525). The HTTP client draws the same line. An error the server itself
   * classified (a class-name trailer, e.g. a conflict or a refusal by a follower) was produced before anything was
   * applied and keeps its type, and so does a failure to connect, which proves the request never left this client. A
   * client deadline that expired unanswered stays a {@link TimeoutException} but says the commit may have landed
   * (issue #8822).
   */
  static RuntimeException toCommitException(final Throwable e) {
    if (deadlineExpiredUnanswered(e))
      return new TimeoutException("Error on transaction commit: the deadline expired before the server answered and the "
          + "outcome is unknown, the transaction may have been committed (" + describe(e) + ")", e);
    if (!responseMayHaveBeenLost(e))
      return toException(e);
    return new TransactionException("Error on transaction commit: the connection was lost and the outcome is unknown, "
        + "the transaction may have been committed (" + describe(e) + ")", e);
  }

  /**
   * {@link #toException} for a failed RPC that applies and commits a write on its own, outside any client
   * transaction: a command, a record create/update/delete, a bulk insert or a time-series write sent without a
   * transaction id (issue #8525). Same line as {@link #toCommitException}: a status-only {@code UNAVAILABLE} that is
   * not a failure to connect may have been raised after the server applied the write and only its response was lost,
   * so it must not surface as a {@link NeedRetryException} - a caller honouring that type would apply the write a
   * second time and report success. It becomes a {@link RemoteException} saying the outcome is unknown, the message
   * the HTTP client raises for the same failure (issue #8136).
   * <p>
   * A client deadline that expired before any answer (a status-only {@code DEADLINE_EXCEEDED}) keeps its
   * {@link TimeoutException} type, which nothing replays, but says the same thing: the server may have applied the
   * write after the client stopped waiting (issue #8822). The admin writes of {@code RemoteGrpcServer} use this mapping
   * too, since none of them is sent again either.
   *
   * @param operation the RPC name, for the message only
   */
  static RuntimeException toAutoCommitWriteException(final Throwable e, final String operation) {
    if (deadlineExpiredUnanswered(e))
      return new TimeoutException("Error on executing remote operation '" + operation
          + "': the deadline expired before the server answered (" + describe(e)
          + "), so the server may already have applied it", e);
    if (!responseMayHaveBeenLost(e))
      return toException(e);
    return new RemoteException("Error on executing remote operation '" + operation
        + "': the connection failed and the request may already have reached the server (" + describe(e)
        + "), so the server may already have applied it. It is not reported as retryable, because a replay could apply"
        + " it twice", e);
  }

  /**
   * {@link #toAutoCommitWriteException} for a streaming write ({@code InsertStream}, {@code InsertBidirectional},
   * {@code TimeSeriesWriteStream}, {@code GraphBatchLoad}) that is not a dry run (issue #8822). These commit as they go
   * - per row, per batch or per chunk - so an answer lost mid-stream means that any part of the stream, from none of it
   * to all of it, may already be durable, and the message says so. Same carve-outs: a class-name trailer and a failure
   * to connect keep the plain mapping.
   *
   * @param operation the RPC name, for the message only
   */
  static RuntimeException toStreamingWriteException(final Throwable e, final String operation) {
    if (deadlineExpiredUnanswered(e))
      return new TimeoutException("Error on executing remote streaming operation '" + operation
          + "': the deadline expired before the server answered (" + describe(e)
          + "), so the server may already have applied some or all of it: the stream may be partially applied", e);
    if (!responseMayHaveBeenLost(e))
      return toException(e);
    return new RemoteException("Error on executing remote streaming operation '" + operation
        + "': the connection failed while the stream was open (" + describe(e)
        + "), so the server may already have applied some or all of it: the stream commits as it goes and may be "
        + "partially applied. It is not reported as retryable, because a replay could apply the applied part twice", e);
  }

  /**
   * Whether {@code e} is a {@code DEADLINE_EXCEEDED} the server did not classify (no class-name trailer): the client's
   * own deadline expired, and the server may still have applied the request. A trailer means the server raised the
   * timeout itself, before it applied anything, and its own message is kept.
   */
  static boolean deadlineExpiredUnanswered(final Throwable e) {
    if (Status.fromThrowable(e).getCode() != Status.Code.DEADLINE_EXCEEDED)
      return false;
    final Metadata trailers = Status.trailersFromThrowable(e);
    return trailers == null || trailers.get(EXCEPTION_CLASS_KEY) == null;
  }

  /**
   * Whether the outcome of a write that failed with {@code e} is unknown: a lost response or an expired client
   * deadline, as {@link #toAutoCommitWriteException} draws the line.
   */
  static boolean outcomeUnknown(final Throwable e) {
    return deadlineExpiredUnanswered(e) || responseMayHaveBeenLost(e);
  }

  /**
   * Whether {@code e} is a status-only {@code UNAVAILABLE} that may have been raised after the server received the
   * request. A class-name trailer means the server answered, so it did not apply anything it was not reporting; a
   * failure to connect means the request never left this client. Everything else - a reset, a GOAWAY, a channel
   * shut down mid-call - can follow a request the server already applied.
   * <p>
   * Over-reporting is deliberate: a client-side {@code Channel shutdown invoked} or a TLS handshake failure also lands
   * here although nothing went out, because the status alone cannot prove it. Reporting an unapplied write as "maybe
   * applied" costs the caller a check; narrowing this to save that check would let a replay apply a write twice.
   */
  static boolean responseMayHaveBeenLost(final Throwable e) {
    if (Status.fromThrowable(e).getCode() != Status.Code.UNAVAILABLE)
      return false;
    final Metadata trailers = Status.trailersFromThrowable(e);
    if (trailers != null && trailers.get(EXCEPTION_CLASS_KEY) != null)
      return false;
    return !provablyNeverSent(e);
  }

  /**
   * Whether the failure proves the request never reached the server: only a failure to establish the connection
   * does - a refused connection ({@link ConnectException}, which Netty's annotated variant extends) or a host that
   * does not resolve ({@link UnknownHostException}). gRPC carries that cause on the {@code UNAVAILABLE} status of a
   * call that could not get a transport.
   * <p>
   * Deliberately narrow: a {@code NoRouteToHostException} or a connect-phase {@code SocketTimeoutException} is not a
   * {@link ConnectException} and stays an unknown outcome. Erring that way costs a caller a retry decision; erring the
   * other way applies a write twice. The walk is depth-bounded so a cyclic cause chain cannot hang it.
   */
  static boolean provablyNeverSent(final Throwable e) {
    // The depth cap also ends a self-referencing or cyclic chain, so no identity check is needed
    Throwable t = e;
    for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (t instanceof ConnectException || t instanceof UnknownHostException)
        return true;
      t = t.getCause();
    }
    return false;
  }

  private static String describe(final Throwable e) {
    final Status status = Status.fromThrowable(e);
    return status.getDescription() != null ? status.getDescription() : status.getCode().name();
  }

  private static RuntimeException reconstructFromClassName(final String exceptionClass, final String msg,
      final Metadata trailers) {
    return switch (exceptionClass) {
      case "com.arcadedb.exception.DuplicatedKeyException" ->
          new DuplicatedKeyException(dupIndex(trailers, msg), dupKeys(trailers, msg), null);
      case "com.arcadedb.exception.ConcurrentModificationException" -> new ConcurrentModificationException(msg);
      case "com.arcadedb.exception.NeedRetryException" -> new NeedRetryException(msg);
      case "com.arcadedb.exception.RecordNotFoundException" -> new RecordNotFoundException(msg, null);
      case "com.arcadedb.exception.TimeoutException" -> new TimeoutException(msg);
      // Stopped on request (issue #9689): its own type, so no retry loop mistakes it for a failure worth repeating
      case "com.arcadedb.exception.QueryTerminatedException" -> new QueryTerminatedException(msg);
      case "java.lang.SecurityException" -> new SecurityException(msg);
      case "com.arcadedb.network.binary.ServerIsNotTheLeaderException" ->
          new ServerIsNotTheLeaderException(msg, leaderAddress(trailers));
      // SchemaException (a missing type/bucket/property) and RecordNotFoundException (a missing record)
      // both classify to the same NOT_FOUND status server-side (issue #7123's ErrorCategory), so without
      // this exact-type reconstruction a schema error was indistinguishable from - and, worse, silently
      // reconstructed AS - a RecordNotFoundException by the legacy status-code fallback below.
      case "com.arcadedb.exception.SchemaException" -> new SchemaException(msg);
      // Unknown class: let the caller fall back to status-code mapping.
      default -> null;
    };
  }

  /**
   * Returns the address to redirect to when a follower refused an RPC that only the leader may run: the
   * leader's gRPC address when the cluster could resolve one, otherwise its HTTP address. Both may be absent -
   * a refusal issued during an election names no leader at all - and the exception then carries a null address,
   * which is what {@code ServerIsNotTheLeaderException} means by "retry, destination unknown".
   * <p>
   * The gRPC address is preferred because it is the only one of the two this client can actually dial. The HTTP
   * address is kept as a fallback because it is always known when a leader is, and a caller that can map an
   * HTTP endpoint to its gRPC port is better off than one told nothing.
   */
  private static String leaderAddress(final Metadata trailers) {
    if (trailers == null)
      return null;
    final String grpcAddress = trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS);
    return grpcAddress != null ? grpcAddress : trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS);
  }

  /**
   * Returns the Base64-decoded index-name trailer, or {@code fallback} (the server-supplied message) when
   * the trailer is absent - e.g. talking to an older server that never emits it - so diagnostics survive.
   */
  private static String dupIndex(final Metadata trailers, final String fallback) {
    final String v = trailers != null ? trailers.get(DUP_INDEX_KEY) : null;
    return v != null ? decodeTrailer(v) : fallback;
  }

  /**
   * Returns the Base64-decoded keys trailer, or {@code fallback} (the server-supplied message) when the
   * trailer is absent, so the reconstructed exception keeps a useful message against pre-upgrade servers.
   */
  private static String dupKeys(final Metadata trailers, final String fallback) {
    final String v = trailers != null ? trailers.get(DUP_KEYS_KEY) : null;
    return v != null ? decodeTrailer(v) : fallback;
  }

  /**
   * Mirror of the server-side Base64 encoding used to carry arbitrary (possibly non-ASCII) trailer values.
   * Falls back to the raw value if it is not valid Base64, so a malformed trailer never masks the error.
   */
  private static String decodeTrailer(final String value) {
    try {
      return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    } catch (final IllegalArgumentException e) {
      return value;
    }
  }
}
