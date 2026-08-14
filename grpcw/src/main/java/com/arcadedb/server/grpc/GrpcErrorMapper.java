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

import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.HAServerPlugin;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutionException;

/**
 * Central, consistent mapping from ArcadeDB engine exceptions to {@link io.grpc.Status} codes.
 * <p>
 * The response protobufs only carry a free-text message, which erases the original exception type. That
 * breaks type-driven client behavior, most importantly {@code RemoteDatabase.transaction()}'s automatic
 * retry on {@link NeedRetryException}. To preserve the type across the wire (the way the HTTP protocol
 * ships the exception class name), this mapper attaches the fully-qualified exception class name in a
 * metadata trailer so the client can reconstruct the exact type; for a {@link DuplicatedKeyException} the
 * index name and keys are carried as well, and for a {@link ServerIsNotTheLeaderException} the address of the
 * node the caller should be talking to instead.
 */
public final class GrpcErrorMapper {
  /** Trailer carrying the fully-qualified engine exception class name so the client can rebuild the type. */
  public static final Metadata.Key<String> EXCEPTION_CLASS_KEY = Metadata.Key.of("arcadedb-exception-class",
      Metadata.ASCII_STRING_MARSHALLER);
  /**
   * Trailer carrying the index name for a {@link DuplicatedKeyException}, Base64-encoded.
   * The value is Base64 so arbitrary index names (unicode, control characters) survive the ASCII-only
   * gRPC metadata channel intact; the client Base64-decodes it back.
   */
  public static final Metadata.Key<String> DUP_INDEX_KEY        = Metadata.Key.of("arcadedb-dup-index",
      Metadata.ASCII_STRING_MARSHALLER);
  /**
   * Trailer carrying the offending keys for a {@link DuplicatedKeyException}, Base64-encoded.
   * The value is Base64 because indexed key values can be arbitrary user data (unicode, non-Latin
   * scripts, control characters) that ASCII gRPC metadata cannot carry losslessly.
   */
  public static final Metadata.Key<String> DUP_KEYS_KEY         = Metadata.Key.of("arcadedb-dup-keys",
      Metadata.ASCII_STRING_MARSHALLER);

  private GrpcErrorMapper() {
  }

  /**
   * Unwraps an {@link ExecutionException} (raised when work runs on a transaction's dedicated executor) to
   * expose the real engine cause.
   */
  public static Throwable unwrap(final Throwable t) {
    if (t instanceof ExecutionException && t.getCause() != null)
      return t.getCause();
    return t;
  }

  /**
   * Maps a throwable to a {@link StatusRuntimeException} suitable for {@code StreamObserver.onError()}, with no
   * cluster to ask where the leader is. Equivalent to {@link #toStatusRuntimeException(Throwable, String,
   * HAServerPlugin)} with a null plugin; a leader refusal mapped this way still carries whatever address the
   * exception itself knows.
   *
   * @param t             the throwable to map (may be an {@link ExecutionException} wrapping the cause)
   * @param contextPrefix optional short prefix for the client-facing description (e.g. "Commit failed")
   */
  public static StatusRuntimeException toStatusRuntimeException(final Throwable t, final String contextPrefix) {
    return toStatusRuntimeException(t, contextPrefix, null);
  }

  /**
   * Maps a throwable to a {@link StatusRuntimeException} suitable for {@code StreamObserver.onError()}.
   * An already-mapped gRPC status (e.g. the security status produced by {@code getDatabase}) is passed
   * through unchanged so it is never masked as {@code INTERNAL}.
   * <p>
   * When the cause is a {@link ServerIsNotTheLeaderException} - whether raised by an explicit leadership check
   * or by the replicated database refusing a schema change on a follower - the answer additionally names where
   * the caller should go, on the {@link LeaderRedirectProtocol} trailers (issue #6183), instead of only the one
   * RPC that happened to build the trailers by hand.
   * <p>
   * That covers the RPCs that report through this mapper: {@code executeCommand}, {@code createRecord},
   * {@code beginTransaction}, {@code commitTransaction} and {@code graphBatchLoad}. Handlers that assemble a
   * {@link Status} themselves ({@code updateRecord}, {@code lookupByRid}, the streaming and bulk-insert paths)
   * do not pass here and would carry no redirect - none of them can raise this exception today, since they
   * neither check leadership nor mutate schema, but a handler that grows either has to route its errors through
   * this mapper to stay redirectable.
   *
   * @param t             the throwable to map (may be an {@link ExecutionException} wrapping the cause)
   * @param contextPrefix optional short prefix for the client-facing description (e.g. "Commit failed")
   * @param ha            this server's HA plugin, or null when HA is inactive or unavailable to the caller
   */
  public static StatusRuntimeException toStatusRuntimeException(final Throwable t, final String contextPrefix,
      final HAServerPlugin ha) {
    final Throwable cause = unwrap(t);

    // Pass through statuses already chosen upstream (security, resource-exhausted, etc.).
    if (cause instanceof StatusRuntimeException sre)
      return sre;
    if (cause instanceof StatusException se)
      return new StatusRuntimeException(se.getStatus(), se.getTrailers());

    final Metadata trailers = new Metadata();
    trailers.put(EXCEPTION_CLASS_KEY, cause.getClass().getName());

    final Status.Code code;
    String redirect = null;
    if (cause instanceof ServerIsNotTheLeaderException notTheLeader) {
      // FAILED_PRECONDITION, not the ABORTED its NeedRetryException ancestry would earn: retrying this call
      // as it stands means asking the same follower again. It is the leader-only work that is impossible
      // here, not a conflict that another attempt could win - the same reading the HTTP protocol takes when
      // it answers 400 rather than 503. What makes it actionable is the address, not a retry.
      code = Status.Code.FAILED_PRECONDITION;
      redirect = attachLeaderRedirect(trailers, ha, notTheLeader);
    } else if (cause instanceof DuplicatedKeyException dup) {
      code = Status.Code.ALREADY_EXISTS;
      if (dup.getIndexName() != null)
        trailers.put(DUP_INDEX_KEY, encodeTrailer(dup.getIndexName()));
      if (dup.getKeys() != null)
        trailers.put(DUP_KEYS_KEY, encodeTrailer(dup.getKeys()));
    } else if (cause instanceof NeedRetryException) {
      // Covers ConcurrentModificationException (a NeedRetryException subclass): retryable conflict.
      code = Status.Code.ABORTED;
    } else if (cause instanceof RecordNotFoundException) {
      code = Status.Code.NOT_FOUND;
    } else if (cause instanceof TimeoutException) {
      code = Status.Code.DEADLINE_EXCEEDED;
    } else if (cause instanceof SecurityException) {
      // Matches java.lang.SecurityException. ArcadeDB's ServerSecurityException does NOT extend it, but
      // security failures are already pre-mapped to a StatusRuntimeException by getDatabase and returned
      // via the pass-through branch above, so they never reach here as a raw exception. If a future
      // onError path hands a raw ServerSecurityException to this mapper it would fall through to INTERNAL.
      code = Status.Code.PERMISSION_DENIED;
    } else if (cause instanceof IllegalArgumentException) {
      code = Status.Code.INVALID_ARGUMENT;
    } else {
      code = Status.Code.INTERNAL;
    }

    final String msg = cause.getMessage() != null ? cause.getMessage() : cause.toString();
    final String prefixed = contextPrefix != null && !contextPrefix.isBlank() ? contextPrefix + ": " + msg : msg;
    final String description = redirect != null ? prefixed + ". " + redirect : prefixed;

    return code.toStatus().withDescription(description).withCause(cause).asRuntimeException(trailers);
  }

  /**
   * Puts the leader's addresses on the trailers of a refusal a follower is answering with, and returns the
   * sentence that says the same thing to whoever reads the description. Two forms, because a message a person
   * reads and a value a client can act on are not the same thing:
   * <ul>
   *   <li>the leader's client-reachable <b>gRPC</b> address on {@code LEADER_GRPC_ADDRESS}, the address the
   *       refused call can actually be retried on. Present when the cluster can resolve one - either a
   *       {@code grpc:} field in {@code arcadedb.ha.serverList}, or a deployment homogeneous enough for the
   *       derive-from-local-port fallback to be unambiguous (issue #6183);</li>
   *   <li>the leader's <b>HTTP</b> address on {@code LEADER_HTTP_ADDRESS}, which is known whenever a leader is
   *       but is not an address this call can be retried on. It is the diagnostic of last resort.</li>
   * </ul>
   * The plugin is asked first because it reads the cluster as it is right now; the address the exception was
   * built with is the fallback for a refusal raised where no plugin was in reach. The wording is not a
   * contract: a client redirecting itself reads the trailers, which the client-side mapper turns back into a
   * {@link ServerIsNotTheLeaderException} carrying the address - the same type the HTTP protocol raises here.
   */
  private static String attachLeaderRedirect(final Metadata trailers, final HAServerPlugin ha,
      final ServerIsNotTheLeaderException cause) {
    String grpcLeader = null;
    String httpLeader = null;
    if (ha != null) {
      // One routing-table read: a concurrent election cannot make the address named here disagree with the
      // leader the rest of the answer is about.
      final HAServerPlugin.RoutingTable routing = ha.getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC);
      grpcLeader = blankToNull(routing != null ? routing.writer() : null);
      httpLeader = blankToNull(ha.getLeaderAddress());
    }
    if (httpLeader == null)
      httpLeader = blankToNull(cause.getLeaderAddress());

    if (grpcLeader != null)
      trailers.put(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS, grpcLeader);
    if (httpLeader != null)
      trailers.put(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS, httpLeader);

    if (grpcLeader != null)
      return "Reconnect to the leader at '" + grpcLeader + "' (gRPC address) and retry";
    if (httpLeader != null)
      return "Reconnect to the leader at '" + httpLeader + "' (HTTP address; use its gRPC port) and retry";
    return "The leader is currently unknown, retry once the election has settled";
  }

  /** An address the cluster could not resolve and one it resolved to nothing are the same answer here. */
  private static String blankToNull(final String address) {
    return address != null && !address.isBlank() ? address : null;
  }

  /**
   * Base64-encodes a trailer value so arbitrary (possibly non-ASCII) index names and key values survive
   * the ASCII-only gRPC metadata channel. The client decodes it with the mirror method.
   */
  static String encodeTrailer(final String value) {
    return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }
}
