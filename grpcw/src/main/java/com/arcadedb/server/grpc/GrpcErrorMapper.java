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

import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
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

  /**
   * The description a CONCEALED failure carries in place of the exception's own message, when the server runs in
   * production mode - {@link ArcadeDBServer#CONCEALED_ERROR_MESSAGE}, so this transport and the control plane's SSE
   * frames say the same thing rather than two things that merely mean the same (issue #7472).
   */
  static final String CONCEALED_DESCRIPTION = ArcadeDBServer.CONCEALED_ERROR_MESSAGE;

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
   * Every RPC in {@code ArcadeDbGrpcService} now reports through this mapper (issue #7123 finished wiring in
   * {@code updateRecord}, {@code lookupByRid}, {@code deleteRecord} and the streaming/bulk-insert paths, which
   * used to hand-roll a narrower {@code RecordNotFoundException}-or-{@code INTERNAL} ladder of their own and so
   * answered a SQL syntax error, a missing type, a division by zero or a permission denial all as
   * {@code INTERNAL}). A handler that cannot raise {@link ServerIsNotTheLeaderException} simply never triggers
   * that branch; it still gets the full classification from {@link #statusCodeFor}.
   *
   * @param t             the throwable to map (may be an {@link ExecutionException} wrapping the cause)
   * @param contextPrefix optional short prefix for the client-facing description (e.g. "Commit failed")
   * @param ha            this server's HA plugin, or null when HA is inactive or unavailable to the caller
   */
  public static StatusRuntimeException toStatusRuntimeException(final Throwable t, final String contextPrefix,
      final HAServerPlugin ha) {
    return toStatusRuntimeException(t, contextPrefix, ha, false);
  }

  /**
   * Same as {@link #toStatusRuntimeException(Throwable, String, HAServerPlugin)}, with the server's production-mode
   * concealment applied.
   * <p>
   * In production the free-form part of the description - the exception's own message, which can carry file paths,
   * engine internals and schema names - is replaced by {@link #CONCEALED_DESCRIPTION}. Everything a client can ACT
   * on survives, and for exactly the reason the HTTP body keeps its {@code exception}/{@code exceptionArgs} fields:
   * the status CODE, the {@link #EXCEPTION_CLASS_KEY} trailer the driver rebuilds the typed exception from, the
   * duplicated-key trailers and the leader-redirect address and sentence are all bounded, structured values rather
   * than free text, and the remote driver and HA depend on them.
   * <p>
   * The gRPC surfaces reported the raw message whatever {@code arcadedb.server.mode} said, so they silently opted
   * out of the concealment the rest of the server applies (issue #7472).
   *
   * @param conceal true when the server runs in production mode - see {@code ArcadeDBServer.isProductionMode()}
   */
  public static StatusRuntimeException toStatusRuntimeException(final Throwable t, final String contextPrefix,
      final HAServerPlugin ha, final boolean conceal) {
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
      addDuplicatedKeyTrailers(trailers, dup);
    } else if (cause instanceof DatabaseOperationInProgressException) {
      // A backup, restore or import of the same database already holds the per-database maintenance slot, so a
      // SQL 'BACKUP DATABASE' or 'IMPORT DATABASE' sent through ExecuteCommand was refused. ABORTED is what the
      // admin service already answers for the server's own trigger/restore/import RPCs, and HTTP's 409: the
      // request is well formed and authorized, and retrying once the other operation finishes is the fix
      // (issue #7443). Without this arm it would read as INTERNAL - a server fault the caller cannot act on.
      code = Status.Code.ABORTED;
    } else {
      code = statusCodeFor(cause);
    }

    final String msg = conceal ?
        CONCEALED_DESCRIPTION :
        cause.getMessage() != null ? cause.getMessage() : cause.toString();
    final String prefixed = contextPrefix != null && !contextPrefix.isBlank() ? contextPrefix + ": " + msg : msg;
    // THE LEADER-REDIRECT SENTENCE SURVIVES CONCEALMENT: IT IS AN ADDRESS THIS SERVER PUT THERE, NOT INTERNAL
    // DETAIL FROM AN EXCEPTION, AND IT IS THE ONLY THING THAT MAKES A FOLLOWER'S REFUSAL ACTIONABLE
    final String description = redirect != null ? prefixed + ". " + redirect : prefixed;

    return code.toStatus().withDescription(description).withCause(cause).asRuntimeException(trailers);
  }

  /**
   * Maps everything not already special-cased above (leader redirects, duplicated keys, and the maintenance-slot
   * conflict all need extra trailers or a distinct reason to earn their own branch) through {@link ErrorCategory},
   * the classification every other wire protocol already answers a failure with (issue #7123): a SQL syntax
   * error, a missing type, a division by zero and a permission denial used to all read as {@code INTERNAL} here -
   * "server broke, safe to retry" to a driver's retry policy - when none of the four will ever succeed on retry,
   * and none but the first is actually the server's fault.
   * <p>
   * Package-visible (not just used by {@link #toStatusRuntimeException}) for the handlers - {@code graphBatchLoad}
   * is the current example - that must attach their own trailers (a partial-commit summary) alongside the
   * classified code rather than the trailer set this class builds for {@code EXCEPTION_CLASS_KEY}/dup-key details.
   */
  static Status.Code statusCodeFor(final Throwable cause) {
    return switch (ErrorCategory.of(cause)) {
      case RETRY -> Status.Code.ABORTED;
      case ARITHMETIC -> Status.Code.OUT_OF_RANGE;
      // Unreachable from toStatusRuntimeException/classifyAndAddTrailers, which both special-case
      // DuplicatedKeyException before reaching here - but ArcadeDbGrpcService.toSearchStatus calls this
      // method directly with no such pre-check, so this arm exists for that caller (and any future one),
      // not just to keep the switch exhaustive over ErrorCategory.
      case DUPLICATED_KEY -> Status.Code.ALREADY_EXISTS;
      case NOT_FOUND, SCHEMA -> Status.Code.NOT_FOUND;
      case SECURITY -> Status.Code.PERMISSION_DENIED;
      case VALIDATION, PARSING -> Status.Code.INVALID_ARGUMENT;
      case TIMEOUT -> Status.Code.DEADLINE_EXCEEDED;
      case SERVER -> Status.Code.INTERNAL;
    };
  }

  /**
   * Same classification as {@link #toStatusRuntimeException}, but layers this class's own trailers (the
   * exception class name, and for a {@link DuplicatedKeyException} the index/keys) onto a trailer set the
   * caller already owns, rather than building a standalone one - for a handler like {@code graphBatchLoad}
   * that must attach its own trailers (a partial-commit summary) alongside these. Before this, such a handler
   * had to call {@link #statusCodeFor} directly and lost the {@code DUP_INDEX_KEY}/{@code DUP_KEYS_KEY}
   * trailers that {@code executeCommand}/{@code createRecord} attach for the identical
   * {@link DuplicatedKeyException} (code review on issue #7123).
   * <p>
   * Also preserves a status already chosen upstream, the same as {@link #toStatusRuntimeException} - a
   * {@code getDatabase()} auth/authz refusal reaches {@code graphBatchLoad} as a raw
   * {@link StatusRuntimeException}/{@link StatusException} the same way it reaches every other RPC, and
   * without this check {@link ErrorCategory#of} would not recognise it and fold it into {@code SERVER}
   * (code review on issue #7123).
   */
  static Status.Code classifyAndAddTrailers(final Throwable t, final Metadata trailers) {
    final Throwable cause = unwrap(t);
    if (cause instanceof StatusRuntimeException sre) {
      if (sre.getTrailers() != null)
        trailers.merge(sre.getTrailers());
      return sre.getStatus().getCode();
    }
    if (cause instanceof StatusException se) {
      if (se.getTrailers() != null)
        trailers.merge(se.getTrailers());
      return se.getStatus().getCode();
    }
    trailers.put(EXCEPTION_CLASS_KEY, cause.getClass().getName());
    if (cause instanceof DuplicatedKeyException dup) {
      addDuplicatedKeyTrailers(trailers, dup);
      return Status.Code.ALREADY_EXISTS;
    }
    return statusCodeFor(cause);
  }

  private static void addDuplicatedKeyTrailers(final Metadata trailers, final DuplicatedKeyException dup) {
    if (dup.getIndexName() != null)
      trailers.put(DUP_INDEX_KEY, encodeTrailer(dup.getIndexName()));
    if (dup.getKeys() != null)
      trailers.put(DUP_KEYS_KEY, encodeTrailer(dup.getKeys()));
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
