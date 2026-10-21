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

import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ErrorConcealment;
import com.arcadedb.server.HAServerPlugin;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

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
   * Trailer carrying the {@link AlreadyExistsException.Kind} of the schema object that holds a taken name (issue #10153), so the
   * client rebuilds the exception with it. A {@link Enum#name()}, plain ASCII.
   */
  public static final Metadata.Key<String> ALREADY_EXISTS_KIND_KEY = Metadata.Key.of("arcadedb-already-exists-kind",
      Metadata.ASCII_STRING_MARSHALLER);
  /**
   * Trailer the admin service names a taken user, API token or database name in, with the refusal's class name (issue
   * #10143), so the client recognises one by what the server says rather than by the class-name trailer it leaves out. Not
   * {@link #EXCEPTION_CLASS_KEY}: a client that predates this trailer reads only a trailer-less {@code ALREADY_EXISTS} as a
   * taken name, and must keep doing so.
   */
  public static final Metadata.Key<String> NAME_TAKEN_KEY = Metadata.Key.of("arcadedb-name-taken",
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
   * When the cause is a {@link ServerIsNotTheLeaderException}, or carries one anywhere along its cause chain (the
   * engine wraps what it raises, issue #10089) - whether raised by an explicit leadership check
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
   * <p>
   * EVERY branch is concealed here, the classified ones included, and that is DELIBERATELY not what
   * {@code ArcadeDbGrpcAdminService.toStatus} does - it conceals only its catch-all and leaves its classified arms
   * alone. The two are not inconsistent, they are answering about different messages. The admin service's arms
   * carry sentences the SERVER wrote about the REQUEST ("Usage: delete backup &lt;database&gt; &lt;fileName&gt;",
   * "token name already issued"), which are bounded and carry nothing the caller did not send. The exceptions that
   * reach here carry ENGINE text: a {@link DuplicatedKeyException}'s message embeds the index name and the
   * offending key VALUE - actual stored data - and a {@link DatabaseOperationInProgressException}'s names a
   * database. Classified is not the same as safe to echo, so do not "fix" one of these to match the other.
   * <p>
   * What survives concealment is what a client can act on without reading prose: the status CODE, the
   * {@link #EXCEPTION_CLASS_KEY} trailer, the duplicated-key index trailer - which goes to a DRIVER rebuilding a typed
   * exception rather than into a human-readable description; the KEYS trailer carries a placeholder because key
   * values are stored data (issue #7760) - and the leader-redirect
   * address and sentence, which this server put there rather than an exception.
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
    // Found anywhere along the cause chain, because the engine wraps what it raises (a commit failure, a record-event listener,
    // a trigger), like every other transport does (issue #10089). A test of the head alone answered ABORTED with no leader
    // address for a refusal one frame down, and the driver retried the same follower until its retries ran out
    final ServerIsNotTheLeaderException refusal = ServerIsNotTheLeaderException.refusalIn(cause);
    final ErrorCategory category = ErrorCategory.of(cause);
    // Every classified failure is looked for along the whole chain, never with an instanceof on its head: the engine wraps
    // what it raises, and a wrapper must not change the answer (issue #10158). The maintenance-slot conflict is not an
    // ErrorCategory of its own, so it is found here, below the two verdicts that outrank everything else; the duplicated
    // key where ErrorCategory ranks it. Neither type takes a cause, so for one raised as itself this is the answer it
    // always had
    final DatabaseOperationInProgressException inProgress =
        refusal == null && category != ErrorCategory.TERMINATED ? CauseChain.find(cause, DatabaseOperationInProgressException.class) : null;
    final DuplicatedKeyException dup =
        inProgress == null && category == ErrorCategory.DUPLICATED_KEY ? CauseChain.find(cause, DuplicatedKeyException.class) : null;
    // The failure is named by the frame that classified it, whatever wraps it, so the driver rebuilds the type it must act
    // on: the one that tells it not to retry (#9689), the one that tells it where to go (#10089), and every other typed
    // failure (#10158) - a SchemaException named by its wrapper is rebuilt from NOT_FOUND alone, as a missing record
    final Throwable classified = refusal != null ? refusal : inProgress != null ? inProgress : ErrorCategory.classifiedCause(cause);
    // A taken schema object name (#10153) is answered ALREADY_EXISTS and named as a SchemaException, its supertype: a client
    // that predates #10153 does not know AlreadyExistsException, and for a class it does not know it falls back to the status,
    // which it reads as a duplicate key. As a SchemaException it rebuilds what the SchemaException-raising refusals came back as
    // before, and a current client tells it apart by the ALREADY_EXISTS status and the kind trailer
    final AlreadyExistsException taken = refusal == null && inProgress == null && category == ErrorCategory.ALREADY_EXISTS ?
        CauseChain.find(cause, AlreadyExistsException.class) :
        null;
    trailers.put(EXCEPTION_CLASS_KEY, category == ErrorCategory.TERMINATED ?
        QueryTerminatedException.class.getName() :
        taken != null ? SchemaException.class.getName() : (classified != null ? classified : cause).getClass().getName());
    if (taken != null && taken.getKind() != null)
      trailers.put(ALREADY_EXISTS_KIND_KEY, taken.getKind().name());

    final Status.Code code;
    String redirect = null;
    if (refusal != null) {
      // FAILED_PRECONDITION, not the ABORTED its NeedRetryException ancestry would earn: retrying this call
      // as it stands means asking the same follower again. It is the leader-only work that is impossible
      // here, not a conflict that another attempt could win - the same reading the HTTP protocol takes when
      // it answers 400 rather than 503. What makes it actionable is the address, not a retry.
      code = Status.Code.FAILED_PRECONDITION;
      redirect = attachLeaderRedirect(trailers, ha, refusal);
    } else if (inProgress != null) {
      // A backup, restore or import of the same database already holds the per-database maintenance slot, so a
      // SQL 'BACKUP DATABASE' or 'IMPORT DATABASE' sent through ExecuteCommand was refused. ABORTED is what the
      // admin service already answers for the server's own trigger/restore/import RPCs, and HTTP's 409: the
      // request is well formed and authorized, and retrying once the other operation finishes is the fix
      // (issue #7443). Without this arm it would read as INTERNAL - a server fault the caller cannot act on.
      code = Status.Code.ABORTED;
    } else if (dup != null) {
      code = Status.Code.ALREADY_EXISTS;
      addDuplicatedKeyTrailers(trailers, dup, conceal);
    } else {
      code = statusCodeFor(category);
    }

    if (conceal)
      logConcealed(GrpcErrorMapper.class, contextPrefix, cause);

    // A taken name keeps its label when concealed, as the HTTP body's 'error' field does: it names the kind, never the content
    final String msg = conceal ?
        taken != null ? taken.getLabel() : CONCEALED_DESCRIPTION :
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
   * Private on purpose (issue #10090). It used to be package-visible for the handlers that must attach their own
   * trailers (a partial-commit summary), but a handler that took the code from here alone lost everything else the
   * mapper does around it - the {@code EXCEPTION_CLASS_KEY} trailer, the termination rewrite, the leader redirect - and
   * {@code toSearchStatus} did exactly that for three RPCs. Those handlers reach the classification through
   * {@link #toStatusException}, which layers their trailers onto the full answer.
   */
  private static Status.Code statusCodeFor(final ErrorCategory category) {
    return switch (category) {
      case RETRY -> Status.Code.ABORTED;
      case ARITHMETIC -> Status.Code.OUT_OF_RANGE;
      // Unreachable from toStatusRuntimeException, which special-cases the duplicated key (it has trailers of its own to
      // add) before reaching here: the arm only keeps the switch exhaustive over ErrorCategory.
      case DUPLICATED_KEY -> Status.Code.ALREADY_EXISTS;
      // A taken schema object name (issue #10153): gRPC's own word for it. It used to be NOT_FOUND - the opposite of what
      // happened - for the SchemaException ones and INTERNAL for the CommandExecutionException ones
      case ALREADY_EXISTS -> Status.Code.ALREADY_EXISTS;
      case NOT_FOUND, SCHEMA -> Status.Code.NOT_FOUND;
      case SECURITY -> Status.Code.PERMISSION_DENIED;
      case VALIDATION, PARSING -> Status.Code.INVALID_ARGUMENT;
      case TIMEOUT -> Status.Code.DEADLINE_EXCEEDED;
      // Stopped on request (issue #9689): gRPC's own word for an operation somebody cancelled, never retried by a client
      case TERMINATED -> Status.Code.CANCELLED;
      case SERVER -> Status.Code.INTERNAL;
    };
  }

  /**
   * The exact answer {@link #toStatusRuntimeException(Throwable, String, HAServerPlugin)} builds - status code,
   * description and this class's own trailers - layered onto a trailer set the caller already owns, as a
   * {@link StatusException}. For a handler like {@code graphBatchLoad} that must attach its own trailers (a
   * partial-commit summary) alongside these.
   * <p>
   * It delegates rather than classifying again, which is the point: the hand-rolled variant this replaced knew
   * about the duplicated-key trailers and the already-mapped pass-through but <b>not</b> about
   * {@link ServerIsNotTheLeaderException}, so a leader refusal raised deep in the load - the replicated database
   * declining a schema change on a follower, as opposed to the explicit up-front leadership check - was
   * classified by {@link ErrorCategory} as {@link ErrorCategory#RETRY}, answered {@code ABORTED}, and dropped the
   * leader address the exception was built to carry. A client that retried went back to the same follower, which
   * refused again for the same reason, because nothing had told it where the leader was; the HTTP surface answers
   * the same refusal with the leader's address (issue #7624). One classification path cannot diverge from itself.
   *
   * @param t             the throwable to map (may be an {@link ExecutionException} wrapping the cause)
   * @param contextPrefix optional short prefix for the client-facing description (e.g. "graphBatchLoad")
   * @param ha            this server's HA plugin, or null when HA is inactive or unavailable to the caller
   * @param trailers      the caller's own trailers, added to in place and carried by the returned exception
   */
  static StatusException toStatusException(final Throwable t, final String contextPrefix, final HAServerPlugin ha,
      final Metadata trailers, final boolean conceal) {
    final StatusRuntimeException mapped = toStatusRuntimeException(t, contextPrefix, ha, conceal);
    if (mapped.getTrailers() != null)
      trailers.merge(mapped.getTrailers());
    return mapped.getStatus().asException(trailers);
  }

  /**
   * The description for a failure a caller maps DIRECTLY - keeping a status code it chose rather than one this
   * class would classify - with the exception's text concealed, and LOGGED, when the server runs in production.
   * <p>
   * One method for both halves because they are one decision: the concealed text tells the operator to check the
   * server log, and nothing else on these paths writes that entry - {@code GrpcLoggingInterceptor} sees the mapped
   * status rather than the cause, and {@code GrpcUnaryCall.respond} logs only the client-cancel race. A caller that
   * concealed without logging would not be hiding the detail from the client, it would be destroying it, which is
   * strictly worse than not concealing at all (PR #7755 review).
   *
   * @param requester the logging context, normally the calling service instance
   */
  static String concealableDescription(final Object requester, final String prefix, final Throwable e,
      final boolean conceal) {
    if (!conceal)
      // THE CLASS NAME WHEN THERE IS NO MESSAGE, LIKE insertErrorMessage AND Importer.describe: A LINK THAT READS
      // "null" NAMES THE FAILURE LESS WELL THAN ITS TYPE DOES. THE RULE LIVES IN ErrorConcealment (issue #10092)
      return prefix + ": " + ErrorConcealment.describe(e);

    logConcealed(requester, prefix, e);
    return prefix + ": " + CONCEALED_DESCRIPTION;
  }

  /**
   * Writes the entry a concealed answer promises, at the level the failure deserves.
   * <p>
   * {@code SEVERE} only for a fault the SERVER is responsible for. Everything else is something the caller did - a
   * duplicated key, a validation refusal, a security denial - and a routine one at that: an upsert-by-insert-and-
   * retry workload produces unique-constraint violations by design, and logging those at {@code SEVERE} would both
   * cost a hot path and drain the word of its meaning for whoever is alerting on it. That is the split
   * {@code AbstractServerHttpHandler} already draws between {@code getInternalErrorLogLevel()} and
   * {@code getUserSevereErrorLogLevel()}, and this is the gRPC side of the same line (PR #7755 review).
   */
  static void logConcealed(final Object requester, final String context, final Throwable cause) {
    final Level level = ErrorCategory.of(cause) == ErrorCategory.SERVER ? Level.SEVERE : Level.FINE;
    LogManager.instance().log(requester, level, "%s (concealed from the client in production mode)", cause,
        context != null ? context : "gRPC");
  }

  /**
   * The index name stays (schema metadata a driver needs to rebuild the typed exception); the KEY VALUES are stored
   * data, so in production the trailer carries {@link ArcadeDBServer#CONCEALED_DUPLICATED_KEYS} instead, the same
   * placeholder the HTTP {@code exceptionArgs} carries, so a client reads one value on either transport (issue #7760).
   */
  private static void addDuplicatedKeyTrailers(final Metadata trailers, final DuplicatedKeyException dup, final boolean conceal) {
    if (dup.getIndexName() != null)
      trailers.put(DUP_INDEX_KEY, encodeTrailer(dup.getIndexName()));
    if (dup.getKeys() != null)
      trailers.put(DUP_KEYS_KEY, encodeTrailer(conceal ? ArcadeDBServer.CONCEALED_DUPLICATED_KEYS : dup.getKeys()));
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
      httpLeader = blankToNull(ha.getLeaderForRefusal());
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
