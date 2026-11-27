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

import com.arcadedb.database.RID;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerControlPlane;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the server-side exception -> gRPC Status mapping (issue #5043). No server needed.
 */
class GrpcErrorMapperTest {

  @Test
  @DisplayName("DuplicatedKeyException maps to ALREADY_EXISTS and carries index + keys trailers")
  void duplicatedKey_mapsToAlreadyExistsWithDetails() {
    final DuplicatedKeyException dup = new DuplicatedKeyException("Person[name]", "[John]", null);

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(dup, "Commit failed");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    final Metadata trailers = sre.getTrailers();
    assertThat(trailers).isNotNull();
    assertThat(trailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DuplicatedKeyException.class.getName());
    // The dup trailers are Base64-encoded so arbitrary key values survive the ASCII metadata channel.
    assertThat(decode(trailers.get(GrpcErrorMapper.DUP_INDEX_KEY))).isEqualTo("Person[name]");
    assertThat(decode(trailers.get(GrpcErrorMapper.DUP_KEYS_KEY))).isEqualTo("[John]");
  }

  @Test
  @DisplayName("Duplicate-key trailers carry non-ASCII index/keys losslessly and stay ASCII on the wire")
  void duplicatedKey_nonAsciiKeysSurviveTrailer() {
    final String indexName = "Persönne[naïve]";
    final String keys = "[Ünïcödé-名前-😀]";
    final DuplicatedKeyException dup = new DuplicatedKeyException(indexName, keys, null);

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(dup, null);

    final Metadata trailers = sre.getTrailers();
    final String wireIndex = trailers.get(GrpcErrorMapper.DUP_INDEX_KEY);
    final String wireKeys = trailers.get(GrpcErrorMapper.DUP_KEYS_KEY);
    // On the wire the value must be pure printable ASCII (0x20-0x7E) so gRPC metadata cannot corrupt it.
    assertThat(wireIndex).matches("[\\x20-\\x7E]+");
    assertThat(wireKeys).matches("[\\x20-\\x7E]+");
    // Decoding restores the original non-ASCII content exactly.
    assertThat(decode(wireIndex)).isEqualTo(indexName);
    assertThat(decode(wireKeys)).isEqualTo(keys);
  }

  private static String decode(final String value) {
    return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("ConcurrentModificationException maps to ABORTED (retryable) with its class name")
  void concurrentModification_mapsToAborted() {
    final ConcurrentModificationException cme = new ConcurrentModificationException("record modified");

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(cme, null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .isEqualTo(ConcurrentModificationException.class.getName());
  }

  @Test
  @DisplayName("NeedRetryException maps to ABORTED with its class name")
  void needRetry_mapsToAborted() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new NeedRetryException("retry"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .isEqualTo(NeedRetryException.class.getName());
  }

  @Test
  @DisplayName("RecordNotFoundException maps to NOT_FOUND")
  void recordNotFound_mapsToNotFound() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new RecordNotFoundException("missing", null), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
  }

  @Test
  @DisplayName("TimeoutException maps to DEADLINE_EXCEEDED")
  void timeout_mapsToDeadlineExceeded() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new TimeoutException("slow"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
  }

  @Test
  @DisplayName("SecurityException maps to PERMISSION_DENIED")
  void security_mapsToPermissionDenied() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new SecurityException("nope"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  @DisplayName("IllegalArgumentException maps to INVALID_ARGUMENT")
  void illegalArgument_mapsToInvalidArgument() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new IllegalArgumentException("bad"),
        null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  /**
   * Regression tests for issue #7123: before, everything below this point - a SQL syntax error, a missing type
   * and a division by zero - fell through the final {@code else} to {@code INTERNAL}, the code a gRPC client
   * reads as "server broke, safe to retry", even though none of the three will ever succeed on retry.
   */
  @Test
  @DisplayName("CommandParsingException (a SQL syntax error) maps to INVALID_ARGUMENT, not INTERNAL")
  void commandParsingException_mapsToInvalidArgument() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new CommandParsingException("unexpected token"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  @DisplayName("CommandSemanticException maps to INVALID_ARGUMENT, not INTERNAL")
  void commandSemanticException_mapsToInvalidArgument() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new CommandSemanticException("undefined variable"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  @DisplayName("SchemaException (a missing type/bucket/property) maps to NOT_FOUND, not INTERNAL")
  void schemaException_mapsToNotFound() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new SchemaException("Type 'Missing' not found"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
  }

  @Test
  @DisplayName("ArithmeticErrorException (overflow/division by zero) maps to OUT_OF_RANGE, not INTERNAL")
  void arithmeticErrorException_mapsToOutOfRange() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ArithmeticErrorException("long overflow"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.OUT_OF_RANGE);
  }

  @Test
  @DisplayName("A retryable conflict still wins over an arithmetic error carried in the same chain")
  void retryableConflictWinsOverArithmeticError() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ArithmeticErrorException("long overflow", new ConcurrentModificationException("retry")), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
  }

  @Test
  @DisplayName("Unknown exception maps to INTERNAL")
  void unknown_mapsToInternal() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new IllegalStateException("boom"),
        null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
  }

  @Test
  @DisplayName("A database the server holds (restore, close database) maps to UNAVAILABLE, not INTERNAL (issue #10533)")
  void databaseHeld_mapsToUnavailable() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseNotAvailableException("Database 'mydb' is not available: a restore of it is in progress"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DatabaseNotAvailableException.class.getName());
  }

  @Test
  @DisplayName("A held database is UNAVAILABLE through a wrapper too")
  void databaseHeld_mapsToUnavailableThroughAWrapper() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new CommandExecutionException("Error on executing the command", new DatabaseNotAvailableException("held")), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    // named by the frame that classified it (issue #10553), as every other classified failure is, not by its wrapper
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DatabaseNotAvailableException.class.getName());
  }

  @Test
  @DisplayName("A database that does not exist is NOT_FOUND named by its class, neither UNAVAILABLE nor INTERNAL (issue #10795)")
  void databaseNotFound_isNotFoundNamedByItsClass() {
    final DatabaseNotFoundException missing = new DatabaseNotFoundException("Database 'nope' does not exist");
    for (final Throwable failure : new Throwable[] { missing, new CommandExecutionException("outer", missing) }) {
      final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(failure, null);

      assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
      // the class, so the client rebuilds the type rather than reading NOT_FOUND as a missing record
      assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DatabaseNotFoundException.class.getName());
    }
  }

  @Test
  @DisplayName("An already-mapped StatusRuntimeException is passed through, not re-wrapped")
  void alreadyMappedStatus_passedThrough() {
    final StatusRuntimeException original = Status.PERMISSION_DENIED
        .withDescription("User has not access to database").asRuntimeException();

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(original, "Failed to begin transaction");

    assertThat(sre).isSameAs(original);
    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  @DisplayName("An ExecutionException wrapping the cause is unwrapped before mapping")
  void executionExceptionIsUnwrapped() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ExecutionException(new DuplicatedKeyException("idx", "[k]", null)), "Commit failed");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(decode(sre.getTrailers().get(GrpcErrorMapper.DUP_INDEX_KEY))).isEqualTo("idx");
  }

  /**
   * Regression tests for the code-review follow-up on issue #7123: {@code graphBatchLoad} attaches its own
   * partial-commit trailer, so it cannot call {@link GrpcErrorMapper#toStatusRuntimeException} directly, which
   * builds a standalone {@link Metadata}. It reaches the identical classification through
   * {@link GrpcErrorMapper#toStatusException}, which layers the mapper's own trailers onto the caller's - before
   * that existed the handler fell back to {@code statusCodeFor} alone, getting the right code for a
   * {@link DuplicatedKeyException} but silently losing the {@code DUP_INDEX_KEY}/{@code DUP_KEYS_KEY} trailers
   * that {@code executeCommand}/{@code createRecord} attach for the identical exception.
   */
  @Test
  @DisplayName("toStatusException adds the exception class name to an existing trailer set")
  void toStatusException_addsExceptionClassToExistingTrailers() {
    final Metadata callerTrailers = new Metadata();
    callerTrailers.put(Metadata.Key.of("caller-own-trailer", Metadata.ASCII_STRING_MARSHALLER), "kept");

    final StatusException se = GrpcErrorMapper.toStatusException(
        new IllegalStateException("boom"), "graphBatchLoad", null, callerTrailers, false);

    assertThat(se.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    assertThat(se.getStatus().getDescription()).isEqualTo("graphBatchLoad: boom");
    assertThat(se.getTrailers()).isSameAs(callerTrailers);
    assertThat(callerTrailers.get(Metadata.Key.of("caller-own-trailer", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("kept");
    assertThat(callerTrailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(IllegalStateException.class.getName());
  }

  @Test
  @DisplayName("toStatusException adds the dup-key trailers alongside the caller's own trailers")
  void toStatusException_duplicatedKeyAddsDupTrailersToo() {
    final Metadata callerTrailers = new Metadata();
    callerTrailers.put(Metadata.Key.of("caller-own-trailer", Metadata.ASCII_STRING_MARSHALLER), "kept");

    final StatusException se = GrpcErrorMapper.toStatusException(
        new DuplicatedKeyException("idx", "[k]", null), "graphBatchLoad", null, callerTrailers, false);

    assertThat(se.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(callerTrailers.get(Metadata.Key.of("caller-own-trailer", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("kept");
    assertThat(decode(callerTrailers.get(GrpcErrorMapper.DUP_INDEX_KEY))).isEqualTo("idx");
    assertThat(decode(callerTrailers.get(GrpcErrorMapper.DUP_KEYS_KEY))).isEqualTo("[k]");
  }

  /**
   * Regression test for a code-review follow-up on issue #7123: {@code graphBatchLoad}'s
   * {@code getDatabase()} call can raise a {@link StatusRuntimeException} (e.g. PERMISSION_DENIED) the
   * same way it does for every other RPC, and the mapping must preserve it - not fold it into SERVER/INTERNAL
   * through {@code ErrorCategory}, which does not know about gRPC's own exception types. Its own description is
   * kept too, rather than being overwritten with a synthesized one.
   */
  @Test
  @DisplayName("toStatusException preserves an already-mapped StatusRuntimeException's code, description and trailers")
  void toStatusException_preservesUpstreamStatusRuntimeException() {
    final Metadata upstreamTrailers = new Metadata();
    upstreamTrailers.put(Metadata.Key.of("upstream-trailer", Metadata.ASCII_STRING_MARSHALLER), "from-getDatabase");
    final StatusRuntimeException upstream = Status.PERMISSION_DENIED.withDescription("no access")
        .asRuntimeException(upstreamTrailers);

    final Metadata callerTrailers = new Metadata();
    callerTrailers.put(Metadata.Key.of("caller-own-trailer", Metadata.ASCII_STRING_MARSHALLER), "kept");

    final StatusException se = GrpcErrorMapper.toStatusException(upstream, "graphBatchLoad", null, callerTrailers, false);

    assertThat(se.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(se.getStatus().getDescription()).isEqualTo("no access");
    assertThat(callerTrailers.get(Metadata.Key.of("caller-own-trailer", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("kept");
    assertThat(callerTrailers.get(Metadata.Key.of("upstream-trailer", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("from-getDatabase");
    // Not misclassified as INTERNAL through ErrorCategory, which does not recognize StatusRuntimeException.
    assertThat(callerTrailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isNull();
  }

  @Test
  @DisplayName("toStatusException unwraps an ExecutionException before classifying")
  void toStatusException_unwrapsExecutionException() {
    final Metadata callerTrailers = new Metadata();

    final StatusException se = GrpcErrorMapper.toStatusException(
        new ExecutionException(new DuplicatedKeyException("idx", "[k]", null)), "graphBatchLoad", null, callerTrailers, false);

    assertThat(se.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(decode(callerTrailers.get(GrpcErrorMapper.DUP_INDEX_KEY))).isEqualTo("idx");
  }

  /**
   * Issue #7472, item 1: the gRPC error paths reported the exception's own message whatever
   * {@code arcadedb.server.mode} said, so this surface silently opted out of the production concealment the HTTP
   * error body applies. What the HTTP body conceals is the FREE-FORM cause text, which can carry file paths, engine
   * internals and schema names; what it keeps is the bounded, structured part a driver acts on. This asserts the
   * same split on this transport.
   */
  @Test
  @DisplayName("production mode conceals the free-form description and keeps the code and trailers")
  void productionMode_concealsTheMessageButNotTheClassification() {
    final DuplicatedKeyException dup = new DuplicatedKeyException("Person[ssn]", "[123-45-6789]", null);

    final StatusRuntimeException verbose = GrpcErrorMapper.toStatusRuntimeException(dup, "Commit failed", null, false);
    assertThat(verbose.getStatus().getDescription())
        .as("development and test keep the full text, as they always did")
        .contains(dup.getMessage());

    final StatusRuntimeException concealed = GrpcErrorMapper.toStatusRuntimeException(dup, "Commit failed", null, true);

    assertThat(concealed.getStatus().getDescription())
        .as("the free-form message is gone")
        .doesNotContain("123-45-6789")
        .doesNotContain("Person[ssn]")
        .contains(GrpcErrorMapper.CONCEALED_DESCRIPTION)
        .as("and the RPC's own label survives - it is the analogue of the HTTP body's bounded 'error' field")
        .startsWith("Commit failed: ");

    assertThat(concealed.getStatus().getCode())
        .as("the classification is what a driver's retry policy reads, and is not free-form")
        .isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(concealed.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .as("the class name is the wire contract the remote driver rebuilds the typed exception from")
        .isEqualTo(DuplicatedKeyException.class.getName());
    assertThat(decode(concealed.getTrailers().get(GrpcErrorMapper.DUP_INDEX_KEY)))
        .as("and the structured duplicated-key details, exactly as the HTTP body keeps its exceptionArgs")
        .isEqualTo("Person[ssn]");
  }

  /**
   * Concealment is the same sentence for every failure. A message that varied - even by exception type - would put
   * back precisely the signal the concealment removes.
   */
  @Test
  @DisplayName("every concealed failure carries the same description")
  void productionMode_concealmentIsIndistinguishableAcrossFailures() {
    final String syntax = GrpcErrorMapper
        .toStatusRuntimeException(new CommandParsingException("Unexpected token at line 3 of /srv/queries/a.sql"),
            "ExecuteCommand", null, true)
        .getStatus().getDescription();
    final String notFound = GrpcErrorMapper
        .toStatusRuntimeException(new SchemaException("Type 'PatientRecord' not found"), "ExecuteCommand", null, true)
        .getStatus().getDescription();

    assertThat(syntax).isEqualTo(notFound);
    assertThat(syntax).doesNotContain("a.sql").doesNotContain("PatientRecord");
  }

  /**
   * A status a caller-facing layer already chose - a {@code getDatabase()} auth refusal - is passed through
   * untouched in every mode, exactly as it is without concealment: it is not an engine exception's free-form text
   * but a refusal this server worded for the client, and rewriting it would tell an authorized caller nothing.
   */
  @Test
  @DisplayName("an already-mapped status is passed through even in production mode")
  void productionMode_leavesAnAlreadyMappedStatusAlone() {
    final StatusRuntimeException upstream = Status.PERMISSION_DENIED
        .withDescription("User 'alice' is not authorized to access database 'db'").asRuntimeException();

    assertThat(GrpcErrorMapper.toStatusRuntimeException(upstream, "ExecuteCommand", null, true).getStatus()
        .getDescription()).isEqualTo(upstream.getStatus().getDescription());
  }

  /**
   * Issue #7624, item 2: a {@link ServerIsNotTheLeaderException} raised DURING a {@code graphBatchLoad} - the
   * replicated database declining a schema change on a follower, as opposed to the explicit leadership check the
   * first chunk runs - used to be classified by {@link com.arcadedb.exception.ErrorCategory} alone. That lands it
   * in {@code RETRY} (it descends from {@code NeedRetryException}), so the answer was {@code ABORTED} with the
   * leader address dropped: a client that retried went back to the same follower, which refused again for the
   * same reason, because nothing had told it where the leader was.
   */
  @Test
  @DisplayName("toStatusException answers a leader refusal with FAILED_PRECONDITION and the leader address")
  void toStatusException_leaderRefusalKeepsTheRedirect() {
    final Metadata callerTrailers = new Metadata();
    callerTrailers.put(Metadata.Key.of("caller-own-trailer", Metadata.ASCII_STRING_MARSHALLER), "kept");

    final StatusException se = GrpcErrorMapper.toStatusException(
        new ServerIsNotTheLeaderException("not the leader", "10.0.0.7:2480"), "graphBatchLoad", null, callerTrailers,
        false);

    assertThat(se.getStatus().getCode())
        .as("retrying the same follower is not the fix; the address is")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(se.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("10.0.0.7:2480");
    assertThat(se.getStatus().getDescription()).contains("10.0.0.7:2480");
    assertThat(callerTrailers.get(Metadata.Key.of("caller-own-trailer", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("kept");
  }

  /*
   * A leader-only refusal names the leader wherever it is raised (issue #6183). Until now only
   * {@code graphBatchLoad} built the redirect trailers, by hand; an engine-raised refusal - a schema change the
   * replicated database rejects on a follower - reached the client as a bare retryable conflict, so a caller could
   * not tell "the leader is unknown, wait for the election" from "the leader is known and nobody told you".
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6183
  @Test
  @DisplayName("A leader refusal maps to FAILED_PRECONDITION, not the ABORTED its NeedRetryException ancestry implies")
  void leaderRefusalIsNotRetryableAsIs() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand",
        ha("localhost:50051", "localhost:2480"));

    // ABORTED would invite the caller to retry the same call against the same follower, forever.
    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .isEqualTo(ServerIsNotTheLeaderException.class.getName());
  }

  // Regression: #6183
  @Test
  @DisplayName("Both leader addresses are advertised, and the dialable one is the one the message names")
  void bothAddressesAreAdvertised() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand",
        ha("db0:50051", "db0:2480"));

    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS)).isEqualTo("db0:50051");
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("db0:2480");
    assertThat(sre.getStatus().getDescription())
        .startsWith("ExecuteCommand: schema changes must run on the leader")
        .contains("'db0:50051' (gRPC address)");
  }

  // Regression: #6183
  @Test
  @DisplayName("With no resolvable gRPC address the HTTP one is named, with the port caveat")
  void httpAddressIsTheFallback() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand",
        ha(null, "db0:2480"));

    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS)).isNull();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("db0:2480");
    assertThat(sre.getStatus().getDescription()).contains("'db0:2480' (HTTP address; use its gRPC port)");
  }

  // Regression: #6183
  @Test
  @DisplayName("A refusal raised mid-election names nothing and says so")
  void anUnknownLeaderIsReportedAsUnknown() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand",
        ha(null, null));

    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS)).isNull();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isNull();
    assertThat(sre.getStatus().getDescription()).contains("The leader is currently unknown");
  }

  // Regression: #6183
  /**
   * The ambiguity guard of issue #6183 answers null rather than an address that may be this very node's. The
   * refusal must then degrade to the HTTP address, never to "the leader is unknown" - it is known, only not
   * dialable over gRPC.
   */
  @Test
  @DisplayName("A suppressed routing table degrades to the HTTP address, not to an unknown leader")
  void aSuppressedRoutingTableStillNamesTheLeader() {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC)).thenReturn(null);
    when(ha.getLeaderAddress()).thenReturn("db0:2480");
    when(ha.getLeaderForRefusal()).thenCallRealMethod();

    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", null), "ExecuteCommand", ha);

    assertThat(sre.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("db0:2480");
    assertThat(sre.getStatus().getDescription()).doesNotContain("currently unknown");
  }

  // Regression: #6183
  /**
   * The engine builds the refusal with the leader's HTTP address already in it. Where no plugin is in reach the
   * mapper still has that much to report, rather than dropping it on the floor.
   */
  @Test
  @DisplayName("Without an HA plugin the address the exception carries is still advertised")
  void theExceptionsOwnAddressIsTheLastResort() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", "db0:2480"), "ExecuteCommand");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(sre.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("db0:2480");
  }

  // Regression: #6183
  /** A blank address is what "resolved to nothing" looks like coming out of the cluster; it is not an address. */
  @Test
  @DisplayName("Blank addresses are treated as absent")
  void blankAddressesAreNotAdvertised() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerIsNotTheLeaderException("schema changes must run on the leader", "  "), "ExecuteCommand",
        ha("  ", ""));

    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_GRPC_ADDRESS)).isNull();
    assertThat(trailers.get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isNull();
    assertThat(sre.getStatus().getDescription()).contains("The leader is currently unknown");
  }

  private static HAServerPlugin ha(final String grpcWriter, final String httpLeader) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC)).thenReturn(
        new HAServerPlugin.RoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC, grpcWriter, List.of()));
    when(ha.getLeaderAddress()).thenReturn(httpLeader);
    // The leader as a refusal names it (issue #10066) is a default method of the interface, which a mock does not run
    when(ha.getLeaderForRefusal()).thenCallRealMethod();
    return ha;
  }

  /*
   * A SQL {@code BACKUP DATABASE} or {@code IMPORT DATABASE} refused by the per-database maintenance slot reaches a
   * gRPC client as {@code ABORTED}, not {@code INTERNAL} (issue #7443).
   * <p>
   * The two gRPC surfaces answer it through different code. The admin service's own RPCs - {@code TriggerBackup},
   * {@code RestoreBackup}, {@code RestoreDatabase}, {@code ImportDatabase} - go through
   * {@code ArcadeDbGrpcAdminService.toStatusException}; a SQL statement goes through {@code ExecuteCommand} on the
   * other service and so through {@link GrpcErrorMapper}. Both now match the engine's
   * {@link DatabaseOperationInProgressException}, which is what makes the two agree: the request is well formed and
   * authorized, and retrying once the other operation finishes is the fix.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  // Regression: #7443
  @Test
  void aRefusedSqlMaintenanceStatementMapsToAbortedRatherThanInternal() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new DatabaseOperationInProgressException("Cannot back up database 'db': a restore of it is already in progress"),
        "ExecuteCommand");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(sre.getStatus().getDescription()).contains("a restore of it is already in progress");
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
        .isEqualTo(DatabaseOperationInProgressException.class.getName());
  }

  // Regression: #7443
  /**
   * The server's own control-plane refusal is the same refusal, and it keeps the {@code ABORTED} it has had since
   * #7384 now that its type extends the engine's - on this mapper too, not only on the admin service's.
   */
  @Test
  void theServersOwnRefusalKeepsTheSameStatusThroughTheSameArm() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new ServerControlPlane.OperationInProgressException(
            "Cannot restore database 'db': a backup of it is already in progress"),
        "RestoreDatabase");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
  }

  /*
   * Issue #7760 on the gRPC transport: production mode replaces the {@code arcadedb-dup-keys} trailer's value with the same
   * placeholder HTTP uses (the key values are stored data) and keeps the class and index-name trailers, the same decision the HTTP
   * {@code exceptionArgs} takes ({@code HttpErrorStatusMappingTest.DuplicatedKeyConcealment}).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String SECRET = "alice@example.com";

  private static DuplicatedKeyException duplicate() {
    return new DuplicatedKeyException("User[email]", "[" + SECRET + "]", new RID(3, 7));
  }

  // Regression: #7760
  @Test
  void productionModeReplacesTheKeysTrailerAndKeepsTheIndex() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(duplicate(), "ExecuteCommand", null, true);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(new String(Base64.getDecoder().decode(sre.getTrailers().get(GrpcErrorMapper.DUP_KEYS_KEY)), StandardCharsets.UTF_8))
        .isEqualTo(ArcadeDBServer.CONCEALED_DUPLICATED_KEYS);
    assertThat(sre.getStatus().getDescription()).doesNotContain(SECRET);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DuplicatedKeyException.class.getName());
    assertThat(new String(Base64.getDecoder().decode(sre.getTrailers().get(GrpcErrorMapper.DUP_INDEX_KEY)), StandardCharsets.UTF_8))
        .isEqualTo("User[email]");
  }

  // Regression: #7760
  @Test
  void developmentModeKeepsTheKeysTrailer() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(duplicate(), "ExecuteCommand", null, false);

    assertThat(new String(Base64.getDecoder().decode(sre.getTrailers().get(GrpcErrorMapper.DUP_KEYS_KEY)), StandardCharsets.UTF_8))
        .isEqualTo("[" + SECRET + "]");
  }

  /*
   * Issue #10153 on the gRPC data plane: a {@code CREATE} on a taken name was {@code INTERNAL} (a {@code CommandExecutionException})
   * or {@code NOT_FOUND} (a {@code SchemaException} - the opposite of what happened). It is {@code ALREADY_EXISTS} now, named in the
   * class trailer as a {@link SchemaException}, its supertype, so a client that predates {@link AlreadyExistsException} rebuilds a
   * SchemaException rather than reading the status as a duplicate key, and with the kind in a trailer of its own.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10153
  @Test
  void aTakenNameIsAlreadyExistsWithTheKindTrailer() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new AlreadyExistsException(AlreadyExistsException.Kind.PROPERTY, "Property 'V.p' already exists"), "Command failed");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    final Metadata trailers = sre.getTrailers();
    assertThat(trailers.get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(SchemaException.class.getName());
    assertThat(trailers.get(GrpcErrorMapper.ALREADY_EXISTS_KIND_KEY)).isEqualTo("PROPERTY");
    assertThat(trailers.get(GrpcErrorMapper.DUP_INDEX_KEY)).as("not a duplicate key").isNull();
    assertThat(sre.getStatus().getDescription()).contains("Property 'V.p' already exists");
  }

  // Regression: #10153
  @Test
  void aWrappedTakenNameIsNamedAsTheConflictNotAsTheWrapper() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new CommandExecutionException("Error on executing the script", new TypeAlreadyExistsException("V")), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(SchemaException.class.getName());
    assertThat(sre.getTrailers().get(GrpcErrorMapper.ALREADY_EXISTS_KIND_KEY)).isEqualTo("TYPE");
  }

  // Regression: #10153
  @Test
  void productionModeKeepsTheLabelOfTheConflict() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new AlreadyExistsException(AlreadyExistsException.Kind.MATERIALIZED_VIEW, "Materialized view 'secret_name' already exists"),
        null, null, true);

    assertThat(sre.getStatus().getDescription()).isEqualTo("Materialized view already exists");
  }

  // Regression: #10153
  @Test
  void aMissingTypeStaysNotFound() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new SchemaException("Type 'X' was not found"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.ALREADY_EXISTS_KIND_KEY)).isNull();
  }

  /*
   * Issue #11314 on gRPC: a value a declared constraint refuses carries the constraint in a trailer, as a taken name carries the
   * kind of object holding it (#10153), so the client rebuilds the {@link ValidationException} with its kind; and a call of an
   * unknown SQL function is the caller's {@code INVALID_ARGUMENT}, where it was the {@code INTERNAL} of a server fault.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11314
  @Test
  void aViolatedConstraintIsInvalidArgumentWithTheKindTrailer() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new CommandExecutionException("Error on executing",
        new ValidationException(ValidationException.Kind.NOT_NULL, "The property 'T.p' cannot be null")), "Command failed");

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(ValidationException.class.getName());
    assertThat(sre.getTrailers().get(GrpcErrorMapper.VALIDATION_KIND_KEY)).isEqualTo("NOT_NULL");
  }

  // Regression: #11314
  @Test
  void aRefusalOfNoDeclaredConstraintCarriesNoKind() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(new ValidationException("refused"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.VALIDATION_KIND_KEY)).isNull();
  }

  // Regression: #11314
  @Test
  void anUnknownFunctionIsInvalidArgument() {
    final StatusRuntimeException sre = GrpcErrorMapper.toStatusRuntimeException(
        new UnknownFunctionException("foo", "Unknown function name 'foo'"), null);

    assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(sre.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(UnknownFunctionException.class.getName());
  }
}
