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

import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

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
        new IllegalStateException("boom"), "graphBatchLoad", null, callerTrailers);

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
        new DuplicatedKeyException("idx", "[k]", null), "graphBatchLoad", null, callerTrailers);

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

    final StatusException se = GrpcErrorMapper.toStatusException(upstream, "graphBatchLoad", null, callerTrailers);

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
        new ExecutionException(new DuplicatedKeyException("idx", "[k]", null)), "graphBatchLoad", null, callerTrailers);

    assertThat(se.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
    assertThat(decode(callerTrailers.get(GrpcErrorMapper.DUP_INDEX_KEY))).isEqualTo("idx");
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
        new ServerIsNotTheLeaderException("not the leader", "10.0.0.7:2480"), "graphBatchLoad", null, callerTrailers);

    assertThat(se.getStatus().getCode())
        .as("retrying the same follower is not the fix; the address is")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(se.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo("10.0.0.7:2480");
    assertThat(se.getStatus().getDescription()).contains("10.0.0.7:2480");
    assertThat(callerTrailers.get(Metadata.Key.of("caller-own-trailer", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("kept");
  }
}
