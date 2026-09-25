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
import com.arcadedb.database.RID;
import com.arcadedb.engine.timeseries.TimeSeriesWalkCoarsenedException;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.fulltext.FullTextQueryParseException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ClusterCapabilityNotReadyException;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSessionException;
import com.arcadedb.server.http.RequestBodyTooLargeException;
import com.arcadedb.server.http.RequestStillInFlightException;
import com.arcadedb.server.http.ResultSetTooLargeException;
import com.arcadedb.server.http.RetryLaterException;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;

import io.micrometer.observation.ObservationRegistry;
import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RequestTooBigException;
import io.undertow.util.HeaderMap;
import io.undertow.util.Methods;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.ArgumentCaptor;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #6201: the HTTP status a failure is answered with must not depend on whether the
 * failure happened to be raised inside a transaction wrapper.
 * <p>
 * {@code DatabaseAbstractHandler} runs {@code execute()} inside the engine's retrying transaction, and the error
 * mapping in {@link AbstractServerHttpHandler} used to be three hand-written {@code instanceof} chains - one for
 * an exception that arrived as itself, one inside the {@code CommandExecutionException} arm and one inside the
 * {@code TransactionException} arm. They were mirrors maintained by hand and were never complete: every new
 * mapping (#4350, #5064, #5191, #5602, #5935, #6191) had to be added to each chain separately, and the half that
 * was missed took a second issue to notice. {@code NeedRetryException} (503) and {@code RecordNotFoundException}
 * (404) were the pair still missing when #6201 was filed, so a retryable conflict raised while a command ran was
 * answered as an opaque 500 "Error on transaction commit" - and a client whose retry policy keys on 503 gave up
 * on a write that would have succeeded on retry.
 * <p>
 * This test pins the property rather than the individual arms: for every mapped exception, the bare exception and
 * the same exception wrapped in each of the two generic wrappers must produce the SAME status. A future mapping
 * added to only one branch of the classification fails here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6201ErrorStatusParityTest {

  /** One mapped failure: how to build it, and the status the contract says it is answered with. */
  private record MappedFailure(String name, int expectedStatus, Supplier<RuntimeException> factory) {
  }

  private static final List<MappedFailure> MAPPED_FAILURES = List.of(
      // The pair #6201 was filed for: both existed only on the un-wrapped chain.
      new MappedFailure("ConcurrentModificationException", 503,
          () -> new ConcurrentModificationException("Record modified by another transaction")),
      new MappedFailure("RecordNotFoundException", 404,
          () -> new RecordNotFoundException("Record not found", new RID(1, 1))),
      // A database closed out from under an in-flight request by an HA snapshot-reinstall resync (issue #5977
      // pattern) is a transient condition, not a permanent failure - it used to fall through to a hard 500,
      // which a remote client's own retry-on-503 loop never saw (issue #6770).
      new MappedFailure("DatabaseIsClosedException", 503, () -> new DatabaseIsClosedException("mydb")),
      // A TimeSeries read a DOWNSAMPLE overtook: the rows it had not reached were replaced by coarser ones, so no
      // answer it can still produce is at one resolution and it refuses instead of returning a silently short one
      // (issue #8166). Transient in the same sense as the two above - downsampling is a maintenance event, not a
      // per-request one - so the same read re-issued succeeds, and answering the generic 500 would have hidden
      // that from exactly the PromQL/Grafana client the refusal was added for (review of PR #8197).
      new MappedFailure("TimeSeriesWalkCoarsenedException", 503,
          () -> new TimeSeriesWalkCoarsenedException("Sealed block [1..2] was replaced by a downsample")),
      // A permanent DROP/CLOSE DATABASE race (not the transient resync above) that lost the retry-then-reresolve
      // round trip: allowLoad=false found no open handle for the name. An accurate 404, not the generic 500 the
      // un-typed DatabaseOperationException used to fall through to (issue #6778, #6770 follow-up).
      new MappedFailure("DatabaseNotAvailableException", 404,
          () -> new DatabaseNotAvailableException("Database 'mydb' is not available")),
      // The mappings each earlier issue had to add to two or three chains by hand.
      new MappedFailure("DuplicatedKeyException", 409, () -> new DuplicatedKeyException("Idx", "[1]", new RID(1, 1))),
      new MappedFailure("TransactionCommittedRemotelyException", 409,
          () -> new TransactionCommittedRemotelyException("committed cluster-wide, do not retry",
              new IllegalStateException("simulated local apply failure"))),
      new MappedFailure("ServerIsNotTheLeaderException", 400,
          () -> new ServerIsNotTheLeaderException("Not the leader", "192.168.0.1:2480")),
      new MappedFailure("SecurityException", 403, () -> new SecurityException("Not allowed")),
      // Not a SecurityException - it extends ServerException - so asking for either type alone is a half-answer.
      new MappedFailure("ServerSecurityException", 403, () -> new ServerSecurityException("Not allowed")),
      new MappedFailure("QueryNotIdempotentException", 400,
          () -> new QueryNotIdempotentException("Query is not idempotent")),
      new MappedFailure("JSONException", 400, () -> new JSONException("Missing property 'command'")),
      new MappedFailure("IllegalArgumentException", 400, () -> new IllegalArgumentException("Unparseable limit")),
      // Issue #7729. It extends CommandExecutionException, whose arm answers 500, so it needs its own arm ahead of
      // that one - exactly like ArithmeticErrorException. It answered 400 as an IllegalArgumentException before it
      // had a type of its own, and a value the caller cannot store is the caller's error either way.
      new MappedFailure("InvalidPropertyTypeException", 400,
          () -> new InvalidPropertyTypeException(
              "TypeError: InvalidPropertyType - Property values can only be of primitive types or arrays thereof.")),
      new MappedFailure("CommandParsingException", 400, () -> new CommandParsingException("Unknown variable 'x'")),
      // Issue #8343: an identical request is still executing, here or - rebuilt by a follower from the leader's answer
      // to a forwarded SQL write - on the leader. It reached this chain inside the auto-commit wrapper as a plain
      // TransactionException and left as a 500, so the wrapped shapes are the ones that matter most.
      new MappedFailure("RequestStillInFlightException", 409,
          () -> new RequestStillInFlightException("Retry it later with the same X-Request-Id", 5)),
      // Issue #8355: a node the follower forwarded a SQL write to refused it before running it, with a back-off (a
      // snapshot install). Rebuilt by the follower, it reached this chain as a plain TransactionException and left as
      // a 500 with no back-off, so the wrapped shapes are again the ones that matter.
      new MappedFailure("RetryLaterException", 503,
          () -> new RetryLaterException("Server is installing a snapshot, please retry", 5)));

  /**
   * The property itself: bare, wrapped in {@code TransactionException} (what the auto-commit wrapper produced),
   * and wrapped in {@code CommandExecutionException} (what a script or a command planner produces) must all
   * answer the same status.
   */
  @TestFactory
  Stream<DynamicTest> statusIsTheSameWrappedAndUnwrapped() {
    return MAPPED_FAILURES.stream().map(failure -> DynamicTest.dynamicTest(failure.name(), () -> {
      final HandledResponse bare = handle(failure.factory().get());
      assertThat(bare.statusCode)
          .as("%s raised directly must answer %d (body=%s)", failure.name(), failure.expectedStatus(), bare.body)
          .isEqualTo(failure.expectedStatus());

      final HandledResponse inTransaction = handle(
          new TransactionException("Error on executing command", failure.factory().get()));
      assertThat(inTransaction.statusCode)
          .as("%s raised INSIDE the auto-commit transaction wrapper must answer the same %d, not a generic 500"
              + " (body=%s)", failure.name(), failure.expectedStatus(), inTransaction.body)
          .isEqualTo(failure.expectedStatus());

      final HandledResponse inCommand = handle(
          new CommandExecutionException("Error on command execution", failure.factory().get()));
      assertThat(inCommand.statusCode)
          .as("%s wrapped by a command planner must answer the same %d (body=%s)", failure.name(),
              failure.expectedStatus(), inCommand.body)
          .isEqualTo(failure.expectedStatus());
    }));
  }

  /**
   * The concrete defect reported in #6201: a lost MVCC race inside the auto-commit wrapper answered 500
   * "Error on transaction commit", which tells a client neither that the write can be re-driven nor why it
   * failed. {@code PostBatchHandler} documents the contract as "NeedRetryException -> 503".
   */
  @Test
  void aConflictInsideTheAutoCommitWrapperIsReportedAsRetryable() {
    final HandledResponse response = handle(new TransactionException("Error on executing command",
        new ConcurrentModificationException("Record #1:1 modified by another transaction")));

    assertThat(response.statusCode).isEqualTo(503);
    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("error")).isEqualTo("Cannot execute command");
    // The typed exception survives too: the remote Java driver and the HA leader-exception reconstruction both
    // rebuild the retryable type from this field, and a generic TransactionException is not retryable.
    assertThat(json.getString("exception")).isEqualTo(ConcurrentModificationException.class.getName());
  }

  /**
   * The concrete defect reported in #6770: a follower's database closes out from under an in-flight read or write
   * request while an HA snapshot-reinstall resync (issue #5977 pattern) is running. Before this fix that degraded
   * to a hard 500 "Internal error" - opaque to a client, and invisible to {@code RemoteHttpComponent}'s
   * NeedRetryException-driven auto-retry, which only fires on a 503. A resync closing and reinstalling the
   * database is transient by construction (the fresh handle the retry resolves sees the reinstalled database), so
   * it must be reported the same retryable way a Raft conflict already is.
   */
  @Test
  void aDatabaseClosedByAConcurrentResyncIsReportedAsRetryable() {
    final HandledResponse response = handle(new DatabaseIsClosedException("mydb"));

    assertThat(response.statusCode).isEqualTo(503);
    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("exception")).isEqualTo(DatabaseIsClosedException.class.getName());
  }

  /**
   * The concrete defect reported in #6778 (a #6770 follow-up): a database dropped/closed permanently by a
   * concurrent admin action races an in-flight request the same way the transient resync above does, but
   * {@code ArcadeDBServer.getDatabase(..., allowLoad=false)} - what the client's automatic 503 retry re-resolves
   * through once it re-hits the database - throws a narrower {@link DatabaseNotAvailableException} rather than
   * the generic {@link com.arcadedb.exception.DatabaseOperationException}. Before this fix that fell through to
   * the generic 500 "Internal error", leaving the client with no accurate signal that the database is simply
   * gone; it now answers the same 404 a client already expects for "the thing you asked for isn't there".
   */
  @Test
  void aPermanentlyUnavailableDatabaseIsReported404NotAGeneric500() {
    final HandledResponse response = handle(new DatabaseNotAvailableException("Database 'mydb' is not available"));

    assertThat(response.statusCode).isEqualTo(404);
    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("error")).isEqualTo("Database not found");
    assertThat(json.getString("exception")).isEqualTo(DatabaseNotAvailableException.class.getName());
  }

  /**
   * A leadership refusal that cannot name a leader is transient, not a client error. A follower forwards a
   * server command to the leader; when the request lands during an election {@code getLeaderAddress()} is still
   * null and the refusal names nobody. That answered 400, which tells every client, driver and load balancer
   * "your request was malformed, do not retry" about a condition that clears itself in milliseconds - and it is
   * the opposite of what a null address already means everywhere else this exception is read ("retry,
   * destination unknown": see {@code GrpcClientErrorMapper#leaderAddress}). It now answers the 503 its
   * {@code NeedRetryException} supertype always implied, so {@code RemoteHttpComponent}'s retry-on-503 loop
   * absorbs it.
   * <p>
   * The named case is unchanged and still 400 (pinned by the mapping table above): there the address is the
   * actionable half, and the forwarding peer rebuilds the typed exception from it (issue #6191).
   */
  @Test
  void aLeadershipRefusalThatNamesNoLeaderIsRetryableNotAClientError() {
    for (final String unnamed : new String[] { null, "", "   " }) {
      final HandledResponse response = handle(new ServerIsNotTheLeaderException("Leader address is unknown", unnamed));

      assertThat(response.statusCode)
          .as("a refusal issued mid-election (leader address %s) must be retryable, not a 400 (body=%s)",
              unnamed == null ? "null" : "'" + unnamed + "'", response.body)
          .isEqualTo(503);
      // The typed exception still survives on the wire: the remote driver and the HA leader-exception
      // reconstruction both rebuild the retryable type from this field.
      assertThat(new JSONObject(response.body).getString("exception"))
          .isEqualTo(ServerIsNotTheLeaderException.class.getName());
    }
  }

  /**
   * The last-resort walk: a security refusal buried under wrappers the classification does not recognise is still
   * a 403, and it must not matter which of the two unrelated security types it is. The walk used to test only
   * {@code SecurityException}, so the same refusal answered 403 one level up and a generic 500 two levels down.
   */
  @Test
  void aSecurityFailureBuriedUnderUnrecognisedWrappersIsStill403() {
    for (final RuntimeException buried : List.of(
        new IllegalStateException("outer", new RuntimeException("middle", new SecurityException("Not allowed"))),
        new IllegalStateException("outer", new RuntimeException("middle", new ServerSecurityException("Not allowed"))))) {
      final HandledResponse response = handle(buried);
      assertThat(response.statusCode)
          .as("a security refusal at depth 2 must still be 403 (body=%s)", response.body)
          .isEqualTo(403);
      assertThat(new JSONObject(response.body).getString("error")).isEqualTo("Security error");
    }
  }

  /**
   * Collapsing three orderings into one means the surviving order has to be the right one where they disagreed.
   * A statement whose text failed to parse is reported as the parsing failure it is, even when the parser's own
   * cause is a {@code JSONException} - which is what the {@code CommandExecutionException|CommandParsingException}
   * chain did, and which is the actionable half for a client: the query text is invalid, and how the parser
   * tripped over it is an implementation detail. Both answer 400, so this pins the message and the wire
   * contract's {@code exception} field rather than the status.
   */
  @Test
  void aParseFailureIsReportedAsOneEvenWhenItsCauseIsMalformedJson() {
    final HandledResponse response = handle(
        new CommandParsingException("Unknown variable 'x'", new JSONException("Missing property 'command'")));

    assertThat(response.statusCode).isEqualTo(400);
    final JSONObject json = new JSONObject(response.body);
    assertThat(json.getString("error")).isEqualTo("Cannot execute command");
    assertThat(json.getString("exception")).isEqualTo(CommandParsingException.class.getName());
  }

  /**
   * The other side of that order: a malformed payload that is NOT a parse failure still answers as one, so
   * moving the parsing arm ahead of the JSON arm did not swallow the #5935 mapping.
   */
  @Test
  void aMalformedPayloadIsStillReportedAsInvalidJson() {
    for (final RuntimeException malformed : List.of(
        new JSONException("Missing property 'command'"),
        new CommandExecutionException("Error on command execution", new JSONException("Missing property 'command'")),
        new TransactionException("Error on executing command", new JSONException("Missing property 'command'")))) {
      final HandledResponse response = handle(malformed);
      assertThat(response.statusCode).isEqualTo(400);
      assertThat(new JSONObject(response.body).getString("error"))
          .as("shape=%s", malformed.getClass().getSimpleName())
          .isEqualTo("Invalid JSON payload");
    }
  }

  /**
   * The un-wrapped fallbacks the classification still has to distinguish once nothing more specific matched, so
   * collapsing the three chains into one did not collapse the three generic 500s into one message.
   */
  @Test
  void theGenericWrappersKeepTheirOwnUnrecognisedFailureMessage() {
    assertThat(new JSONObject(handle(new TransactionException("Error on commit")).body).getString("error"))
        .isEqualTo("Error on transaction commit");
    assertThat(new JSONObject(handle(new CommandExecutionException("boom", new IllegalStateException("x"))).body)
        .getString("error")).isEqualTo("Cannot execute command");
    assertThat(new JSONObject(handle(new IllegalStateException("unexpected internal state")).body).getString("error"))
        .isEqualTo("Internal error");
  }

  /**
   * Issue #7396: the classification is readable on its own - {@code PostBatchHandler} puts its status in the
   * in-band error line of a streamed load that failed after its 200 was sent - so it must be exactly what the
   * sender answers with, for every mapped failure in every shape. A decision the sender made on its own, or one
   * the classifier reports differently from what goes on the wire, would be the second chain the split removed.
   */
  @TestFactory
  Stream<DynamicTest> theClassificationIsExactlyWhatIsSent() {
    return MAPPED_FAILURES.stream().flatMap(failure -> Stream.<Supplier<RuntimeException>>of(
            failure.factory(),
            () -> new TransactionException("Error on executing command", failure.factory().get()),
            () -> new CommandExecutionException("Error on command execution", failure.factory().get()))
        .map(shape -> DynamicTest.dynamicTest(failure.name() + " as " + shape.get().getClass().getSimpleName(), () -> {
          final AbstractServerHttpHandler.ErrorClassification classification = handler(shape.get())
              .classifyError(shape.get());
          final HandledResponse sent = handle(shape.get());
          final JSONObject body = new JSONObject(sent.body);

          assertThat(classification.status()).as("status (body=%s)", sent.body).isEqualTo(sent.statusCode)
              .isEqualTo(failure.expectedStatus());
          assertThat(classification.message()).as("label").isEqualTo(body.getString("error"));
          assertThat(classification.reported().getClass().getName()).as("exception")
              .isEqualTo(body.getString("exception"));
          assertThat(classification.exceptionArgs()).as("exceptionArgs")
              .isEqualTo(body.has("exceptionArgs") ? body.getString("exceptionArgs") : null);
        })));
  }

  /**
   * The arms the #6201 table above does not list, among them the one arm that reads server configuration - the
   * wire-body cap puts the configured limit in exceptionArgs, so the classifier cannot be a static function of
   * the exception alone, and a streamed batch load refused mid-upload for exceeding it is exactly the failure
   * {@code PostBatchHandler} now reports in band with this status.
   */
  @TestFactory
  Stream<DynamicTest> theClassificationOfTheRemainingArmsIsExactlyWhatIsSent() {
    return Stream.of(
        new MappedFailure("RequestTooBigException", 413, () -> new UncheckedIOException(new RequestTooBigException("too big"))),
        new MappedFailure("ResultSetTooLargeException", 413, () -> new ResultSetTooLargeException("too many rows", 10)),
        new MappedFailure("RequestBodyTooLargeException", 413, () -> new RequestBodyTooLargeException("too big decoded", 20)),
        new MappedFailure("HttpSessionException", 404, () -> new HttpSessionException("session gone")),
        new MappedFailure("DatabaseOperationInProgressException", 409,
            () -> new DatabaseOperationInProgressException("backup running")),
        new MappedFailure("ArithmeticErrorException", 400, () -> new ArithmeticErrorException("/ by zero")),
        new MappedFailure("FullTextQueryParseException", 400,
            () -> new FullTextQueryParseException("bad query", new IllegalStateException("x"))),
        // exceptionArgs DERIVED from the exception (capability|peer,peer) rather than a literal: the arm most
        // likely to drift if the sender ever built it differently from the classifier (PR #8237 review).
        new MappedFailure("ClusterCapabilityNotReadyException", 409,
            () -> new ClusterCapabilityNotReadyException("peers have not proved they can decode it", "security-v2",
                List.of("node-b:2424", "node-c:2424"))),
        // The leaderless refusal falls through the 400 arm to the NeedRetryException one: the two arms of one type.
        new MappedFailure("ServerIsNotTheLeaderException without a leader", 503,
            () -> new ServerIsNotTheLeaderException("Leader address is unknown", null))
    ).map(failure -> DynamicTest.dynamicTest(failure.name(), () -> {
      final AbstractServerHttpHandler.ErrorClassification classification = handler(failure.factory().get())
          .classifyError(failure.factory().get());
      final HandledResponse sent = handle(failure.factory().get());
      final JSONObject body = new JSONObject(sent.body);

      assertThat(classification.status()).as("status (body=%s)", sent.body).isEqualTo(sent.statusCode)
          .isEqualTo(failure.expectedStatus());
      assertThat(classification.message()).as("label").isEqualTo(body.getString("error"));
      assertThat(classification.reported().getClass().getName()).as("exception")
          .isEqualTo(body.getString("exception"));
      assertThat(classification.exceptionArgs()).as("exceptionArgs")
          .isEqualTo(body.has("exceptionArgs") ? body.getString("exceptionArgs") : null);
      if (failure.name().equals("ClusterCapabilityNotReadyException"))
        assertThat(classification.exceptionArgs()).isEqualTo("security-v2|node-b:2424,node-c:2424");
    }));
  }

  /** The unmapped fallbacks too: each generic 500 keeps its own label and reported throwable. */
  @Test
  void theClassificationOfAnUnrecognisedFailureIsTheGeneric500ItIsSentAs() {
    for (final RuntimeException unrecognised : List.of(new IllegalStateException("unexpected internal state"),
        new TransactionException("Error on commit"),
        new CommandExecutionException("boom", new IllegalStateException("x")))) {
      final AbstractServerHttpHandler.ErrorClassification classification = handler(unrecognised)
          .classifyError(unrecognised);
      final JSONObject body = new JSONObject(handle(unrecognised).body);
      assertThat(classification.status()).isEqualTo(500);
      assertThat(classification.message()).isEqualTo(body.getString("error"));
      assertThat(classification.reported().getClass().getName()).isEqualTo(body.getString("exception"));
    }
  }

  private record HandledResponse(int statusCode, String body, HeaderMap responseHeaders) {
  }

  /**
   * Issue #8343: the in-flight refusal is answered 409 with a {@code Retry-After}, and its body names the exception and
   * carries the back-off in {@code exceptionArgs} - what a follower that forwarded the request rebuilds it from. Raised
   * inside the auto-commit wrapper, as a follower's forward to the leader raises it, it used to be a 500 with neither.
   */
  @Test
  void aRequestStillInFlightIsAConflictWithARetryAfterWrappedOrNot() {
    for (final RuntimeException shape : List.<RuntimeException>of(
        new RequestStillInFlightException("Retry it later with the same X-Request-Id", 7),
        new TransactionException("Error on executing command",
            new RequestStillInFlightException("Retry it later with the same X-Request-Id", 7)))) {
      final HandledResponse response = handle(shape);

      assertThat(response.statusCode).as("body=%s", response.body).isEqualTo(409);
      assertThat(response.responseHeaders.getFirst("Retry-After")).isEqualTo("7");
      final JSONObject json = new JSONObject(response.body);
      assertThat(json.getString("error")).contains("still executing");
      assertThat(json.getString("exception")).isEqualTo(RequestStillInFlightException.class.getName());
      assertThat(json.getString("exceptionArgs")).isEqualTo("7");
    }
  }

  /**
   * Issue #8355: a refusal-before-execution with a back-off, rebuilt by a follower from the answer of the node it
   * forwarded a SQL write to, is answered 503 with that {@code Retry-After}, and its body names the exception and
   * carries the back-off in {@code exceptionArgs} so one more hop can rebuild it. Raised inside the auto-commit
   * wrapper, as the forward raises it, it used to be a 500 with neither.
   */
  @Test
  void aRetryLaterIsServiceUnavailableWithARetryAfterWrappedOrNot() {
    for (final RuntimeException shape : List.<RuntimeException>of(
        new RetryLaterException("Server is installing a snapshot, please retry", 8),
        new TransactionException("Error on executing command",
            new RetryLaterException("Server is installing a snapshot, please retry", 8)),
        new CommandExecutionException("Error on executing command",
            new RetryLaterException("Server is installing a snapshot, please retry", 8)))) {
      final HandledResponse response = handle(shape);

      assertThat(response.statusCode).as("body=%s", response.body).isEqualTo(503);
      assertThat(response.responseHeaders.getFirst("Retry-After")).isEqualTo("8");
      final JSONObject json = new JSONObject(response.body);
      assertThat(json.getString("exception")).isEqualTo(RetryLaterException.class.getName());
      assertThat(json.getString("exceptionArgs")).isEqualTo("8");
    }
  }

  /** Every other failure is sent without a Retry-After: the header belongs to the refusals that state a back-off. */
  @Test
  void noOtherFailureIsSentWithARetryAfter() {
    for (final MappedFailure failure : MAPPED_FAILURES)
      if (!failure.name().equals("RequestStillInFlightException") && !failure.name().equals("RetryLaterException"))
        assertThat(handle(failure.factory().get()).responseHeaders.getFirst("Retry-After"))
            .as("%s must not carry a Retry-After", failure.name())
            .isNull();
  }

  private ThrowingHandler handler(final RuntimeException toThrow) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getObservationRegistry()).thenReturn(ObservationRegistry.create());
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getServerName()).thenReturn("test");

    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    return new ThrowingHandler(httpServer, toThrow);
  }

  /**
   * Runs the real {@link AbstractServerHttpHandler#handleRequest} against a handler whose {@code execute()}
   * throws the given exception, and captures the status code and JSON body the classification produces.
   */
  private HandledResponse handle(final RuntimeException toThrow) {
    final Sender sender = mock(Sender.class);
    final HttpServerExchange exchange = mock(HttpServerExchange.class);
    final int[] statusCode = { 200 };
    when(exchange.setStatusCode(anyInt())).thenAnswer(invocation -> {
      statusCode[0] = invocation.getArgument(0);
      return exchange;
    });
    when(exchange.getStatusCode()).thenAnswer(invocation -> statusCode[0]);
    when(exchange.getRequestHeaders()).thenReturn(new HeaderMap());
    final HeaderMap responseHeaders = new HeaderMap();
    when(exchange.getResponseHeaders()).thenReturn(responseHeaders);
    when(exchange.getRequestMethod()).thenReturn(Methods.POST);
    when(exchange.getRelativePath()).thenReturn("/command/graph");
    when(exchange.getResponseSender()).thenReturn(sender);

    handler(toThrow).handleRequest(exchange);

    final ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
    verify(sender).send(body.capture());
    return new HandledResponse(statusCode[0], body.getValue(), responseHeaders);
  }

  /** Handler whose execute() throws, standing in for a command that fails while running. */
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
      // Skip the Authorization machinery: this test targets the error classification only.
      return false;
    }
  }
}
