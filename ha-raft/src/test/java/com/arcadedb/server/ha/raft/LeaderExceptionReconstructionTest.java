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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DeletedEntityException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.ExternalResourceException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.ObjectInUseException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ReplicationQueueFullException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.RequestStillInFlightException;
import com.arcadedb.server.http.RetryLaterException;
import com.arcadedb.utility.FileUtils;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How a follower rebuilds the exception the leader refused a forwarded write with ({@code RaftReplicatedDatabase.reconstructLeaderException}):
 * retryable and non-retryable outcomes, in-flight and snapshot-install refusals, and validation kinds.
 */
class LeaderExceptionReconstructionTest {
  // ---- #8479 ----
  // Issue #8479: a follower that forwarded a write to the leader rebuilt the leader's refusal only when its exception class
  // was an exact key of {@code LEADER_EXCEPTION_FACTORIES}. A {@link ReplicatedPageConflictException} (issue #6965), which
  // the leader answers with 503 as the retryable conflict it is, missed that lookup and came back as a plain
  // {@link TransactionException}: {@code database.transaction(..., retries)} on the follower did not retry it, and the
  // follower's HTTP client got a non-retryable error for a conflict a plain retry resolves.
  //
  // The same exact-name lookup dropped the other refusals the leader's group committer raises BEFORE the entry reaches
  // the Raft log - {@link ReplicationQueueFullException} and a plain {@link QuorumNotReachedException} - so they are
  // rebuilt too. The two outcomes that mean the entry did, or may have, reached the log
  // ({@link MajorityCommittedAllFailedException}, {@link ReplicationDispatchedTimeoutException}) are deliberately NOT: a
  // retry of either can apply the write twice, so they stay non-retryable on the follower (and, since issue #8481, are no
  // longer {@link QuorumNotReachedException} subtypes on the leader either).

  private static final String CONFLICT_DETAIL = "[6965 db='graph' page=28/25 base=75 cluster=76] Concurrent modification on "
      + "page 28/25 of database 'graph': the transaction was validated against version 75 but the cluster is at version 76. "
      + "Please retry the operation";

  private static String leaderBody(final String exceptionClass, final String detail) {
    return "{\"error\":\"Cannot execute command\",\"requestId\":\"r-1\",\"exception\":\"" + exceptionClass + "\",\"detail\":\""
        + detail + "\"}";
  }

  // Regression for #8479: a page conflict from the leader is rebuilt as the retryable page conflict
  @Test
  void aPageConflictFromTheLeaderIsRebuiltAsTheRetryablePageConflict() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(ReplicatedPageConflictException.class.getName(), CONFLICT_DETAIL), null);

    assertThat(rebuilt).isInstanceOf(ReplicatedPageConflictException.class);
    assertThat(rebuilt).isInstanceOf(ConcurrentModificationException.class);
    assertThat(rebuilt).isInstanceOf(NeedRetryException.class);
    assertThat(rebuilt.getMessage()).isEqualTo(CONFLICT_DETAIL);

    // The (String) constructor parses the machine-readable header back, so the page and version survive the hop.
    final ReplicatedPageConflictException conflict = (ReplicatedPageConflictException) rebuilt;
    assertThat(conflict.getDatabaseName()).isEqualTo("graph");
    assertThat(conflict.getFileId()).isEqualTo(28);
    assertThat(conflict.getPageNumber()).isEqualTo(25);
    assertThat(conflict.getClusterVersion()).isEqualTo(76);
  }

  // Regression for #8479: a page conflict without its detail is still retryable
  /** In production mode the leader may conceal 'detail': the rebuilt conflict is still retryable, only without its page. */
  @Test
  void aPageConflictWithoutItsDetailIsStillRetryable() {
    final String body = "{\"error\":\"Cannot execute command\",\"exception\":\"" + ReplicatedPageConflictException.class.getName()
        + "\"}";

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503, body, null);

    assertThat(rebuilt).isInstanceOf(ReplicatedPageConflictException.class);
    assertThat(((ReplicatedPageConflictException) rebuilt).getClusterVersion()).isEqualTo(-1);
  }

  // Regression for #8479: a full replication queue on the leader is rebuilt as retryable
  @Test
  void aFullReplicationQueueOnTheLeaderIsRebuiltAsRetryable() {
    final String detail = "Replication queue is full (0 remaining of 1024 max). Server is overloaded, retry later";
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(ReplicationQueueFullException.class.getName(), detail), null);

    assertThat(rebuilt).isInstanceOf(ReplicationQueueFullException.class);
    assertThat(rebuilt).isInstanceOf(NeedRetryException.class);
    assertThat(rebuilt.getMessage()).isEqualTo(detail);
  }

  // Regression for #8479: a quorum not reached before dispatch is rebuilt as retryable
  @Test
  void aQuorumNotReachedBeforeDispatchIsRebuiltAsRetryable() {
    final String detail = "Group commit timed out after 20000ms (cancelled before dispatch)";
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(QuorumNotReachedException.class.getName(), detail), null);

    assertThat(rebuilt).isInstanceOf(QuorumNotReachedException.class);
    assertThat(rebuilt).isInstanceOf(NeedRetryException.class);
    assertThat(rebuilt.getMessage()).isEqualTo(detail);
  }

  // Regression for #8479: a majority committed refusal stays non retryable
  /**
   * MAJORITY committed the entry: the write is durable cluster-wide. Retrying the forwarded command would run it a second
   * time, so the follower must not rebuild it as a NeedRetryException. Since issue #8481 the leader answers it 409; the 503
   * below is what a leader that predates #8481 sends, and the rebuild keys on the exception class, not on the status.
   */
  @Test
  void aMajorityCommittedRefusalStaysNonRetryable() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(MajorityCommittedAllFailedException.class.getName(), "ALL quorum not reached"), null);

    assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
    assertThat(rebuilt).isInstanceOf(TransactionException.class);
  }

  // Regression for #8479: a dispatched timeout stays non retryable
  /** The entry was dispatched and may still commit: an indeterminate outcome is not safe to retry either. */
  @Test
  void aDispatchedTimeoutStaysNonRetryable() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        leaderBody(ReplicationDispatchedTimeoutException.class.getName(), "Group commit timed out (entry was dispatched to Raft)"),
        null);

    assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
    assertThat(rebuilt).isInstanceOf(TransactionException.class);
  }

  // Regression for #8479: the followers transaction retry loop retries the rebuilt conflict only
  /**
   * The caller the issue is about: {@code database.transaction(..., retries)} on the follower. The rebuilt page conflict
   * must make it run the block again, and the rebuilt MAJORITY-committed refusal must make it stop at the first attempt.
   */
  @Test
  void theFollowersTransactionRetryLoopRetriesTheRebuiltConflictOnly() {
    final String path = "target/databases/LeaderExceptionReconstructionTest";
    FileUtils.deleteRecursively(new File(path));
    try (final DatabaseFactory factory = new DatabaseFactory(path); final Database db = factory.create()) {
      final AtomicInteger conflictAttempts = new AtomicInteger();
      db.transaction(() -> {
        if (conflictAttempts.incrementAndGet() == 1)
          throw RaftReplicatedDatabase.reconstructLeaderException(503,
              leaderBody(ReplicatedPageConflictException.class.getName(), CONFLICT_DETAIL), null);
      }, false, 3);
      assertThat(conflictAttempts.get()).isEqualTo(2);

      final AtomicInteger committedAttempts = new AtomicInteger();
      assertThatThrownBy(() -> db.transaction(() -> {
        committedAttempts.incrementAndGet();
        throw RaftReplicatedDatabase.reconstructLeaderException(503,
            leaderBody(MajorityCommittedAllFailedException.class.getName(), "ALL quorum not reached"), null);
      }, false, 3)).isInstanceOf(TransactionException.class);
      assertThat(committedAttempts.get()).isEqualTo(1);
    } finally {
      FileUtils.deleteRecursively(new File(path));
    }
  }
  /**
   * Issue #8481: {@link MajorityCommittedAllFailedException} and {@link ReplicationDispatchedTimeoutException} extended
   * {@code QuorumNotReachedException}, and therefore {@link NeedRetryException}, although both are raised AFTER the entry
   * was dispatched to Raft: the first once the MAJORITY committed it (the leader completes its local commit before
   * rethrowing it), the second when the outcome is unknown and the entry may still commit. Every retry contract read them
   * as "nothing happened, retry" - {@code Database.transaction(block, joinTx, retries)} re-ran the block, the HTTP layer
   * answered 503, and the Java remote client resent the request - so a committed write ran a second time.
   *
   * The end-to-end paths (the leader-local retry loop against a real cluster, and the HTTP answer read by the remote
   * client) are driven by {@link HaForwardedWriteIdempotencyDisruptiveIT}; this class pins the contract each of them keys
   * on without a cluster.
   */
  @Nested
  class CommittedReplicationOutcome {

    private static final String PATH = "target/databases/LeaderExceptionReconstructionTest-CommittedReplicationOutcome";

    @AfterEach
    void cleanup() {
      FileUtils.deleteRecursively(new File(PATH));
    }

    // Regression for #8481: a majority committed outcome is A committed remotely outcome and not retryable
    @Test
    void aMajorityCommittedOutcomeIsACommittedRemotelyOutcomeAndNotRetryable() {
      final MajorityCommittedAllFailedException e = new MajorityCommittedAllFailedException("ALL quorum not reached");

      assertThat(e).isNotInstanceOf(NeedRetryException.class);
      // The type the HTTP layer answers 409 "do not retry" for, and the one the remote client rebuilds.
      assertThat(e).isInstanceOf(TransactionCommittedRemotelyException.class);
    }

    // Regression for #8481: a dispatched timeout is A non retryable transaction failure
    @Test
    void aDispatchedTimeoutIsANonRetryableTransactionFailure() {
      final ReplicationDispatchedTimeoutException e = new ReplicationDispatchedTimeoutException("outcome unknown");

      assertThat(e).isNotInstanceOf(NeedRetryException.class);
      assertThat(e).isInstanceOf(TransactionException.class);
      // Not a committed-remotely outcome either: the entry may NOT have committed, so it must not claim it did.
      assertThat(e).isNotInstanceOf(TransactionCommittedRemotelyException.class);
    }

    // Regression for #8481: the transaction retry loop runs the block once for A majority committed outcome
    /**
     * The retry loop the issue names: {@code database.transaction(block, false, retries)} catches NeedRetryException and
     * runs the block again. On the leader these two surface from {@code commit()} inside that loop.
     */
    @Test
    void theTransactionRetryLoopRunsTheBlockOnceForAMajorityCommittedOutcome() {
      assertBlockRunsOnce(() -> new MajorityCommittedAllFailedException("ALL quorum not reached after MAJORITY commit"),
          MajorityCommittedAllFailedException.class);
    }

    // Regression for #8481: the transaction retry loop runs the block once for A dispatched timeout
    @Test
    void theTransactionRetryLoopRunsTheBlockOnceForADispatchedTimeout() {
      assertBlockRunsOnce(() -> new ReplicationDispatchedTimeoutException("Group commit timed out (entry was dispatched to Raft)"),
          ReplicationDispatchedTimeoutException.class);
    }

    // Regression for #8481: a forwarded majority committed outcome is rebuilt as committed remotely
    /**
     * A follower forwarding a write rebuilds the leader's answer by class name. The leader now answers the MAJORITY
     * committed outcome 409, and the follower must rebuild it as the committed-remotely type so it answers its own client
     * 409 too, rather than the 500 a plain TransactionException leaves as.
     */
    @Test
    void aForwardedMajorityCommittedOutcomeIsRebuiltAsCommittedRemotely() {
      final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(409,
          leaderBody(MajorityCommittedAllFailedException.class.getName(), "ALL quorum not reached"), null);

      assertThat(rebuilt).isInstanceOf(TransactionCommittedRemotelyException.class);
      assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
      assertThat(rebuilt.getMessage()).isEqualTo("ALL quorum not reached");
    }

    // Regression for #8481: a forwarded dispatched timeout stays A non retryable transaction failure
    @Test
    void aForwardedDispatchedTimeoutStaysANonRetryableTransactionFailure() {
      final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(500,
          leaderBody(ReplicationDispatchedTimeoutException.class.getName(), "outcome unknown"), null);

      assertThat(rebuilt).isInstanceOf(TransactionException.class);
      assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
      assertThat(rebuilt).isNotInstanceOf(TransactionCommittedRemotelyException.class);
    }

    private static void assertBlockRunsOnce(final Supplier<RuntimeException> outcome, final Class<? extends RuntimeException> expected) {
      FileUtils.deleteRecursively(new File(PATH));
      try (final DatabaseFactory factory = new DatabaseFactory(PATH); final Database db = factory.create()) {
        final AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> db.transaction(() -> {
          attempts.incrementAndGet();
          throw outcome.get();
        }, false, 3)).isInstanceOf(expected);
        assertThat(attempts.get()).as("a committed or possibly committed write must not be run again").isEqualTo(1);
      }
    }

    private static String leaderBody(final String exceptionClass, final String detail) {
      return "{\"error\":\"Cannot execute command\",\"requestId\":\"r-1\",\"exception\":\"" + exceptionClass + "\",\"detail\":\""
          + detail + "\"}";
    }

  }
  // ---- #8343 ----
  // Issue #8343: a follower that forwarded a SQL write to the leader, carrying the client's {@code X-Request-Id}
  // (issue #8323), and got back the leader's "an identical request is still executing" {@code 409} (issue #8324) turned
  // it into a plain {@link TransactionException} - the body named no exception class - and answered its client 500
  // "Error on transaction commit" with no {@code Retry-After}. Nothing had run twice, but the client read a failure
  // instead of "retry later with the same id".
  //
  // The bodies below are the ones the leader's {@code AbstractServerHttpHandler.buildErrorBody} produces for the refusal,
  // in development mode (with {@code detail}) and in production mode (without it).

  private static final String DEVELOPMENT_BODY = "{\"error\":\"A request with the same X-Request-Id is still executing\","
      + "\"exception\":\"" + RequestStillInFlightException.class.getName() + "\",\"exceptionArgs\":\"5\","
      + "\"detail\":\"The request was not executed again. Retry it later with the same X-Request-Id to receive the "
      + "result of the execution in progress\"}";

  private static final String PRODUCTION_BODY = "{\"error\":\"A request with the same X-Request-Id is still executing\","
      + "\"exception\":\"" + RequestStillInFlightException.class.getName() + "\",\"exceptionArgs\":\"5\"}";

  // Regression for #8343: the leaders in flight refusal is rebuilt as itself with its back off
  @Test
  void theLeadersInFlightRefusalIsRebuiltAsItselfWithItsBackOff() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(409, DEVELOPMENT_BODY);

    assertThat(rebuilt).isInstanceOf(RequestStillInFlightException.class);
    assertThat(((RequestStillInFlightException) rebuilt).getRetryAfterSeconds()).isEqualTo(5L);
    assertThat(rebuilt.getMessage()).contains("Retry it later with the same X-Request-Id");
  }

  // Regression for #8343: a production mode body without detail is rebuilt too
  @Test
  void aProductionModeBodyWithoutDetailIsRebuiltToo() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(409, PRODUCTION_BODY);

    assertThat(rebuilt).isInstanceOf(RequestStillInFlightException.class);
    assertThat(((RequestStillInFlightException) rebuilt).getRetryAfterSeconds()).isEqualTo(5L);
    assertThat(rebuilt.getMessage()).contains("still executing");
  }

  // Regression for #8343: the rebuilt refusal is not A server side retryable exception
  /**
   * Not retryable on the server side: a retry loop on the follower that caught it would resend the forward under a new
   * forward ordinal, which the leader keys separately, and run the write a second time.
   */
  @Test
  void theRebuiltRefusalIsNotAServerSideRetryableException() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(409, DEVELOPMENT_BODY);

    assertThat(rebuilt).isNotInstanceOf(NeedRetryException.class);
    assertThat(rebuilt).isNotInstanceOf(TransactionException.class);
  }

  // Regression for #8343: a missing or malformed back off falls back to the default instead of losing the refusal
  @Test
  void aMissingOrMalformedBackOffFallsBackToTheDefaultInsteadOfLosingTheRefusal() {
    for (final String args : new String[] { null, "", "soon", "-3", "0" }) {
      final String body = "{\"error\":\"A request with the same X-Request-Id is still executing\",\"exception\":\""
          + RequestStillInFlightException.class.getName() + "\"" + (args != null ? ",\"exceptionArgs\":\"" + args + "\"" : "")
          + "}";

      final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(409, body);

      assertThat(rebuilt).as("exceptionArgs=%s", args).isInstanceOf(RequestStillInFlightException.class);
      assertThat(((RequestStillInFlightException) rebuilt).getRetryAfterSeconds())
          .as("exceptionArgs=%s", args)
          .isGreaterThanOrEqualTo(1L);
    }
    assertThat(RaftReplicatedDatabase.parseRetryAfterSeconds("soon"))
        .isEqualTo(RaftReplicatedDatabase.DEFAULT_IN_FLIGHT_RETRY_AFTER_SECONDS);
    assertThat(RaftReplicatedDatabase.parseRetryAfterSeconds(" 12 ")).isEqualTo(12L);
  }
  // ---- #8355 ----
  // Issue #8355: a follower that forwarded a SQL write to a node answering {@code 503} + {@code Retry-After: 5} because
  // it is installing a snapshot - refused in {@code AbstractServerHttpHandler.handleRequest} before any handler, and so
  // before the write, ran - rebuilt that answer as a plain {@link TransactionException}: the body names no exception
  // class. The follower then answered its client 500 "Error on transaction commit" with no {@code Retry-After}, for a
  // write that never ran and is safe to retry.
  //
  // The refusal is rebuilt only when the answer proves it is that refusal: the typed body the gate sends since this fix
  // ({@link #SNAPSHOT_INSTALL_BODY}), or - from a leader that predates it, during a rolling upgrade - the gate's own exact
  // untyped body ({@link #PRE_8355_SNAPSHOT_INSTALL_BODY}). Any other 503 may come from something between the two nodes,
  // after the write ran, and must not be retried by this node (PR #8402 review).

  private static final String SNAPSHOT_INSTALL_BODY = "{\"error\":\"" + RetryLaterException.SNAPSHOT_INSTALL_REFUSAL
      + "\",\"detail\":\"" + RetryLaterException.SNAPSHOT_INSTALL_REFUSAL + "\",\"exception\":\""
      + RetryLaterException.class.getName() + "\",\"exceptionArgs\":\"5\"}";

  private static final String PRE_8355_SNAPSHOT_INSTALL_BODY =
      "{\"error\":\"Server is installing a snapshot, please retry\",\"detail\":\"\"}";

  // Regression for #8355: the snapshot install refusal is rebuilt as A retry later with the leaders back off
  @Test
  void theSnapshotInstallRefusalIsRebuiltAsARetryLaterWithTheLeadersBackOff() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503, SNAPSHOT_INSTALL_BODY, "5");

    assertThat(rebuilt).isInstanceOf(RetryLaterException.class);
    assertThat(((RetryLaterException) rebuilt).getRetryAfterSeconds()).isEqualTo(5L);
    assertThat(rebuilt.getMessage()).contains("Server is installing a snapshot, please retry");
  }

  // Regression for #8355: the untyped refusal of A leader that predates the fix is rebuilt too
  /** A leader that predates this fix sends the refusal untyped; its exact text is still proof enough of its origin. */
  @Test
  void theUntypedRefusalOfALeaderThatPredatesTheFixIsRebuiltToo() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
        PRE_8355_SNAPSHOT_INSTALL_BODY, "5");

    assertThat(rebuilt).isInstanceOf(RetryLaterException.class);
    assertThat(((RetryLaterException) rebuilt).getRetryAfterSeconds()).isEqualTo(5L);
    assertThat(rebuilt.getMessage())
        .as("the empty detail must not hide the reason the leader gave")
        .contains("Server is installing a snapshot, please retry");
  }

  // Regression for #8355: the rebuilt refusal is A need retry exception and not A failed transaction
  /**
   * Retryable on the server side too, deliberately, unlike the in-flight 409 (issue #8343): the refusing node answered
   * before its idempotency gate ran, so it neither executed the write nor reserved its {@code X-Request-Id}. A retry
   * loop on the follower that re-forwards it under a new forward ordinal (issue #8323) sends the write for the first
   * time, not the second.
   */
  @Test
  void theRebuiltRefusalIsANeedRetryExceptionAndNotAFailedTransaction() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503, SNAPSHOT_INSTALL_BODY, "5");

    assertThat(rebuilt).isInstanceOf(NeedRetryException.class);
    assertThat(rebuilt).isNotInstanceOf(TransactionException.class);
  }

  // Regression for #8355: a back off the leader did not state or stated as A date falls back to the default
  /** The untyped refusal states its back-off in the Retry-After header alone. */
  @Test
  void aBackOffTheLeaderDidNotStateOrStatedAsADateFallsBackToTheDefault() {
    for (final String header : new String[] { null, "", "Wed, 21 Oct 2026 07:28:00 GMT" }) {
      final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503,
          PRE_8355_SNAPSHOT_INSTALL_BODY, header);

      assertThat(rebuilt).as("Retry-After=%s", header).isInstanceOf(RetryLaterException.class);
      assertThat(((RetryLaterException) rebuilt).getRetryAfterSeconds()).as("Retry-After=%s", header)
          .isEqualTo(RaftReplicatedDatabase.DEFAULT_IN_FLIGHT_RETRY_AFTER_SECONDS);
    }
    assertThat(((RetryLaterException) RaftReplicatedDatabase.reconstructLeaderException(503,
        PRE_8355_SNAPSHOT_INSTALL_BODY, "12")).getRetryAfterSeconds()).isEqualTo(12L);
  }

  // Regression for #8355: a back off below one second is clamped to one second
  /** A back-off stated as a number below one second is a number, so it is clamped to one second, not defaulted. */
  @Test
  void aBackOffBelowOneSecondIsClampedToOneSecond() {
    for (final String header : new String[] { "-1", "0" })
      assertThat(((RetryLaterException) RaftReplicatedDatabase.reconstructLeaderException(503,
          PRE_8355_SNAPSHOT_INSTALL_BODY, header)).getRetryAfterSeconds()).as("Retry-After=%s", header).isEqualTo(1L);
  }

  // Regression for #8355: an anonymous503 is still A failed transaction
  /**
   * The review finding on PR #8402: an anonymous 503 - no body, or a JSON body that names no exception and is not the
   * gate's own - may have been produced by something between the two nodes after the write ran. A retry by this node
   * would go out under a new forward ordinal, which the leader keys separately, and could run the write twice.
   */
  @Test
  void anAnonymous503IsStillAFailedTransaction() {
    for (final String body : new String[] { null, "", "{}", "{\"error\":\"Service Unavailable\"}",
        "{\"error\":\"upstream connect error\",\"detail\":\"\"}" })
      assertThat(RaftReplicatedDatabase.reconstructLeaderException(503, body, "5")).as("body=%s", body)
          .isExactlyInstanceOf(TransactionException.class);
  }

  // Regression for #8355: a typed retry later from another hop is rebuilt with its back off
  /**
   * One more hop: a node that forwarded the write to a follower that itself rebuilt the refusal gets the follower's
   * typed answer, whose back-off travels in {@code exceptionArgs}, and must keep both.
   */
  @Test
  void aTypedRetryLaterFromAnotherHopIsRebuiltWithItsBackOff() {
    final String body = "{\"error\":\"Cannot execute command\",\"exception\":\"" + RetryLaterException.class.getName()
        + "\",\"exceptionArgs\":\"7\",\"detail\":\"Server is installing a snapshot, please retry\"}";

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503, body, null);

    assertThat(rebuilt).isInstanceOf(RetryLaterException.class);
    assertThat(((RetryLaterException) rebuilt).getRetryAfterSeconds()).isEqualTo(7L);
    assertThat(rebuilt.getMessage()).contains("installing a snapshot");
  }

  // Regression for #8355: a non json body or another untyped status is still A failed transaction
  /**
   * What stays as it was. A 503 whose body is not JSON did not come from ArcadeDB's own gate - a proxy in between, say
   * - and nothing proves the write did not run behind it. Every other untyped status is not a refusal-before-execution
   * contract at all.
   */
  @Test
  void aNonJsonBodyOrAnotherUntypedStatusIsStillAFailedTransaction() {
    assertThat(RaftReplicatedDatabase.reconstructLeaderException(503, "<html>Service Unavailable</html>", "5"))
        .isExactlyInstanceOf(TransactionException.class);
    assertThat(RaftReplicatedDatabase.reconstructLeaderException(500, PRE_8355_SNAPSHOT_INSTALL_BODY, "5"))
        .isExactlyInstanceOf(TransactionException.class);
    assertThat(RaftReplicatedDatabase.reconstructLeaderException(502, "", null))
        .isExactlyInstanceOf(TransactionException.class);
  }

  // Regression for #8355: the form without A header still rebuilds the refusal
  /** The two-argument form every existing caller and test uses keeps its meaning: no header, the body's back-off. */
  @Test
  void theFormWithoutAHeaderStillRebuildsTheRefusal() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503, SNAPSHOT_INSTALL_BODY);

    assertThat(rebuilt).isInstanceOf(RetryLaterException.class);
    assertThat(((RetryLaterException) rebuilt).getRetryAfterSeconds()).isEqualTo(5L);
  }
  // ---- #11314 ----
  // Issue #11314: a write a follower forwards to the leader and the leader refuses for a violated constraint comes back as the
  // {@link ValidationException} the leader raised, with the constraint the leader named in {@code exceptionArgs}, so the follower
  // answers its client with the same kind; an unknown function comes back as itself rather than as a server-side
  // {@code TransactionException}.

  // Regression for #11314: a violated constraint keeps its kind across the forward
  @Test
  void aViolatedConstraintKeepsItsKindAcrossTheForward() {
    final String body = new JSONObject().put("error", "Constraint violated (NOT_NULL)").put("exception", ValidationException.class.getName())
        .put("exceptionArgs", "NOT_NULL").put("detail", "The property 'T.p' cannot be null").toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(400, body, null);

    assertThat(rebuilt).isInstanceOf(ValidationException.class).hasMessage("The property 'T.p' cannot be null");
    assertThat(((ValidationException) rebuilt).getKind()).isEqualTo(ValidationException.Kind.NOT_NULL);
  }

  // Regression for #11314: a leader that names no kind is rebuilt with none
  @Test
  void aLeaderThatNamesNoKindIsRebuiltWithNone() {
    final String body = new JSONObject().put("error", "Validation error").put("exception", ValidationException.class.getName())
        .put("detail", "refused").toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(400, body, null);

    assertThat(rebuilt).isInstanceOf(ValidationException.class);
    assertThat(((ValidationException) rebuilt).getKind()).isNull();
  }

  // Regression for #11314: an unknown function is rebuilt as itself
  @Test
  void anUnknownFunctionIsRebuiltAsItself() {
    final String body = new JSONObject().put("error", "Cannot execute command").put("exception", UnknownFunctionException.class.getName())
        .put("detail", "Unknown function name 'foo'").toString();

    assertThat(RaftReplicatedDatabase.reconstructLeaderException(400, body, null)).isInstanceOf(UnknownFunctionException.class)
        .hasMessage("Unknown function name 'foo'");
  }

  // ---- #11974 ----
  // Issue #11974: three openCypher user errors are typed on the leader (409 / 404 / 400), so a write a follower forwards and the leader
  // refuses with one of them must come back as that type, where it was rebuilt as a plain TransactionException, which the follower
  // answered 500 "Error on transaction commit" (and Bolt on the follower as an unexplained server fault).

  // Regression for #11974: a DELETE of a node that still has relationships, refused by the leader
  @Test
  void aDeleteOfAConnectedNodeIsRebuiltAsObjectInUse() {
    final String body = new JSONObject().put("error", "Cannot execute command").put("exception", ObjectInUseException.class.getName())
        .put("detail", "DeleteConnectedNode: Cannot delete node #1:0 because it still has relationships").toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(409, body, null);

    assertThat(rebuilt).isExactlyInstanceOf(ObjectInUseException.class)
        .hasMessage("DeleteConnectedNode: Cannot delete node #1:0 because it still has relationships");
  }

  // Regression for #11974: a use of a node deleted earlier in the statement, refused by the leader
  @Test
  void aUseOfAnEntityTheStatementDeletedIsRebuiltAsDeletedEntity() {
    final String body = new JSONObject().put("error", "Entity deleted").put("exception", DeletedEntityException.class.getName())
        .put("detail", "DeletedEntityAccess: Cannot access a deleted entity").toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(404, body, null);

    assertThat(rebuilt).isExactlyInstanceOf(DeletedEntityException.class).hasMessage("DeletedEntityAccess: Cannot access a deleted entity");
    assertThat(ErrorCategory.of(rebuilt)).isEqualTo(ErrorCategory.NOT_FOUND);
  }

  // Regression for #11974: a LOAD CSV URL that cannot be opened, refused by the leader
  @Test
  void anUnreadableExternalResourceIsRebuiltAsExternalResource() {
    final String body = new JSONObject().put("error", "Cannot execute command").put("exception", ExternalResourceException.class.getName())
        .put("detail", "Error opening CSV file: file:///missing.csv").toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(400, body, null);

    assertThat(rebuilt).isExactlyInstanceOf(ExternalResourceException.class).hasMessage("Error opening CSV file: file:///missing.csv");
    assertThat(ErrorCategory.of(rebuilt)).isEqualTo(ErrorCategory.VALIDATION);
  }
}
