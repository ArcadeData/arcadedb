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

import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.http.RetryLaterException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8355: a follower that forwarded a SQL write to a node answering {@code 503} + {@code Retry-After: 5} because
 * it is installing a snapshot - refused in {@code AbstractServerHttpHandler.handleRequest} before any handler, and so
 * before the write, ran - rebuilt that answer as a plain {@link TransactionException}: the body names no exception
 * class. The follower then answered its client 500 "Error on transaction commit" with no {@code Retry-After}, for a
 * write that never ran and is safe to retry.
 * <p>
 * The refusal is rebuilt only when the answer proves it is that refusal: the typed body the gate sends since this fix
 * ({@link #SNAPSHOT_INSTALL_BODY}), or - from a leader that predates it, during a rolling upgrade - the gate's own exact
 * untyped body ({@link #PRE_8355_SNAPSHOT_INSTALL_BODY}). Any other 503 may come from something between the two nodes,
 * after the write ran, and must not be retried by this node (PR #8402 review).
 */
class Issue8355UntypedLeader503ReconstructionTest {

  private static final String SNAPSHOT_INSTALL_BODY = "{\"error\":\"" + RetryLaterException.SNAPSHOT_INSTALL_REFUSAL
      + "\",\"detail\":\"" + RetryLaterException.SNAPSHOT_INSTALL_REFUSAL + "\",\"exception\":\""
      + RetryLaterException.class.getName() + "\",\"exceptionArgs\":\"5\"}";

  private static final String PRE_8355_SNAPSHOT_INSTALL_BODY =
      "{\"error\":\"Server is installing a snapshot, please retry\",\"detail\":\"\"}";

  @Test
  void theSnapshotInstallRefusalIsRebuiltAsARetryLaterWithTheLeadersBackOff() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503, SNAPSHOT_INSTALL_BODY, "5");

    assertThat(rebuilt).isInstanceOf(RetryLaterException.class);
    assertThat(((RetryLaterException) rebuilt).getRetryAfterSeconds()).isEqualTo(5L);
    assertThat(rebuilt.getMessage()).contains("Server is installing a snapshot, please retry");
  }

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

  /** A back-off stated as a number below one second is a number, so it is clamped to one second, not defaulted. */
  @Test
  void aBackOffBelowOneSecondIsClampedToOneSecond() {
    for (final String header : new String[] { "-1", "0" })
      assertThat(((RetryLaterException) RaftReplicatedDatabase.reconstructLeaderException(503,
          PRE_8355_SNAPSHOT_INSTALL_BODY, header)).getRetryAfterSeconds()).as("Retry-After=%s", header).isEqualTo(1L);
  }

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

  /** The two-argument form every existing caller and test uses keeps its meaning: no header, the body's back-off. */
  @Test
  void theFormWithoutAHeaderStillRebuildsTheRefusal() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(503, SNAPSHOT_INSTALL_BODY);

    assertThat(rebuilt).isInstanceOf(RetryLaterException.class);
    assertThat(((RetryLaterException) rebuilt).getRetryAfterSeconds()).isEqualTo(5L);
  }
}
