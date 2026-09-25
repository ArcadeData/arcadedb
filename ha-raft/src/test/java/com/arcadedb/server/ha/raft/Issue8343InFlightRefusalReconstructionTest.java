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
import com.arcadedb.server.http.RequestStillInFlightException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8343: a follower that forwarded a SQL write to the leader, carrying the client's {@code X-Request-Id}
 * (issue #8323), and got back the leader's "an identical request is still executing" {@code 409} (issue #8324) turned
 * it into a plain {@link TransactionException} - the body named no exception class - and answered its client 500
 * "Error on transaction commit" with no {@code Retry-After}. Nothing had run twice, but the client read a failure
 * instead of "retry later with the same id".
 * <p>
 * The bodies below are the ones the leader's {@code AbstractServerHttpHandler.buildErrorBody} produces for the refusal,
 * in development mode (with {@code detail}) and in production mode (without it).
 */
class Issue8343InFlightRefusalReconstructionTest {

  private static final String DEVELOPMENT_BODY = "{\"error\":\"A request with the same X-Request-Id is still executing\","
      + "\"exception\":\"" + RequestStillInFlightException.class.getName() + "\",\"exceptionArgs\":\"5\","
      + "\"detail\":\"The request was not executed again. Retry it later with the same X-Request-Id to receive the "
      + "result of the execution in progress\"}";

  private static final String PRODUCTION_BODY = "{\"error\":\"A request with the same X-Request-Id is still executing\","
      + "\"exception\":\"" + RequestStillInFlightException.class.getName() + "\",\"exceptionArgs\":\"5\"}";

  @Test
  void theLeadersInFlightRefusalIsRebuiltAsItselfWithItsBackOff() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(409, DEVELOPMENT_BODY);

    assertThat(rebuilt).isInstanceOf(RequestStillInFlightException.class);
    assertThat(((RequestStillInFlightException) rebuilt).getRetryAfterSeconds()).isEqualTo(5L);
    assertThat(rebuilt.getMessage()).contains("Retry it later with the same X-Request-Id");
  }

  @Test
  void aProductionModeBodyWithoutDetailIsRebuiltToo() {
    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(409, PRODUCTION_BODY);

    assertThat(rebuilt).isInstanceOf(RequestStillInFlightException.class);
    assertThat(((RequestStillInFlightException) rebuilt).getRetryAfterSeconds()).isEqualTo(5L);
    assertThat(rebuilt.getMessage()).contains("still executing");
  }

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
}
