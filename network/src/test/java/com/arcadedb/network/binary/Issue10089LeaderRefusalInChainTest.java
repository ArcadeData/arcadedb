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
package com.arcadedb.network.binary;

import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TransactionException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10089: {@link ServerIsNotTheLeaderException#redirectIn} is the one authority on "is this a leader refusal a
 * client can be redirected on", read by every transport, but it answers only for a refusal that NAMES the leader. The
 * gRPC mapper also asks the cluster where the leader is, so a refusal that names none is still one it can redirect on:
 * {@link ServerIsNotTheLeaderException#refusalIn} is the same walk of the cause chain without the address condition,
 * so that transport does not need a walk of its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10089LeaderRefusalInChainTest {
  private static ServerIsNotTheLeaderException refusal(final String leader) {
    return new ServerIsNotTheLeaderException("not the leader", leader);
  }

  @Test
  void aRefusalIsFoundAtTheHeadAndAnywhereAlongTheChain() {
    final ServerIsNotTheLeaderException refusal = refusal("leader:2480");

    assertThat(ServerIsNotTheLeaderException.refusalIn(refusal)).isSameAs(refusal);
    assertThat(ServerIsNotTheLeaderException.refusalIn(new DatabaseOperationException("wrapped", refusal))).isSameAs(refusal);
    assertThat(ServerIsNotTheLeaderException.refusalIn(
        new ExecutionException(new TransactionException("commit", new DatabaseOperationException("listener", refusal)))))
        .isSameAs(refusal);
  }

  @Test
  void refusalInDoesNotNeedTheRefusalToNameALeaderButRedirectInDoes() {
    final ServerIsNotTheLeaderException midElection = refusal(null);
    final DatabaseOperationException wrapped = new DatabaseOperationException("wrapped", midElection);

    assertThat(ServerIsNotTheLeaderException.refusalIn(wrapped)).isSameAs(midElection);
    assertThat(ServerIsNotTheLeaderException.redirectIn(wrapped)).isNull();
    assertThat(ServerIsNotTheLeaderException.redirectIn(new DatabaseOperationException("wrapped", refusal("  ")))).isNull();
    assertThat(ServerIsNotTheLeaderException.redirectIn(new DatabaseOperationException("wrapped", refusal("leader:2480"))))
        .isNotNull();
  }

  @Test
  void aChainThatIsNotARefusalAnswersNull() {
    assertThat(ServerIsNotTheLeaderException.refusalIn(null)).isNull();
    assertThat(ServerIsNotTheLeaderException.refusalIn(new IllegalStateException("boom"))).isNull();
    assertThat(ServerIsNotTheLeaderException.refusalIn(new DatabaseOperationException("wrapped", new IllegalStateException("boom"))))
        .isNull();
  }

  @Test
  void aStatementTerminatedOnRequestKeepsThatVerdict() {
    final QueryTerminatedException stop = new QueryTerminatedException("terminated");
    stop.initCause(refusal("leader:2480"));
    final DatabaseOperationException terminated = new DatabaseOperationException("stopped", stop);

    assertThat(ServerIsNotTheLeaderException.refusalIn(terminated)).isNull();
    assertThat(ServerIsNotTheLeaderException.redirectIn(terminated)).isNull();
  }
}
