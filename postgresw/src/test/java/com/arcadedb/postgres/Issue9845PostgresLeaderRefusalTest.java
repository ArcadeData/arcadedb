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
package com.arcadedb.postgres;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9845: a follower's refusal of leader-only work reached a Postgres client as {@code 40001} with the leader's
 * address dropped. The wire has no redirect, so the SQLSTATE stays, but the message names the leader - in production
 * mode too, where the engine text itself is concealed.
 */
class Issue9845PostgresLeaderRefusalTest {

  private static final String LEADER = "arcadedb-0:2480";

  private static ServerIsNotTheLeaderException refusal(final String leader) {
    return new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server", leader);
  }

  @Test
  void aRefusalNamingTheLeaderKeepsItsSqlStateAndNamesTheLeader() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(refusal(LEADER))).isEqualTo("40001");
    assertThat(PostgresNetworkExecutor.withLeaderRedirect("Error on executing query: refused", refusal(LEADER)))
        .isEqualTo("Error on executing query: refused. The leader is server '" + LEADER + "'");
  }

  @Test
  void aWrappedRefusalNamesTheLeader() {
    final Throwable wrapped = new TransactionException("Error on commit", refusal(LEADER));
    assertThat(PostgresNetworkExecutor.withLeaderRedirect("Error on committing transaction", wrapped))
        .endsWith("The leader is server '" + LEADER + "'");
  }

  @Test
  void theAddressSurvivesConcealment() {
    assertThat(PostgresNetworkExecutor.withLeaderRedirect(ArcadeDBServer.CONCEALED_ERROR_MESSAGE, refusal(LEADER)))
        .startsWith(ArcadeDBServer.CONCEALED_ERROR_MESSAGE)
        .endsWith("The leader is server '" + LEADER + "'");
  }

  @Test
  void theAddressCannotEndTheNulTerminatedField() {
    assertThat(PostgresNetworkExecutor.withLeaderRedirect("refused", refusal("host\u0000tail"))).doesNotContain("\u0000")
        .endsWith("'host tail'");
  }

  @Test
  void aRefusalNamingNoLeaderAddsNothing() {
    assertThat(PostgresNetworkExecutor.withLeaderRedirect("refused", refusal(null))).isEqualTo("refused");
    assertThat(PostgresNetworkExecutor.withLeaderRedirect("refused", refusal(" "))).isEqualTo("refused");
    assertThat(PostgresNetworkExecutor.sqlStateFor(refusal(null))).isEqualTo("40001");
  }

  @Test
  void otherFailuresAreUnchanged() {
    assertThat(PostgresNetworkExecutor.withLeaderRedirect("retry", new ConcurrentModificationException("retry"))).isEqualTo("retry");
    final QueryTerminatedException terminated = new QueryTerminatedException("terminated");
    terminated.initCause(refusal(LEADER));
    assertThat(PostgresNetworkExecutor.withLeaderRedirect("terminated", terminated)).isEqualTo("terminated");
  }
}
