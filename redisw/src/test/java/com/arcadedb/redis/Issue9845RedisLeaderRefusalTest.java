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
package com.arcadedb.redis;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9845, the RESP sibling of the Bolt, MongoDB and Postgres transports: a follower's refusal of leader-only work
 * that names the leader is {@code READONLY} - what Redis answers a write sent to a replica - with the leader named, not
 * the {@code TRYAGAIN} that would send the client back to the same follower.
 */
class Issue9845RedisLeaderRefusalTest {

  private static final String LEADER = "arcadedb-0:2480";

  private static ServerIsNotTheLeaderException refusal(final String leader) {
    return new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server", leader);
  }

  @Test
  void aRefusalNamingTheLeaderIsReadOnly() {
    assertThat(RedisNetworkExecutor.respErrorPrefix(refusal(LEADER))).isEqualTo("READONLY");
    assertThat(RedisNetworkExecutor.respErrorPrefix(new TransactionException("commit", refusal(LEADER)))).isEqualTo("READONLY");
  }

  @Test
  void theMessageNamesTheLeaderEvenWhenConcealed() {
    assertThat(RedisNetworkExecutor.withLeaderRedirect("refused", refusal(LEADER))).isEqualTo("refused. The leader is server '" + LEADER + "'");
    assertThat(RedisNetworkExecutor.withLeaderRedirect(ArcadeDBServer.CONCEALED_ERROR_MESSAGE, refusal(LEADER)))
        .startsWith(ArcadeDBServer.CONCEALED_ERROR_MESSAGE)
        .endsWith("The leader is server '" + LEADER + "'");
  }

  @Test
  void theAddressCannotBreakTheOneLineReply() {
    assertThat(RedisNetworkExecutor.withLeaderRedirect("refused", refusal("host\r\n-ERR forged")))
        .doesNotContain("\r").doesNotContain("\n");
  }

  @Test
  void aRefusalNamingNoLeaderStaysTryAgain() {
    assertThat(RedisNetworkExecutor.respErrorPrefix(refusal(null))).isEqualTo("TRYAGAIN");
    assertThat(RedisNetworkExecutor.withLeaderRedirect("refused", refusal(""))).isEqualTo("refused");
  }

  @Test
  void anOrdinaryConflictIsUnchanged() {
    assertThat(RedisNetworkExecutor.respErrorPrefix(new ConcurrentModificationException("retry"))).isEqualTo("TRYAGAIN");
    assertThat(RedisNetworkExecutor.withLeaderRedirect("retry", new ConcurrentModificationException("retry"))).isEqualTo("retry");
  }
}
