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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8356, the security-replication siblings of the fix in
 * {@code RaftReplicatedDatabase}: {@code RaftHAPlugin.replicateSecurityUsers/Groups/ApiTokens} all dereferenced
 * {@code raftHAServer.getTransactionBroker()} directly. {@code raftHAServer} being non-null only proves the
 * plugin has started - {@code RaftHAServer.stop()} clears its {@code transactionBroker} field separately, and
 * can do so while the plugin's own {@code raftHAServer} reference is still set, the same shutdown window
 * {@code RaftReplicatedDatabase}'s commit path raced.
 * <p>
 * Unlike the commit path, every method here already collapses any non-{@code TransactionException} into a
 * {@code TransactionException} ({@code catch (Exception e)}), so the null broker still surfaces as one - just a
 * clean, message-carrying one instead of a bare {@code NullPointerException} with no explanation.
 */
class Issue8356RaftHAPluginNullTransactionBrokerTest {

  @Test
  void replicateSecurityUsersReportsANullBrokerInsteadOfThrowingNPE() {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    final RaftHAServer raftServer = mock(RaftHAServer.class);
    when(raftServer.getTransactionBroker()).thenReturn(null);
    plugin.setRaftHAServer(raftServer);

    assertThatThrownBy(() -> plugin.replicateSecurityUsers("[]", null))
        .isInstanceOf(TransactionException.class)
        .isNotInstanceOf(NullPointerException.class)
        .cause().isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("transaction broker");
  }
}
