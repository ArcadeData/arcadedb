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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.server.ha.raft.ratis.RatisRefusedEntryErrorFilter;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12365: a client was answered a {@link ConcurrentModificationException} for a transaction every node applied. The
 * submitter tells "the leader refused the entry before appending it" (a definite no, so the commit is withdrawn) from any other
 * failure by a {@link StateMachineException} whose direct cause is a retryable exception, and Ratis wraps a failure of
 * {@code applyTransaction} - an entry already in the log - in the same exception. A retryable exception escaping the apply of a
 * committed entry therefore read as a refusal. The apply now carries it one level down, where the refusal test does not look.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12365AppliedEntryFailureTest {
  private static final RaftGroupMemberId MEMBER = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("peer-0"), RaftGroupId.randomId());

  @Test
  void aRetryableFailureOfACommittedEntryIsNotReadAsARefusalBeforeTheAppend() {
    final ConcurrentModificationException conflict = new ConcurrentModificationException("Concurrent modification on page 3/0");

    // what Ratis hands the submitter for a failed apply: the state machine's exception wrapped in a StateMachineException
    final Exception applied = ArcadeStateMachine.appliedEntryFailure(42L, conflict);
    assertThat(applied).isInstanceOf(ReplicationException.class).hasCause(conflict).hasMessageContaining("index 42");
    assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(new CompletionException(new StateMachineException(MEMBER, applied))))
        .as("an entry that is in the log is never answered as refused").isNull();

    // without the guard the same failure was the refusal shape
    assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(new CompletionException(new StateMachineException(MEMBER, conflict))))
        .isSameAs(conflict);
  }

  @Test
  void theRefusalBeforeTheAppendIsStillADefiniteRefusal() {
    final ConcurrentModificationException conflict = new ConcurrentModificationException("refused before the append");
    assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(
        new CompletionException(new StateMachineException(conflict.getMessage(), conflict, false)))).isSameAs(conflict);
  }

  @Test
  void anyOtherFailureIsCarriedAsItIs() {
    final IOException io = new IOException("disk");
    final ReplicationException replication = new ReplicationException("resync required");
    final IllegalStateException state = new IllegalStateException("bug");
    assertThat(ArcadeStateMachine.appliedEntryFailure(1L, io)).isSameAs(io);
    assertThat(ArcadeStateMachine.appliedEntryFailure(1L, replication)).isSameAs(replication);
    assertThat(ArcadeStateMachine.appliedEntryFailure(1L, state)).isSameAs(state);
    final OutOfMemoryError oom = new OutOfMemoryError("heap");
    assertThat(ArcadeStateMachine.appliedEntryFailure(1L, oom)).isInstanceOf(RuntimeException.class).hasCause(oom);
  }
}
