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
package com.arcadedb.server.ha.raft.ratis;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Ratis client's SEVERE for an entry the leader refused before appending it (issue #6965, and since issue #11951 the
 * refusal of a leader stepping down for a leadership transfer) is dropped; every other failure of the same logger stays visible.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RatisRefusedEntryErrorFilterTest {

  @Test
  void dropsOnlyTheRefusedEntryRecord() {
    final RatisRefusedEntryErrorFilter filter = new RatisRefusedEntryErrorFilter(null);

    final LogRecord refused = new LogRecord(Level.SEVERE, "Failed to send request, message=Message:0100(size=97)");
    refused.setThrown(new CompletionException(new StateMachineException("conflict",
        new ConcurrentModificationException("Concurrent modification on page 3/0"), false)));
    assertThat(filter.isLoggable(refused)).as("a pre-append refusal is an ordinary retryable conflict").isFalse();

    final LogRecord otherStateMachineFailure = new LogRecord(Level.SEVERE, "Failed to send request");
    otherStateMachineFailure.setThrown(new CompletionException(new StateMachineException("boom", new IOException("disk"), true)));
    assertThat(filter.isLoggable(otherStateMachineFailure)).as("a genuine state machine failure stays visible").isTrue();

    final LogRecord transportFailure = new LogRecord(Level.SEVERE, "Failed to send request");
    transportFailure.setThrown(new CompletionException(new IOException("connection reset")));
    assertThat(filter.isLoggable(transportFailure)).isTrue();

    assertThat(filter.isLoggable(new LogRecord(Level.SEVERE, "Failed to send request"))).as("no throwable at all").isTrue();
  }

  /**
   * Issue #11951: Ratis raises {@link LeaderSteppingDownException} in one place only, {@code RaftServerImpl.checkLeaderState},
   * which runs before the entry is appended. The leader refused the write while a leadership transfer was pending: a definite,
   * retryable outcome, recognised however the Ratis client delivers it - as itself, as the {@link CompletionException}
   * {@code OrderedAsync} completes the send future with, and as the {@link ExecutionException} {@code get()} rethrows.
   */
  @Test
  void aLeaderSteppingDownRefusalIsARetryableRefusalBeforeAppend() {
    final LeaderSteppingDownException steppingDown = new LeaderSteppingDownException("s1@group-ABC is stepping down");
    for (final Throwable delivered : List.of(steppingDown, new CompletionException(steppingDown),
        new ExecutionException(new CompletionException(steppingDown)))) {
      final NeedRetryException refused = RatisRefusedEntryErrorFilter.refusedBeforeAppend(delivered);
      assertThat(refused).as("delivered as %s", delivered.getClass().getSimpleName()).isNotNull();
      assertThat(refused.getMessage()).contains("stepping down").contains("s1@group-ABC");
    }

    final LogRecord record = new LogRecord(Level.SEVERE, "Failed to send request, message=Message:0100(size=97)");
    record.setThrown(new CompletionException(steppingDown));
    assertThat(new RatisRefusedEntryErrorFilter(null).isLoggable(record))
        .as("a write refused during a planned leadership transfer is not a send failure").isFalse();
  }

  /**
   * The neighbours of the stepping-down refusal stay what they were: a reply the leader lost, a client that was closed, a
   * request that timed out and a failed transfer say nothing about whether an entry reached the log, and a
   * {@code NotLeaderException} is also the answer a deposed leader gives the entries it had ALREADY appended. Matched by type,
   * never by message text.
   */
  @Test
  void otherRaftFailuresAreNotRefusalsBeforeAppend() {
    final RaftGroupMemberId member = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("s1"), RaftGroupId.randomId());
    for (final Throwable failure : List.<Throwable>of(new NotLeaderException(member, null, List.of()),
        new LeaderNotReadyException(member), new AlreadyClosedException("client-1->RAFT is closed"),
        new TimeoutIOException("request #7 timeout 10s"), new TransferLeadershipException("s1 failed to transfer"),
        new RaftException("s1@group-ABC is stepping down"), new IOException("connection reset"))) {
      assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(failure)).as("%s", failure).isNull();
      assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(new CompletionException(failure))).as("%s", failure).isNull();
    }
  }

  /** A cyclic cause chain must neither hang the walk nor hide a refusal buried in it. */
  @Test
  void aCyclicCauseChainIsWalkedToTheBound() {
    final RuntimeException a = new RuntimeException("a");
    final RuntimeException b = new RuntimeException("b", a);
    a.initCause(b);
    assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(a)).isNull();

    final ConcurrentModificationException conflict = new ConcurrentModificationException("Concurrent modification on page 3/0");
    final StateMachineException refusal = new StateMachineException("conflict", conflict, false);
    final RuntimeException c = new RuntimeException("c", refusal);
    conflict.initCause(c);
    assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(c)).isSameAs(conflict);
  }

  @Test
  void chainsToTheFilterTheLoggerAlreadyCarried() {
    final Filter rejectAll = record -> false;
    final RatisRefusedEntryErrorFilter filter = new RatisRefusedEntryErrorFilter(rejectAll);
    assertThat(filter.isLoggable(new LogRecord(Level.SEVERE, "anything"))).isFalse();
    assertThat(filter.getDelegate()).isSameAs(rejectAll);
  }

  /** The filter is keyed to a Ratis-internal class name: a Ratis bump that renames it must turn this red. */
  @Test
  void theLoggerNameIsAClassOfTheShippedRatis() throws ClassNotFoundException {
    assertThat(Class.forName(RatisRefusedEntryErrorFilter.RATIS_ORDERED_ASYNC_LOGGER)).isNotNull();
  }

  @Test
  void installIsIdempotent() {
    RatisRefusedEntryErrorFilter.install();
    final Logger logger = Logger.getLogger(RatisRefusedEntryErrorFilter.RATIS_ORDERED_ASYNC_LOGGER);
    final Filter installed = logger.getFilter();
    assertThat(installed).isInstanceOf(RatisRefusedEntryErrorFilter.class);

    RatisRefusedEntryErrorFilter.install();
    assertThat(logger.getFilter()).as("a second install must not stack filters").isSameAs(installed);
  }
}
