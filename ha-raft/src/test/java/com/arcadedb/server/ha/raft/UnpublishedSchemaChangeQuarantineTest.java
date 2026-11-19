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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A schema change that ran on its proposer but could not be published (refused, unpublishable or oversized entry) must quarantine the database and, for a compaction, hold it off.
 */
class UnpublishedSchemaChangeQuarantineTest {

  // ---- #9555 ----
  // a schema session runs on its proposer FIRST and ships afterwards. A compaction allocates the file id,
  // writes the compacted file and swaps the index's sub-indexes before {@code replicateSchema}; a DDL creates its files
  // and changes the schema before its {@code SCHEMA_ENTRY} goes out. When the leader refuses that entry before appending
  // it - the stale-session refusal of #9547, or a group commit that never dispatched it - no other node will ever hold the
  // change, and nothing marked this node as diverged: it was caught only when a later committed entry collided with the
  // file id it kept.
  // <p>
  // A DEFINITE refusal now quarantines the database on the proposer and starts its resync (and, on a leader, the
  // leadership hand-off a resync needs). An INDETERMINATE failure must not: its entry may still commit, and then the
  // local change is exactly the committed one.

  /** The reported path: the compaction ran here, its publishing entry was refused, and the node is now marked. */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aCompactionWhosePublishingEntryIsRefusedQuarantinesTheProposer() {
    final FakeRaftHAServer raft = leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new NeedRetryException("Refused a schema change on database 'issue9555' allocated in Raft term 7")));
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(raft.calls("handOffLeadershipToResync")).as("a leader cannot resync from itself, so it hands off")
        .hasSize(1);
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is still released").isNull();
  }

  /** A group commit that never dispatched the entry is just as definite as a refusal by the leader. */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aCompactionWhosePublishingEntryWasNeverDispatchedQuarantinesTheProposer() {
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new QuorumNotReachedException("Group commit timed out after 1000ms (cancelled before dispatch)"))));

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(QuorumNotReachedException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /**
   * A sealed TimeSeries store too big for one entry ships its leading slices ahead of the publishing entry (#4416). The
   * sealed file is already swapped here, so a refused slice leaves the same divergence as a refused publishing entry.
   */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aCompactionWhoseDeliveryOnlySealedSliceIsRefusedQuarantinesTheProposer() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker().fails("replicateSealedChunk",
        new NeedRetryException("Refused a schema change on database 'issue9555' allocated in Raft term 7"));
    final FakeRaftHAServer raft = leader(broker);
    raft.getConfiguration().setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, 64 * 1024);
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      replicated.recordTimeSeriesSealedChange("Sensor", 0, "Sensor_0.ts.sealed", new byte[256 * 1024]);
      return true;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(broker.calls("replicateSealedChunk")).as("the refusal came from a delivery-only slice").hasSize(1);
    assertThat(broker.calls("replicateSchema")).as("so nothing was published").isEmpty();
    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /** The followers already staged the leading slices; refusing a later one still leaves the change unpublished. */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aCompactionWhoseSealedSliceIsRefusedMidSequenceQuarantinesTheProposer() {
    final AtomicInteger slices = new AtomicInteger();
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker().on("replicateSealedChunk", args -> {
      if (slices.incrementAndGet() == 2)
        throw new NeedRetryException("Refused a schema change on database 'issue9555' allocated in Raft term 7");
      return null;
    });
    final FakeRaftHAServer raft = leader(broker);
    raft.getConfiguration().setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, 64 * 1024);
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      replicated.recordTimeSeriesSealedChange("Sensor", 0, "Sensor_0.ts.sealed", new byte[256 * 1024]);
      return true;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(broker.calls("replicateSealedChunk")).as("the first slice went out, the second was refused").hasSize(2);
    assertThat(broker.calls("replicateSchema")).isEmpty();
    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /**
   * The contract the fix is keyed on: the quarantine fires on a {@code NeedRetryException}, so the failures whose entry
   * may already be in the log must never become one.
   */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void theIndeterminateReplicationFailuresAreNotRetryableRefusals() {
    assertThat(new ReplicationDispatchedTimeoutException("dispatched")).isNotInstanceOf(NeedRetryException.class);
    assertThat(new MajorityCommittedAllFailedException("committed", null, 1L)).isNotInstanceOf(NeedRetryException.class);
  }

  /**
   * Dispatched and unanswered: since issue #11240 the compaction waits for the log to settle the entry, and one this node
   * still cannot tell committed from truncated when the wait ends is quarantined, because the swap it made here may be a
   * layout no other node holds. A committed or truncated one is covered by {@code CompactionPersistedOnlyOncePublishedTest}.
   */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aCompactionWhosePublishingEntryKeepsAnUnknownOutcomeIsQuarantined() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer();
    server.getConfiguration().setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 50L);
    final RaftReplicatedDatabase replicated = new RaftReplicatedDatabase(server, db, leader(new FakeRaftTransactionBroker()
        .fails("replicateSchema", new ReplicationDispatchedTimeoutException("Group commit timed out (entry was dispatched to Raft)"))));

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(ReplicationDispatchedTimeoutException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /** The DDL entry point: a schema change on a node whose session went stale is refused after it ran here. */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aSchemaChangeWhosePublishingEntryIsRefusedQuarantinesTheProposer() {
    final FakeRaftHAServer raft = leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new NeedRetryException("Refused a schema change on database 'issue9555' allocated in Raft term 7")));
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      replicated.getSchema().createDocumentType("Created", 1);
      return null;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
    assertThat(db.getFileManager().getRecordedChanges()).isNull();
  }

  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aSchemaChangeWhosePublishingEntryHasAnUnknownOutcomeIsNotQuarantined() {
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new ReplicationDispatchedTimeoutException("Group commit timed out (entry was dispatched to Raft)"))));

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      replicated.getSchema().createDocumentType("Created", 1);
      return null;
    })).isInstanceOf(ReplicationDispatchedTimeoutException.class);

    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  /**
   * An instalment carries pages the session already committed here (#6136), so its refusal leaves the same divergence
   * as a refused final entry, even though the exception reaches the session through the engine's commit path.
   */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aSchemaChangeWhoseInstalmentIsRefusedQuarantinesTheProposer() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker() {
      @Override
      long walChunkBudget() {
        // Every buffered commit crosses it, so the first one ships an instalment
        return 1L;
      }
    }.fails("replicateSchemaInstalment",
        new NeedRetryException("Refused a schema change on database 'issue9555' allocated in Raft term 7"));
    // Created before the database is wrapped, so the type exists without a session of its own
    db.getSchema().createDocumentType("Filled", 1);
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      replicated.transaction(() -> replicated.newDocument("Filled").set("value", 1L).save());
      return null;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(broker.calls("replicateSchemaInstalment")).as("the refusal came from an instalment").isNotEmpty();
    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /**
   * The refused instalment's files were folded into what the session considers shipped, so a callback that swallows the
   * refusal must not be allowed to publish a final entry that leaves them out: the session fails and is quarantined.
   */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aSchemaChangeThatSwallowsARefusedInstalmentIsNotPublished() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker() {
      @Override
      long walChunkBudget() {
        return 1L;
      }
    }.fails("replicateSchemaInstalment",
        new NeedRetryException("Refused a schema change on database 'issue9555' allocated in Raft term 7"));
    db.getSchema().createDocumentType("Filled", 1);
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      try {
        replicated.transaction(() -> replicated.newDocument("Filled").set("value", 1L).save());
      } catch (final RuntimeException swallowed) {
        // the callback carries on as if the instalment had gone out
      }
      return null;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(broker.calls("replicateSchemaInstalment")).isNotEmpty();
    // Only the compensating retirement may go out: no schema document and nothing to create
    assertThat(broker.calls("replicateSchema")).allSatisfy(args -> {
      assertThat(args.get(1)).isEqualTo("");
      assertThat((Map<?, ?>) args.get(2)).isEmpty();
    });
    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /** A proposer that is no longer the leader is quarantined and resynced, and has no leadership to hand off. */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aProposerThatIsNoLongerTheLeaderIsQuarantinedWithoutAHandOff() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(stateMachine).leader(false).currentTerm(7L)
        .transactionBroker(new FakeRaftTransactionBroker());
    stateMachine.setRaftHAServer(raft);

    assertThat(stateMachine.quarantineUnpublishedSchemaChange(db.getName(), "compaction",
        new NeedRetryException("Refused a schema change on database 'issue9555' proposed by another node"))).isTrue();

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();
    assertThat(stateMachine.quarantineUnpublishedSchemaChange(db.getName(), "compaction", null))
        .as("a second refusal finds the quarantine already standing").isFalse();
  }

  /** A refusal before the callback ran (the leader was not ready) changed nothing here, so there is nothing to resync. */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aSchemaChangeRefusedBeforeItRanIsNotQuarantined() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(stateMachine).leader(true).leaderReady(false)
        .currentTerm(7L).quorumTimeout(100L).transactionBroker(new FakeRaftTransactionBroker());
    stateMachine.setRaftHAServer(raft);
    final RaftReplicatedDatabase replicated = replicated(raft);
    final AtomicInteger ran = new AtomicInteger();

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      ran.incrementAndGet();
      return null;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(ran).hasValue(0);
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  /** The only voter has no peer to resync from: a quarantine there could never be lifted. */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void theOnlyVoterIsNotQuarantined() {
    final FakeRaftHAServer raft = leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new NeedRetryException("Refused a schema change on database 'issue9555' allocated in Raft term 7")))
        .returns("isSoleVoter", true);
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
    assertThat(raft.calls("handOffLeadershipToResync")).isEmpty();
  }

  /** The whole session publishes: nothing is quarantined. */
  // Regression for #9555: a schema session runs on its proposer FIRST and ships afterwards.
  @Test
  void aCompactionThatIsPublishedIsNotQuarantined() throws Exception {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThat(replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isTrue();

    assertThat(broker.calls("replicateSchema")).hasSize(1);
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

// ---- #9558 ----
  // the follow-up of #9555: a schema session that changed its proposer locally and then failed with
  // anything but a retryable refusal still left the proposer diverged. Decided per failure class:
  // <ul>
  //   <li>a DETERMINISTIC failure after the change ran - an entry too large to replicate, a sealed plan that cannot fit -
  //       quarantines the proposer like a refusal does, and a COMPACTION is then held off on that node for a growing
  //       window, so the resync followed by the same compaction on the next schedule cannot loop;</li>
  //   <li>a callback that fails on its own after a commit it made was applied here and never shipped quarantines the
  //       proposer, while one that left nothing behind does not;</li>
  //   <li>an indeterminate failure, even wrapped by the engine, never quarantines.</li>
  // </ul>

  @TempDir
  Path tempDir;

  private final AtomicLong clock = new AtomicLong(1_000_000L);

  private LocalDatabase      db;
  private ArcadeStateMachine stateMachine;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue9558").toString()).create();
    stateMachine = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db;
      }

      @Override
      ClientId localClientId() {
        return null;
      }

      @Override
      long currentRaftTerm() {
        return 7L;
      }
    };
    stateMachine.compactionBackOffClock = clock::get;
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Compaction (runWithCompactionReplication)
  // ---------------------------------------------------------------------------------------------------------------

  /** The splitter cannot fit the publishing entry: the compaction ran here, so the node is quarantined and held off. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aCompactionWhosePublishingEntryIsTooLargeIsQuarantinedAndHeldOff() throws Exception {
    final FakeRaftHAServer raft = leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new ReplicatedEntryTooLargeException("Schema change for database 'issue9558' cannot be split")));
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(ReplicatedEntryTooLargeException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isTrue();
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is still released").isNull();
  }

  /**
   * The session-wide sealed plan is refused before any slice ships (#6933) - but after the compaction swapped the
   * sealed store here, so the change is just as unpublished.
   */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aCompactionWhoseSealedPlanCannotFitIsQuarantinedAndHeldOff() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker() {
      @Override
      long maxEntrySize() {
        // Too small for even one slice per store once the schema JSON is on the publishing entry
        return 2_000L;
      }
    };
    final FakeRaftHAServer raft = leader(broker);
    raft.getConfiguration().setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, 64 * 1024);
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      replicated.recordTimeSeriesSealedChange("Sensor", 0, "Sensor_0.ts.sealed", new byte[256 * 1024]);
      return true;
    })).isInstanceOf(ReplicatedEntryTooLargeException.class);

    assertThat(broker.calls("replicateSealedChunk")).as("refused before anything shipped").isEmpty();
    assertThat(broker.calls("replicateSchema")).isEmpty();
    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isTrue();
  }

  /** A delivery-only slice the transport cannot carry is as deterministic as a publishing entry that cannot. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aCompactionWhoseSealedSliceIsTooLargeIsQuarantinedAndHeldOff() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker().fails("replicateSealedChunk",
        new ReplicatedEntryTooLargeException("Sealed TimeSeries store slice is above the maximum replicated entry size"));
    final FakeRaftHAServer raft = leader(broker);
    raft.getConfiguration().setValue(GlobalConfiguration.HA_TS_MAX_SEALED_INLINE_SIZE, 64 * 1024);
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      replicated.recordTimeSeriesSealedChange("Sensor", 0, "Sensor_0.ts.sealed", new byte[256 * 1024]);
      return true;
    })).isInstanceOf(ReplicatedEntryTooLargeException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isTrue();
  }

  /**
   * The point of the back-off: after the resync, the next schedule does not run the same compaction again while the
   * window holds, and runs it once the window is over.
   */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aHeldOffCompactionIsDeferredUntilTheWindowEnds() throws Exception {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker().fails("replicateSchema",
        new ReplicatedEntryTooLargeException("Schema change for database 'issue9558' cannot be split"));
    final RaftReplicatedDatabase replicated = replicated(leader(broker));
    final AtomicInteger ran = new AtomicInteger();

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      ran.incrementAndGet();
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(ReplicatedEntryTooLargeException.class);
    // The resync would lift the quarantine; the back-off has to outlive it
    stateMachine.clearDivergedDatabase(db.getName());

    assertThat(replicated.runWithCompactionReplication(() -> {
      ran.incrementAndGet();
      return true;
    })).as("deferred inside the window").isFalse();
    assertThat(ran).hasValue(1);
    assertThat(db.getFileManager().getRecordedChanges()).as("no recording session was claimed").isNull();

    clock.addAndGet(ArcadeStateMachine.COMPACTION_BACK_OFF_BASE_MS);
    broker.on("replicateSchema", args -> null);
    assertThat(replicated.runWithCompactionReplication(() -> {
      ran.incrementAndGet();
      db.getSchema().createDocumentType("CompactedAgain", 1);
      return true;
    })).as("runs again once the window is over").isTrue();
    assertThat(ran).hasValue(2);
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isFalse();
  }

  /** Each failure in a row doubles the window, up to its cap; a published compaction forgets the streak. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void theWindowDoublesWithEveryFailureInARowAndAPublishedCompactionResetsIt() throws Exception {
    final String name = db.getName();
    assertThat(stateMachine.deferCompactionAfterUnpublishableChange(name))
        .isEqualTo(ArcadeStateMachine.COMPACTION_BACK_OFF_BASE_MS);
    assertThat(stateMachine.deferCompactionAfterUnpublishableChange(name))
        .isEqualTo(2 * ArcadeStateMachine.COMPACTION_BACK_OFF_BASE_MS);
    for (int i = 0; i < 40; i++)
      stateMachine.deferCompactionAfterUnpublishableChange(name);
    assertThat(stateMachine.deferCompactionAfterUnpublishableChange(name))
        .isEqualTo(ArcadeStateMachine.COMPACTION_BACK_OFF_MAX_MS);

    clock.addAndGet(ArcadeStateMachine.COMPACTION_BACK_OFF_MAX_MS);
    assertThat(stateMachine.isCompactionHeldOff(name)).isFalse();
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker()));
    assertThat(replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isTrue();

    assertThat(stateMachine.deferCompactionAfterUnpublishableChange(name))
        .as("the streak starts over").isEqualTo(ArcadeStateMachine.COMPACTION_BACK_OFF_BASE_MS);
  }

  /** A database dropped while held off takes the hold-off with it: one recreated under the name starts clean. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void droppingADatabaseForgetsItsCompactionHoldOff() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.compactionBackOffClock = clock::get;
    sm.setServer(FakeArcadeDBServer.create((String) null, new ContextConfiguration()));
    sm.deferCompactionAfterUnpublishableChange("dropped9558");
    sm.deferCompactionAfterUnpublishableChange("other9558");

    sm.applyDropDatabaseEntry(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry("dropped9558")), 1L);

    assertThat(sm.isCompactionHeldOff("dropped9558")).isFalse();
    assertThat(sm.deferCompactionAfterUnpublishableChange("dropped9558")).as("its streak starts over")
        .isEqualTo(ArcadeStateMachine.COMPACTION_BACK_OFF_BASE_MS);
    assertThat(sm.isCompactionHeldOff("other9558")).as("an unrelated database keeps its hold-off").isTrue();
  }

  /**
   * The broker looked up AFTER the compaction ran (#9555 only guarded the submits): gone, it leaves the change
   * unpublished, but it is transient, so the node is quarantined and NOT held off.
   */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aCompactionWhoseBrokerIsGoneAfterItRanIsQuarantinedButNotHeldOff() {
    final FakeRaftHAServer raft = leader(new FakeRaftTransactionBroker());
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      raft.transactionBroker(null);
      return true;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isFalse();
  }

  /** A retryable refusal is transient by construction: quarantined as in #9555, never held off. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aCompactionRefusedRetryablyIsNotHeldOff() {
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new NeedRetryException("Refused a schema change on database 'issue9558' allocated in Raft term 7"))));

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isFalse();
  }

  /** The only voter is not quarantined (no peer to resync from), but the next schedule would hit the same wall. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void theOnlyVoterIsHeldOffWithoutAQuarantine() {
    final FakeRaftHAServer raft = leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new ReplicatedEntryTooLargeException("Schema change for database 'issue9558' cannot be split")))
        .returns("isSoleVoter", true);
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(ReplicatedEntryTooLargeException.class);

    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isTrue();
  }

  /** A compaction that fails on its own having committed nothing left nothing behind. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aCompactionThatFailsBeforeCommittingAnythingIsNotQuarantined() {
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker()));

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      throw new IllegalStateException("compaction failed on its own");
    })).isInstanceOf(IllegalStateException.class);

    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isFalse();
  }

  /**
   * A compaction that fails on its own AFTER a commit it made was applied here (the TimeSeries mutable-bucket clear
   * rides the session's buffer): those pages are on this node only.
   */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aCompactionThatFailsAfterACommitAppliedHereIsQuarantined() {
    db.getSchema().createDocumentType("Filled", 1);
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker()));

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      replicated.transaction(() -> replicated.newDocument("Filled").set("value", 1L).save());
      throw new IllegalStateException("compaction failed on its own");
    })).isInstanceOf(IllegalStateException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /**
   * An interrupt (shutdown, a cancelled scheduler) after a commit was applied here leaves those pages just as
   * unpublished, so the node is quarantined - but it says nothing about the next run, so compactions are NOT held off
   * (code review on PR #9586).
   */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aCompactionInterruptedAfterACommitAppliedHereIsQuarantinedButNotHeldOff() {
    db.getSchema().createDocumentType("Filled", 1);
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker()));

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      replicated.transaction(() -> replicated.newDocument("Filled").set("value", 1L).save());
      throw new InterruptedException("scheduler cancelled");
    })).isInstanceOf(InterruptedException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isFalse();
  }

  /** A failure that is not provably deterministic - an error of the transport, an I/O error - is not held off either. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aCompactionWhosePublishingEntryFailsForANonDeterministicReasonIsNotHeldOff() {
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new IllegalStateException("transport failed"))));

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(IllegalStateException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).isFalse();
  }

  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void onlyAnEntryTooLargeIsADeterministicFailure() {
    assertThat(RaftReplicatedDatabase.isDeterministicPublishFailure(new TransactionException("wrapped",
        new ReplicatedEntryTooLargeException("too large")))).isTrue();
    assertThat(RaftReplicatedDatabase.isDeterministicPublishFailure(new NeedRetryException("refused"))).isFalse();
    assertThat(RaftReplicatedDatabase.isDeterministicPublishFailure(new InterruptedException("stop"))).isFalse();
    assertThat(RaftReplicatedDatabase.isDeterministicPublishFailure(new OutOfMemoryError("heap"))).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // DDL (recordFileChanges)
  // ---------------------------------------------------------------------------------------------------------------

  /** The final entry cannot be split: the DDL ran here and is not published. Quarantined; a DDL is never held off. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aSchemaChangeWhosePublishingEntryIsTooLargeIsQuarantined() {
    final FakeRaftHAServer raft = leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new ReplicatedEntryTooLargeException("Schema change for database 'issue9558' cannot be split")));
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      replicated.getSchema().createDocumentType("Created", 1);
      return null;
    })).isInstanceOf(ReplicatedEntryTooLargeException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
    assertThat(raft.calls("handOffLeadershipToResync")).hasSize(1);
    assertThat(stateMachine.isCompactionHeldOff(db.getName())).as("a DDL holds no compaction off").isFalse();
    assertThat(db.getFileManager().getRecordedChanges()).isNull();
  }

  /** The broker gone when the final entry is about to go out: the DDL is complete here and unpublished. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aSchemaChangeWhoseBrokerIsGoneBeforeTheFinalEntryIsQuarantined() {
    final FakeRaftHAServer raft = leader(new FakeRaftTransactionBroker());
    final RaftReplicatedDatabase replicated = replicated(raft);

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      replicated.getSchema().createDocumentType("Created", 1);
      raft.transactionBroker(null);
      return null;
    })).isInstanceOf(NeedRetryException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /**
   * An instalment too large to split, swallowed by the callback: its files were already folded into what the session
   * considers shipped, so the final entry must not go out, and the node is quarantined.
   */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aSchemaChangeThatSwallowsAnInstalmentTooLargeIsNotPublishedAndIsQuarantined() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker() {
      @Override
      long walChunkBudget() {
        return 1L;
      }
    }.fails("replicateSchemaInstalment",
        new ReplicatedEntryTooLargeException("Schema change chunk 1/1 contains a single indivisible WAL entry"));
    db.getSchema().createDocumentType("Filled", 1);
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      try {
        replicated.transaction(() -> replicated.newDocument("Filled").set("value", 1L).save());
      } catch (final RuntimeException swallowed) {
        // the callback carries on as if the instalment had gone out
      }
      return null;
    })).isInstanceOf(ReplicatedEntryTooLargeException.class);

    assertThat(broker.calls("replicateSchema")).allSatisfy(args -> assertThat(args.get(1)).isEqualTo(""));
    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /** The callback fails on its own after committing into a type that already existed: those pages are here only. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aSchemaChangeWhoseCallbackFailsAfterACommitAppliedHereIsQuarantined() {
    db.getSchema().createDocumentType("Filled", 1);
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      replicated.transaction(() -> replicated.newDocument("Filled").set("value", 1L).save());
      throw new IllegalStateException("the DDL failed on its own");
    })).isInstanceOf(IllegalStateException.class);

    assertThat(broker.calls("replicateSchema")).as("nothing was published").isEmpty();
    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /**
   * The case the issue names: an instalment shipped, then the callback committed again and failed. The shipped pages
   * reached the followers; the ones still buffered did not, and this node keeps them.
   */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aSchemaChangeWhoseCallbackFailsAfterAnInstalmentShippedIsQuarantined() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker() {
      @Override
      long walChunkBudget() {
        return 32 * 1024L;
      }
    };
    db.getSchema().createDocumentType("Filled", 1);
    final RaftReplicatedDatabase replicated = replicated(leader(broker));
    final String padding = "x".repeat(200);

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      // Well past the threshold: ships as an instalment
      replicated.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          replicated.newDocument("Filled").set("value", i).set("padding", padding).save();
      });
      // Well below it: stays in the buffer
      replicated.transaction(() -> replicated.newDocument("Filled").set("value", -1).save());
      throw new IllegalStateException("the DDL failed on its own");
    })).isInstanceOf(IllegalStateException.class);

    assertThat(broker.calls("replicateSchemaInstalment")).as("an instalment shipped first").hasSize(1);
    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  /** Every commit shipped before the callback failed: the followers hold what this node holds, so no quarantine. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aSchemaChangeWhoseCallbackFailsWithEveryCommitShippedIsNotQuarantined() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker() {
      @Override
      long walChunkBudget() {
        return 1L;
      }
    };
    db.getSchema().createDocumentType("Filled", 1);
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      replicated.transaction(() -> replicated.newDocument("Filled").set("value", 1L).save());
      throw new IllegalStateException("the DDL failed on its own");
    })).isInstanceOf(IllegalStateException.class);

    assertThat(broker.calls("replicateSchemaInstalment")).isNotEmpty();
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  /** A refused DDL that never committed anything - the common case - leaves nothing behind and is not quarantined. */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aSchemaChangeWhoseCallbackFailsBeforeCommittingAnythingIsNotQuarantined() {
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker()));

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      throw new IllegalArgumentException("Type 'Created' already exists");
    })).isInstanceOf(IllegalArgumentException.class);

    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  /**
   * An instalment whose outcome is unknown may still commit and carry its pages to every node: the session that then
   * fails is not quarantined, even when the engine wraps the instalment's exception on its way out.
   */
  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void aSchemaChangeWhoseInstalmentHasAnUnknownOutcomeIsNotQuarantined() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker() {
      @Override
      long walChunkBudget() {
        return 1L;
      }
    }.fails("replicateSchemaInstalment",
        new ReplicationDispatchedTimeoutException("Group commit timed out (entry was dispatched to Raft)"));
    db.getSchema().createDocumentType("Filled", 1);
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      try {
        replicated.transaction(() -> replicated.newDocument("Filled").set("value", 1L).save());
      } catch (final RuntimeException e) {
        throw new TransactionException("index build failed", e);
      }
      return null;
    })).isInstanceOf(TransactionException.class);

    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  // Regression for #9558: the follow-up of #9555: a schema session that changed its proposer locally and then failed with anything but a retryable refusal still left the pro...
  @Test
  void anIndeterminateFailureIsRecognisedThroughTheCauseChain() {
    assertThat(RaftReplicatedDatabase.isIndeterminatePublishFailure(new TransactionException("wrapped",
        new ReplicationDispatchedTimeoutException("dispatched")))).isTrue();
    assertThat(RaftReplicatedDatabase.isIndeterminatePublishFailure(
        new MajorityCommittedAllFailedException("committed", null, 1L))).isTrue();
    assertThat(RaftReplicatedDatabase.isIndeterminatePublishFailure(
        new ReplicatedEntryTooLargeException("too large"))).isFalse();
    assertThat(RaftReplicatedDatabase.isIndeterminatePublishFailure(new NeedRetryException("refused"))).isFalse();
  }

  private FakeRaftHAServer leader(final FakeRaftTransactionBroker broker) {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(stateMachine).leader(true).currentTerm(7L)
        .transactionBroker(broker);
    stateMachine.setRaftHAServer(raft);
    return raft;
  }

  private RaftReplicatedDatabase replicated(final FakeRaftHAServer raft) {
    return new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), db, raft);
  }
}
