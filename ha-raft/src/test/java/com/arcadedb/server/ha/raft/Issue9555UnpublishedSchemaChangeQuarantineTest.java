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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9555: a schema session runs on its proposer FIRST and ships afterwards. A compaction allocates the file id,
 * writes the compacted file and swaps the index's sub-indexes before {@code replicateSchema}; a DDL creates its files
 * and changes the schema before its {@code SCHEMA_ENTRY} goes out. When the leader refuses that entry before appending
 * it - the stale-session refusal of #9547, or a group commit that never dispatched it - no other node will ever hold the
 * change, and nothing marked this node as diverged: it was caught only when a later committed entry collided with the
 * file id it kept.
 * <p>
 * A DEFINITE refusal now quarantines the database on the proposer and starts its resync (and, on a leader, the
 * leadership hand-off a resync needs). An INDETERMINATE failure must not: its entry may still commit, and then the
 * local change is exactly the committed one.
 */
class Issue9555UnpublishedSchemaChangeQuarantineTest {
  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private ArcadeStateMachine stateMachine;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue9555").toString()).create();
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
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  /** The reported path: the compaction ran here, its publishing entry was refused, and the node is now marked. */
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

  /** Dispatched and unanswered: the entry may still commit, so the local compaction may be the committed one. */
  @Test
  void aCompactionWhosePublishingEntryHasAnUnknownOutcomeIsNotQuarantined() {
    final RaftReplicatedDatabase replicated = replicated(leader(new FakeRaftTransactionBroker().fails("replicateSchema",
        new ReplicationDispatchedTimeoutException("Group commit timed out (entry was dispatched to Raft)"))));

    assertThatThrownBy(() -> replicated.runWithCompactionReplication(() -> {
      db.getSchema().createDocumentType("Compacted", 1);
      return true;
    })).isInstanceOf(ReplicationDispatchedTimeoutException.class);

    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  /** The DDL entry point: a schema change on a node whose session went stale is refused after it ran here. */
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
