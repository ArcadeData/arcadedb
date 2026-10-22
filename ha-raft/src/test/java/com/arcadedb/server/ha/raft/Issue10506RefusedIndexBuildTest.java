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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10506: on a Raft leader a {@code CREATE INDEX ... UNIQUE} refused for a duplicate key AFTER at least one batch of the build
 * was committed ended its schema session with the build's pages buffered and the index files still on disk, because the clean-up
 * of the failed build ran in the catch arm of {@code TypeIndexBuilder.create()}, after the session had ended. The session read that
 * as a change left on this node only and quarantined the database (UNPUBLISHED_SCHEMA_CHANGE), so a refused DDL cost a resync of
 * the leader's whole database.
 * <p>
 * The clean-up now runs inside the session: what the build created is gone by the time the session decides, and nothing is
 * quarantined. A refused REPLACING build is the exception, and stays quarantined on purpose: it dropped the index it replaces
 * inside the session, a file every other node still holds.
 */
class Issue10506RefusedIndexBuildTest {
  private static final int RECORDS = 500;

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private ArcadeStateMachine stateMachine;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue10506").toString()).create();
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

  /** The duplicate is met by the commit of a batch after the first: the batches before it were committed and buffered. */
  @Test
  void aBuildRefusedByALaterBatchIsCleanedUpInsideTheSessionAndNotQuarantined() {
    populateWithADuplicateAtTheEnd(1);
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));
    final Set<Integer> filesBefore = fileIds();

    final TypeIndexBuilder builder = replicated.getSchema().buildTypeIndex("Work", new String[] { "id" });
    builder.withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withBatchSize(50);

    assertThatThrownBy(builder::create).satisfiesAnyOf(
        e -> assertThat(e).isInstanceOf(DuplicatedKeyException.class),
        e -> assertThat(e).isInstanceOf(IndexException.class).hasRootCauseInstanceOf(DuplicatedKeyException.class));

    assertRefusedWithNothingLeftBehind(broker, filesBefore);
  }

  /** The duplicate is found by the pass over the finished index (issue #10349), after every batch of every bucket was committed. */
  @Test
  void aBuildRefusedByTheDeferredUniquenessCheckIsCleanedUpInsideTheSessionAndNotQuarantined() {
    db.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 100L);
    populateWithADuplicateAtTheEnd(3);
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));
    final Set<Integer> filesBefore = fileIds();

    final TypeIndexBuilder builder = replicated.getSchema().buildTypeIndex("Work", new String[] { "id" });
    builder.withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true);

    assertThatThrownBy(builder::create).isInstanceOf(IndexException.class).hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertRefusedWithNothingLeftBehind(broker, filesBefore);
  }

  /**
   * The same refusal when the build REPLACES an index (withReplaceIfIncompatible): the replaced index was dropped inside the
   * session, so its file is gone here and still on every other node. That change is unpublished, whatever the clean-up of the
   * new index does, and the node must still be quarantined. Making this refusal cost no resync either is issue #10569.
   */
  @Test
  void aRefusedReplacingBuildDroppedAnIndexOtherNodesStillHoldAndIsQuarantined() {
    populateWithADuplicateAtTheEnd(1);
    db.getSchema().getType("Work").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    final TypeIndexBuilder builder = replicated.getSchema().buildTypeIndex("Work", new String[] { "id" });
    builder.withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withReplaceIfIncompatible(true).withBatchSize(50);

    assertThatThrownBy(builder::create).isInstanceOf(RuntimeException.class);

    assertThat(stateMachine.quarantineCause(db.getName())).isEqualTo(DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE);
  }

  private void assertRefusedWithNothingLeftBehind(final FakeRaftTransactionBroker broker, final Set<Integer> filesBefore) {
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).as("a refused CREATE INDEX does not quarantine the leader").isFalse();
    assertThat(stateMachine.quarantineCause(db.getName())).isNull();
    assertThat(broker.calls("replicateSchema")).as("nothing was published").isEmpty();
    assertThat(db.getSchema().getType("Work").getIndexByProperties("id")).as("the index is gone").isNull();
    assertThat(fileIds()).as("no file of the failed build is left").isEqualTo(filesBefore);
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is released").isNull();

    // The type still takes writes, including the duplicate key the index refused
    db.transaction(() -> db.newDocument("Work").set("id", "k0").save());
  }

  private Set<Integer> fileIds() {
    final Set<Integer> ids = new HashSet<>();
    for (final var file : db.getFileManager().getFiles())
      if (file != null)
        ids.add(file.getFileId());
    return ids;
  }

  /**
   * {@code RECORDS} distinct keys, then one more record holding the first key again: in the same bucket when the type has one, in
   * another bucket otherwise, where only the pass over the finished index meets it (the build of a bucket refuses a duplicate
   * of its own before it commits anything).
   */
  private void populateWithADuplicateAtTheEnd(final int buckets) {
    final DocumentType type = db.getSchema().buildDocumentType().withName("Work").withTotalBuckets(buckets).create();
    type.createProperty("id", Type.STRING);
    db.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        db.newDocument("Work").set("id", "k" + i).save();
    });
    final int holder = db.query("sql", "SELECT FROM Work WHERE id = 'k0'").nextIfAvailable().getIdentity().get().getBucketId();
    String twinBucket = null;
    for (final Bucket bucket : type.getBuckets(false))
      if (buckets == 1 || bucket.getFileId() != holder)
        twinBucket = bucket.getName();
    final String target = twinBucket;
    db.transaction(() -> db.newDocument("Work").set("id", "k0").save(target));
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
