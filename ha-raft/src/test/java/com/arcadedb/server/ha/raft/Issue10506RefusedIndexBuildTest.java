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
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
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
import java.util.List;
import java.util.Map;
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
 * quarantined. A refused REPLACING build dropped the index it replaces inside the session, a file every other node still holds:
 * since issue #10569 it builds that index again inside the same session and publishes the net state instead of quarantining.
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
   * session, a file every other node still holds. Issue #10569: the replaced index is now built again inside the same session,
   * which publishes the net state - the replaced files removed, the restored ones added - and costs the caller only an error.
   */
  @Test
  void aRefusedReplacingBuildRestoresTheReplacedIndexInsideTheSessionAndPublishesIt() {
    populateWithADuplicateAtTheEnd(1);
    db.getSchema().getType("Work").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));
    final Set<Integer> replacedFiles = indexFileIds();
    final Set<Integer> filesBefore = fileIds();

    final TypeIndexBuilder builder = replicated.getSchema().buildTypeIndex("Work", new String[] { "id" });
    builder.withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withReplaceIfIncompatible(true).withBatchSize(50);

    assertThatThrownBy(builder::create).satisfiesAnyOf(
        e -> assertThat(e).isInstanceOf(DuplicatedKeyException.class),
        e -> assertThat(e).isInstanceOf(IndexException.class).hasRootCauseInstanceOf(DuplicatedKeyException.class));

    assertReplacedIndexRestoredAndPublished(broker, replacedFiles, filesBefore);
  }

  /** As above, refused by the pass over the finished index (issue #10349) after every bucket was built. */
  @Test
  void aReplacingBuildRefusedByTheDeferredUniquenessCheckRestoresTheReplacedIndexInsideTheSession() {
    db.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 100L);
    populateWithADuplicateAtTheEnd(3);
    db.getSchema().getType("Work").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));
    final Set<Integer> replacedFiles = indexFileIds();
    final Set<Integer> filesBefore = fileIds();

    final TypeIndexBuilder builder = replicated.getSchema().buildTypeIndex("Work", new String[] { "id" });
    builder.withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withReplaceIfIncompatible(true);

    assertThatThrownBy(builder::create).isInstanceOf(IndexException.class).hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertReplacedIndexRestoredAndPublished(broker, replacedFiles, filesBefore);
  }

  /** The replacing build users actually reach: a Cypher UNIQUE constraint over a plain index, on data that violates it. */
  @Test
  void aRefusedCypherUniqueConstraintOverAPlainIndexRestoresThePlainIndexInsideTheSession() {
    final DocumentType type = db.getSchema().buildVertexType().withName("Work").withTotalBuckets(1).create();
    type.createProperty("id", Type.STRING);
    db.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        db.newVertex("Work").set("id", "k" + i).save();
      db.newVertex("Work").set("id", "k0").save();
    });
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));
    final Set<Integer> replacedFiles = indexFileIds();
    final Set<Integer> filesBefore = fileIds();

    assertThatThrownBy(() -> replicated.command("opencypher", "CREATE CONSTRAINT FOR (n:Work) REQUIRE n.id IS UNIQUE"))
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertReplacedIndexRestoredAndPublished(broker, replacedFiles, filesBefore);
  }

  @SuppressWarnings("unchecked")
  private void assertReplacedIndexRestoredAndPublished(final FakeRaftTransactionBroker broker, final Set<Integer> replacedFiles,
      final Set<Integer> filesBefore) {
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).as("a refused replacing CREATE INDEX does not quarantine the leader")
        .isFalse();
    assertThat(stateMachine.quarantineCause(db.getName())).isNull();
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is released").isNull();

    // The replaced definition is back, complete: one entry per record, the duplicate key included
    final TypeIndex restored = db.getSchema().getType("Work").getIndexByProperties("id");
    assertThat(restored).as("the replaced index is restored").isNotNull();
    assertThat(restored.isUnique()).isFalse();
    assertThat(restored.countEntries()).isEqualTo(RECORDS + 1L);

    final Set<Integer> restoredFiles = indexFileIds();
    final Set<Integer> expectedFiles = new HashSet<>(filesBefore);
    expectedFiles.removeAll(replacedFiles);
    expectedFiles.addAll(restoredFiles);
    assertThat(fileIds()).as("no file of the failed build is left").isEqualTo(expectedFiles);

    // ONE schema entry publishes the net state: the replaced files removed, the restored ones added
    final List<List<Object>> published = broker.calls("replicateSchema");
    assertThat(published).hasSize(1);
    final List<Object> entry = published.getFirst();
    assertThat(((Map<Integer, String>) entry.get(3)).keySet()).as("filesToRemove").isEqualTo(replacedFiles);
    assertThat(((Map<Integer, String>) entry.get(2)).keySet()).as("filesToAdd").isEqualTo(restoredFiles);

    // ... and none of its WAL names a file of the failed build, which no other node was ever told to create
    final List<byte[]> walEntries = (List<byte[]>) entry.get(4);
    final List<Map<Integer, Integer>> bucketDeltas = (List<Map<Integer, Integer>>) entry.get(5);
    assertThat(bucketDeltas).hasSameSizeAs(walEntries);
    assertThat(walEntries).as("the restored index's pages are shipped").isNotEmpty();
    for (final byte[] wal : walEntries)
      for (final WALFile.WALPage page : ArcadeStateMachine.deserializeWalTransaction(wal).pages)
        assertThat(db.getFileManager().existsFile(page.fileId)).as("WAL page of file %d", page.fileId).isTrue();

    // The type still takes writes, including the duplicate key the unique index refused
    db.transaction(() -> db.command("sql", "INSERT INTO Work SET id = 'k0'"));
  }

  private Set<Integer> indexFileIds() {
    return new HashSet<>(db.getSchema().getType("Work").getIndexByProperties("id").getFileIds());
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
