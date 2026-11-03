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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Design-review suspicion 4 (verification only): a follower that restarts in the middle of the delivery-only chunk sequence of
 * a replicated LSM compaction (after the chunk that creates the compacted file, before the publishing entry) runs the full
 * schema load, whose {@code sweepOrphanCompactedIndexFiles} might drop the compacted file nothing references yet. The
 * publishing entry would then point the index at a file this node no longer holds.
 * <p>
 * The leader side runs a real compaction through {@link RaftReplicatedDatabase#runWithCompactionReplication} over a fake
 * broker; the captured payload is split with the production splitter and applied to a byte-identical copy of the database
 * through {@link ArcadeStateMachine#applySchemaEntry}, closing and reopening that copy between two chunks, which is what a
 * snapshot taken mid-sequence followed by a restart amounts to (Ratis replays only the entries after the marker).
 */
class CompactionChunksAcrossFollowerRestartTest {
  private static final String DB         = "compaction";
  private static final String INDEX_NAME = "Work[id]";
  private static final int    RECORDS    = 4_000;
  private static final int    PAGE_SIZE  = 16 * 1024;

  @TempDir
  Path tempDir;

  private Path          followerPath;
  private LocalDatabase leaderDb;
  private LocalDatabase followerDb;

  /**
   * @param fillerTypes one-bucket types created after the index, each taking one file id: they move the ids the compaction
   *                    allocates, and with them the order in which the leader's {@code HashMap} of new files ships their pages
   */
  private void setUp(final int fillerTypes) throws IOException {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    final Path leaderPath = tempDir.resolve("leader").resolve(DB);
    followerPath = tempDir.resolve("follower").resolve(DB);

    leaderDb = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).create();
    final DocumentType type = leaderDb.getSchema().buildDocumentType().withName("Work").withTotalBuckets(1).create();
    type.createProperty("id", Type.STRING);
    leaderDb.getSchema().buildTypeIndex("Work", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
        .withPageSize(PAGE_SIZE).create();
    for (int i = 0; i < fillerTypes; i++)
      leaderDb.getSchema().buildDocumentType().withName("Filler" + i).withTotalBuckets(1).create();
    for (int from = 0; from < RECORDS; from += 500) {
      final int start = from;
      leaderDb.transaction(() -> {
        for (int i = start; i < start + 500; i++)
          leaderDb.newDocument("Work").set("id", key(i)).save();
      });
    }
    leaderDb.close();

    // The follower starts byte-identical to the leader, as after a snapshot install
    FileUtils.copyDirectory(leaderPath.toFile(), followerPath.toFile());
    leaderDb = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).open();
    followerDb = (LocalDatabase) new DatabaseFactory(followerPath.toString()).open();
  }

  @AfterEach
  void tearDown() {
    try {
      if (leaderDb != null && leaderDb.isOpen())
        leaderDb.drop();
      if (followerDb != null && followerDb.isOpen())
        followerDb.drop();
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    }
  }

  /** Control: the whole sequence applied without a restart leaves the follower with the complete index. */
  @Test
  void theWholeSequenceWithoutARestartLeavesTheIndexComplete() throws IOException {
    setUp(0);
    final Compaction compaction = compactOnTheLeader();
    for (int i = 0; i < compaction.chunks.size(); i++)
      apply(compaction.chunks.get(i), i);

    assertFollowerIndexComplete(compaction);
  }

  /**
   * A restart after EVERY possible position inside the sequence: after the first chunk (it creates the compacted file and
   * carries its first pages), in the middle, and right before the publishing entry (every page delivered, nothing referencing
   * the files yet). Each position starts from a fresh copy of the pre-compaction follower.
   */
  @Test
  void aRestartAtAnyPositionInsideTheSequenceKeepsTheDeliveredFilesAndTheIndexComplete() throws IOException {
    setUp(0);
    final Compaction compaction = restartAtEveryPosition();
    // Ids 3 (compacted), 4 (bloom filter), 5 (new mutable): the compacted file's pages ship first
    assertThat(new ArrayList<>(compaction.filesToAdd.keySet())).as("shipping order of the new files").isSorted();
  }

  /**
   * The same with the new files' ids at 14 (compacted), 15 (bloom filter) and 16 (new mutable): the leader iterates its
   * {@code HashMap} of new files in bucket order, so the new mutable (bucket 0) ships its pages, header included, BEFORE the
   * compacted file it names as its sub-index. A restart in between finds a mutable claiming a compacted file with no pages.
   */
  @Test
  void aRestartAtAnyPositionWhenTheNewMutableShipsBeforeTheCompactedFile() throws IOException {
    setUp(11);
    final Compaction compaction = restartAtEveryPosition();
    assertThat(compaction.filesToAdd.keySet().iterator().next()).as("the first file whose pages ship").isEqualTo(16);
  }

  private Compaction restartAtEveryPosition() throws IOException {
    followerDb.close();
    final Path pristine = tempDir.resolve("pristine").resolve(DB);
    FileUtils.copyDirectory(followerPath.toFile(), pristine.toFile());

    final Compaction compaction = compactOnTheLeader();
    for (int appliedBeforeRestart = 1; appliedBeforeRestart < compaction.chunks.size(); appliedBeforeRestart++) {
      followerPath = tempDir.resolve("follower-" + appliedBeforeRestart).resolve(DB);
      FileUtils.copyDirectory(pristine.toFile(), followerPath.toFile());
      followerDb = (LocalDatabase) new DatabaseFactory(followerPath.toString()).open();
      restartAfter(compaction, appliedBeforeRestart);
      followerDb.close();
    }
    followerDb = null;
    return compaction;
  }

  private void restartAfter(final Compaction compaction, final int appliedBeforeRestart) {
    for (int i = 0; i < appliedBeforeRestart; i++)
      apply(compaction.chunks.get(i), i);

    for (final int fileId : compaction.filesToAdd.keySet())
      assertThat(followerDb.getFileManager().existsFile(fileId)).as("file %d (%s) delivered by the first chunk", fileId,
          compaction.filesToAdd.get(fileId)).isTrue();

    // A snapshot marker covering the chunks applied so far, then a restart: the database is closed and opened again (full
    // schema load, orphan sweep), and Ratis replays only the entries after the marker
    followerDb.close();
    followerDb = (LocalDatabase) new DatabaseFactory(followerPath.toString()).open();

    for (final int fileId : compaction.filesToAdd.keySet())
      assertThat(followerDb.getFileManager().existsFile(fileId)).as(
          "restart after %d of %d chunks: file %d (%s) is still there after the reopen", appliedBeforeRestart,
          compaction.chunks.size(), fileId, compaction.filesToAdd.get(fileId)).isTrue();

    for (int i = appliedBeforeRestart; i < compaction.chunks.size(); i++)
      apply(compaction.chunks.get(i), i);

    assertFollowerIndexComplete(compaction);
  }

  private void assertFollowerIndexComplete(final Compaction compaction) {
    for (final int fileId : compaction.filesToAdd.keySet())
      assertThat(followerDb.getFileManager().existsFile(fileId)).as("file %d (%s) the compaction published", fileId,
          compaction.filesToAdd.get(fileId)).isTrue();

    final Index index = followerDb.getSchema().getIndexByName(INDEX_NAME);
    assertThat(index.countEntries()).as("entries of the follower's index after the compaction").isEqualTo(RECORDS);
    for (int i = 0; i < RECORDS; i += 97)
      assertThat(index.get(new Object[] { key(i) }).hasNext()).as("key %d findable on the follower", i).isTrue();
  }

  private void apply(final RaftLogEntryCodec.DecodedEntry chunk, final int position) {
    stateMachineOver(() -> followerDb).applySchemaEntry(chunk, 1_000L + position, false);
  }

  /**
   * Runs one compaction of the index on the leader through the Raft wrapper, and returns the publishing payload it handed the
   * broker, split by the production splitter into at least three entries.
   */
  @SuppressWarnings("unchecked")
  private Compaction compactOnTheLeader() {
    // One page per WAL entry, so the splitter has something to split
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker() {
      @Override
      long walChunkBudget() {
        return PAGE_SIZE;
      }
    };
    final ArcadeStateMachine leaderStateMachine = stateMachineOver(() -> leaderDb);
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(leaderStateMachine).leader(true).currentTerm(7L)
        .transactionBroker(broker);
    leaderStateMachine.setRaftHAServer(raft);
    new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), leaderDb, raft);

    final TypeIndex index = (TypeIndex) leaderDb.getSchema().getIndexByName(INDEX_NAME);
    index.scheduleCompaction();
    try {
      assertThat(index.compact()).as("the compaction ran on the leader").isTrue();
    } catch (final Exception e) {
      throw new AssertionError(e);
    }

    final List<List<Object>> published = broker.calls("replicateSchema");
    assertThat(published).as("the compaction published one schema change").hasSize(1);
    final List<Object> call = published.getFirst();
    final String dbName = (String) call.get(0);
    final String schemaJson = (String) call.get(1);
    final Map<Integer, String> filesToAdd = (Map<Integer, String>) call.get(2);
    final Map<Integer, String> filesToRemove = (Map<Integer, String>) call.get(3);
    final List<byte[]> walEntries = (List<byte[]>) call.get(4);
    final List<Map<Integer, Integer>> bucketDeltas = (List<Map<Integer, Integer>>) call.get(5);
    assertThat(filesToAdd).as("files the compaction created").isNotEmpty();
    assertThat(walEntries.size()).as("WAL entries the compaction shipped").isGreaterThanOrEqualTo(3);

    int largestWal = 0;
    for (final byte[] wal : walEntries)
      largestWal = Math.max(largestWal, wal.length);
    final int lastHeader = RaftLogEntryCodec.encodeSchemaEntry(dbName, schemaJson, Collections.emptyMap(), filesToRemove,
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()).size();
    final int firstHeader = RaftLogEntryCodec.encodeSchemaEntry(dbName, "", filesToAdd, Collections.emptyMap(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()).size();
    final long cap = Math.max(lastHeader, firstHeader) + largestWal + 64L;

    final List<RaftLogEntryCodec.DecodedEntry> chunks = new ArrayList<>();
    for (final ByteString chunk : RaftTransactionBroker.splitSchemaEntry(dbName, schemaJson, filesToAdd, filesToRemove,
        walEntries, bucketDeltas, Collections.emptyList(), cap, Integer.MAX_VALUE))
      chunks.add(RaftLogEntryCodec.decode(chunk));
    assertThat(chunks.size()).as("entries the compaction was split into").isGreaterThanOrEqualTo(3);
    assertThat(chunks.getFirst().moreChunksFollow()).isTrue();
    assertThat(chunks.getLast().moreChunksFollow()).isFalse();
    return new Compaction(filesToAdd, chunks);
  }

  private static ArcadeStateMachine stateMachineOver(final DatabaseSupplier database) {
    return new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return database.get();
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

  private static String key(final int i) {
    final long mixed = i * 0x9E3779B97F4A7C15L;
    return Long.toHexString(mixed) + "-" + Long.toHexString(Long.reverse(mixed) ^ i) + "-" + i;
  }

  @FunctionalInterface
  private interface DatabaseSupplier {
    DatabaseInternal get();
  }

  private record Compaction(Map<Integer, String> filesToAdd, List<RaftLogEntryCodec.DecodedEntry> chunks) {
  }
}
