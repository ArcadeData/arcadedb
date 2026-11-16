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
import com.arcadedb.utility.LockManager;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #12236: on a healthy cluster under plain concurrent inserts, every follower refused the same entry with a WAL version
 * gap on page 1 (or 0) of an index file a compaction had just created, and installed a snapshot. The Raft log held a
 * transaction writing version 2 of that page BEFORE the entry of the compaction that creates the file, so the followers
 * skipped the page (the file did not exist there yet), then created it at version 1, and refused version 3.
 * <p>
 * The transaction was a commit that had passed the leader's wait for an open recording session before the compaction opened
 * one, and that reached its file locks only after the swap: it locked the new mutable file the swap had just released and
 * dispatched its entry while the compaction was still serializing the one creating that file.
 */
class Issue12236CompactionNewFileHeldUntilPublishedTest {
  private static final String   DB         = "issue12236";
  private static final String   INDEX_NAME = "Work[id]";
  private static final int      RECORDS    = 4_000;
  private static final int      PAGE_SIZE  = 16 * 1024;
  private static final long     TERM       = 7L;
  /** A hang detector, never a latency bound: every wait below ends on an event. */
  private static final Duration HANG       = Duration.ofSeconds(60);

  @TempDir
  Path tempDir;

  private LocalDatabase          leaderDb;
  private LocalDatabase          followerDb;
  private RaftReplicatedDatabase replicated;
  private ExecutorService        racer;

  @BeforeEach
  void setUp() throws IOException {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    final Path leaderPath = tempDir.resolve("leader").resolve(DB);
    final Path followerPath = tempDir.resolve("follower").resolve(DB);

    leaderDb = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).create();
    final DocumentType type = leaderDb.getSchema().buildDocumentType().withName("Work").withTotalBuckets(1).create();
    type.createProperty("id", Type.STRING);
    leaderDb.getSchema().buildTypeIndex("Work", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
        .withPageSize(PAGE_SIZE).create();
    for (int from = 0; from < RECORDS; from += 500) {
      final int start = from;
      leaderDb.transaction(() -> {
        for (int i = start; i < start + 500; i++)
          leaderDb.newDocument("Work").set("id", "k" + i).save();
      });
    }
    leaderDb.close();

    // Both nodes start from the same copy, as after a snapshot
    FileUtils.copyDirectory(leaderPath.toFile(), followerPath.toFile());
    leaderDb = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).open();
    followerDb = (LocalDatabase) new DatabaseFactory(followerPath.toString()).open();
    // The racing commit waits on file locks while the test stages the interleaving
    leaderDb.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, HANG.toMillis());
    racer = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() {
    try {
      racer.shutdownNow();
      if (leaderDb != null && leaderDb.isOpen())
        leaderDb.drop();
      if (followerDb != null && followerDb.isOpen())
        followerDb.drop();
    } finally {
      GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    }
  }

  /**
   * A commit that is past the check for an open recording session and queued for a lock when the compaction starts reaches
   * its locks after the swap, while the compaction is still on its way to the log (the slow side on a loaded leader). Its
   * entry must land after the compaction's, so a follower applies both, and the next commit on the same page, without a gap.
   */
  @Test
  void aCommitRacingTheSwapReachesTheLogAfterTheEntryCreatingItsFile() throws Exception {
    final List<LogEntry> log = new CopyOnWriteArrayList<>();
    final AtomicLong lastIndex = new AtomicLong();
    final CountDownLatch compactionEntryAppended = new CountDownLatch(1);
    final ArcadeStateMachine[] leaderStateMachine = new ArcadeStateMachine[1];
    final int bucketFileId = leaderDb.getSchema().getType("Work").getBuckets(false).getFirst().getFileId();
    final Object blocker = new Object();
    final Set<Integer> createdByCompaction = ConcurrentHashMap.newKeySet();

    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    broker.on("replicateTransaction", args -> {
      // the entry takes its log position when it is dispatched
      final long index = lastIndex.incrementAndGet();
      @SuppressWarnings("unchecked")
      final ByteString payload = RaftLogEntryCodec.encodeTxEntry(DB, (byte[]) args[1], (Map<Integer, Integer>) args[2]);
      log.add(new LogEntry(index, "TX", payload));
      // committed and applied here (publishing the pages) a quorum round trip later: not before the compaction's entry is out
      awaitOrFail(compactionEntryAppended, "the compaction's entry");
      leaderStateMachine[0].applyTransaction(entry(leaderStateMachine[0], payload, index));
      return index;
    });
    broker.on("replicateSchema", args -> {
      @SuppressWarnings("unchecked")
      final Map<Integer, String> filesToAdd = (Map<Integer, String>) args[2];
      createdByCompaction.addAll(filesToAdd.keySet());
      // The swap is done: the commit queued before the compaction started runs into the swapped index now
      leaderDb.getTransactionManager().unlockFile(bucketFileId, blocker);
      // ...and the compaction is the slow side: its entry goes out once that commit has either dispatched its own entry, or
      // is parked on the lock of a file this entry creates
      await().atMost(HANG).pollInterval(Duration.ofMillis(2))
          .until(() -> !broker.calls("replicateTransaction").isEmpty() || waitersOnAnyOf(createdByCompaction) > 0);
      @SuppressWarnings("unchecked")
      final ByteString payload = RaftLogEntryCodec.encodeSchemaEntry(DB, (String) args[1], filesToAdd,
          (Map<Integer, String>) args[3], (List<byte[]>) args[4], (List<Map<Integer, Integer>>) args[5]);
      log.add(new LogEntry(lastIndex.incrementAndGet(), "SCHEMA", payload));
      compactionEntryAppended.countDown();
      return null;
    });
    leaderStateMachine[0] = leaderOver(broker);

    // The racing commit passes the check for an open recording session and queues for the bucket's lock
    assertThat(leaderDb.getTransactionManager().tryLockFile(bucketFileId, 0, blocker)).isEqualTo(LockManager.LOCK_STATUS.YES);
    final Future<?> racing = racer.submit(() -> {
      replicated.begin();
      replicated.newDocument("Work").set("id", "raced").save();
      replicated.commit();
    });
    await().atMost(HANG).pollInterval(Duration.ofMillis(2)).until(() -> waitersOnAnyOf(Set.of(bucketFileId)) > 0);

    final TypeIndex leaderIndex = (TypeIndex) leaderDb.getSchema().getIndexByName(INDEX_NAME);
    leaderIndex.scheduleCompaction();
    assertThat(leaderIndex.compact()).as("the compaction ran").isTrue();
    racing.get(HANG.toMillis(), TimeUnit.MILLISECONDS);

    // The next commit writes the next version of the page the raced one wrote
    replicated.begin();
    replicated.newDocument("Work").set("id", "after").save();
    replicated.commit();
    assertThat(leaderDb.getSchema().getIndexByName(INDEX_NAME).countEntries()).isEqualTo(RECORDS + 2L);

    final List<LogEntry> ordered = new ArrayList<>(log);
    ordered.sort(Comparator.comparingLong(LogEntry::index));

    final ArcadeStateMachine follower = stateMachineOver(followerDb);
    for (final LogEntry logEntry : ordered) {
      follower.applyTransaction(entry(follower, logEntry.payload(), logEntry.index()));
      assertThat(follower.isDatabaseDiverged(DB))
          .as("follower after the %s entry at index %d of %s (a WAL version gap on the compaction's new file)", logEntry.kind(),
              logEntry.index(), ordered.stream().map(LogEntry::kind).toList()).isFalse();
    }
    assertThat(ordered.stream().map(LogEntry::kind).toList()).as("the log").containsExactly("SCHEMA", "TX", "TX");

    final Index followerIndex = followerDb.getSchema().getIndexByName(INDEX_NAME);
    assertThat(followerIndex.get(new Object[] { "raced" }).hasNext()).as("the raced key on the follower").isTrue();
    assertThat(followerIndex.get(new Object[] { "after" }).hasNext()).as("the next key on the follower").isTrue();
    assertThat(followerIndex.countEntries()).as("index entries on the follower").isEqualTo(RECORDS + 2L);
  }

  private int waitersOnAnyOf(final Collection<Integer> fileIds) {
    int waiters = 0;
    for (final LockManager.LockStats lock : leaderDb.getTransactionManager().getLockStats())
      for (final int fileId : fileIds)
        if (lock.resource().equals(String.valueOf(fileId)))
          waiters += lock.waiters();
    return waiters;
  }

  private static void awaitOrFail(final CountDownLatch latch, final String what) {
    try {
      if (!latch.await(HANG.toMillis(), TimeUnit.MILLISECONDS))
        throw new AssertionError("Timed out waiting for " + what);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted waiting for " + what, e);
    }
  }

  private ArcadeStateMachine leaderOver(final FakeRaftTransactionBroker broker) {
    final ArcadeStateMachine leaderStateMachine = stateMachineOver(leaderDb);
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(leaderStateMachine).leader(true).currentTerm(TERM)
        .transactionBroker(broker);
    leaderStateMachine.setRaftHAServer(raft);
    replicated = new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), leaderDb, raft);
    return leaderStateMachine;
  }

  private static TransactionContext entry(final ArcadeStateMachine stateMachine, final ByteString payload, final long index) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(TERM).setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
    return TransactionContext.newBuilder().setStateMachine(stateMachine).setLogEntry(logEntry).build();
  }

  private static ArcadeStateMachine stateMachineOver(final DatabaseInternal database) {
    return new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return database;
      }

      @Override
      ClientId localClientId() {
        return null;
      }

      @Override
      long currentRaftTerm() {
        return TERM;
      }
    };
  }

  private record LogEntry(long index, String kind, ByteString payload) {
  }
}
