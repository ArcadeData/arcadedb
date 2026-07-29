/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5492: a commit that runs inside a {@code recordFileChanges()} callback
 * has its WAL buffered instead of shipped as a {@code TX_ENTRY}, because at that point the files the
 * DDL is creating do not yet exist on followers. The buffered entries are meant to travel embedded in
 * the {@code SCHEMA_ENTRY} sent once the callback returns.
 * <p>
 * The send was guarded on {@code addFiles}, {@code removeFiles} and the schema version only, with the
 * drained WAL absent from the condition and the {@code finally} clearing the buffer unconditionally.
 * A callback that committed pages without creating a file or bumping the schema version therefore had
 * those pages applied on the leader - {@code commit2ndPhase} runs before the buffering - and the only
 * copy that would have reached followers silently discarded.
 * <p>
 * The follower's page version then trails the leader's by the number of dropped page writes, so the
 * next ordinary transaction touching one of those pages ships a version the follower cannot apply:
 * {@code WALVersionGapException}, database marked diverged, snapshot resync. Both halves are asserted
 * below so a regression that loses the write is distinguishable from one that also diverges the
 * cluster.
 * <p>
 * The sibling {@code runWithCompactionReplication} always guarded the same two thread-locals with
 * {@code walEntries.isEmpty()} included; the asymmetry between the two was the defect.
 */
@Tag("slow")
class Issue5492SchemaWalNotShippedIT extends BaseRaftHATest {

  private static final String TYPE_NAME = "SchemaWalDoc";

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void cleanupHooks() {
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
  }

  @Test
  void walBufferedInsideRecordFileChangesReachesFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());

    // Create the type through the ordinary DDL path and let it replicate, so the backing files exist
    // on both nodes before the callback below writes into them. Without this the follower would drop
    // the pages for a missing file and the test could not tell that apart from the defect.
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    leaderDb.command("sql", "INSERT INTO " + TYPE_NAME + " SET id = 0");
    waitForAllServers();
    assertThat(countOn(followerDb)).as("baseline replicated before the callback").isEqualTo(1);

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    // The shape the guard dropped: a recordFileChanges() callback that commits a record into an
    // already-existing type and touches nothing else. No file is created, none is removed, and the
    // schema version does not move, so none of the three conditions in the send guard held.
    final DatabaseInternal wrapped = ((DatabaseInternal) leaderDb).getWrappedDatabaseInstance();
    wrapped.recordFileChanges(() -> {
      wrapped.begin();
      wrapped.newDocument(TYPE_NAME).set("id", 1).save();
      wrapped.commit();
      return null;
    });

    assertThat(countOn(leaderDb)).as("leader applied the record written inside the callback").isEqualTo(2);

    waitForAllServers();
    assertThat(awaitCountOn(followerDb, 2))
        .as("WAL buffered inside recordFileChanges must reach the follower, not be discarded")
        .isEqualTo(2);

    // A page whose version was bumped on the leader but not on the follower does not fail until the
    // next transaction touches it. Commit an ordinary write into the same bucket and require the
    // follower to apply it without a gap.
    leaderDb.command("sql", "INSERT INTO " + TYPE_NAME + " SET id = 2");
    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("follower must see no WAL version gap after the callback's pages were replicated")
        .isZero();
    assertThat(awaitCountOn(followerDb, 3))
        .as("follower converges with the leader after the subsequent ordinary write")
        .isEqualTo(3);
  }

  private long countOn(final Database db) {
    // count() rather than count(*): the latter reads a cached per-bucket counter, which is the wrong
    // tool when the question is whether the pages themselves arrived.
    return ((Number) db.command("sql", "SELECT count(id) AS cnt FROM " + TYPE_NAME).next().getProperty("cnt"))
        .longValue();
  }

  private long awaitCountOn(final Database db, final long expected) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    long count = countOn(db);
    while (count != expected && System.currentTimeMillis() < deadline) {
      Thread.sleep(250);
      count = countOn(db);
    }
    return count;
  }
}
