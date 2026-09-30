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

import com.arcadedb.database.Database;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7438: a replica keeps inserting into a bucket while the leader runs a {@code CREATE INDEX}
 * over it. Before the fix the leader accepted a replica entry between the index build and the {@code SCHEMA_ENTRY}
 * that publishes it, so the index missed that record on every node, or the followers spliced two entries claiming the
 * same page version. The leader now refuses replica entries for the database for the length of the DDL, and the
 * replica's retry loop rides it out: every insert the replica was acknowledged for is in the index, on every node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue7438ReplicaWritesDuringLeaderDdlIT extends BaseRaftHATest {
  private static final String TYPE_NAME        = "Issue7438Doc";
  private static final int    SEED             = 3000;
  private static final int    MIN_ACKNOWLEDGED = 20;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void replicaInsertsDuringLeaderCreateIndexAreAllIndexed() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      final DocumentType type = leaderDb.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.getOrCreateProperty("id", Type.LONG);
      type.getOrCreateProperty("tag", Type.STRING);
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < SEED; i++)
        leaderDb.newDocument(TYPE_NAME).set("id", (long) i).set("tag", "seed").save();
    });
    waitForReplicationIsCompleted(replicaIndex);

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicInteger acknowledged = new AtomicInteger();
    final AtomicInteger refused = new AtomicInteger();
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      final Future<?> writer = pool.submit(() -> {
        int next = SEED;
        while (!stop.get()) {
          final long id = next;
          try {
            replicaDb.transaction(() -> replicaDb.newDocument(TYPE_NAME).set("id", id).set("tag", "replica").save(), false, 0);
            acknowledged.incrementAndGet();
            next++;
          } catch (final NeedRetryException e) {
            // Refused while the leader runs its DDL: the same id is retried, as any caller would.
            refused.incrementAndGet();
          }
        }
      });

      // DDLs (property plus index) back to back until the replica's writer has been refused at least once: a build that finishes without ever
      // overlapping a replica write proves nothing, so the loop keeps opening windows (bounded) rather than assume one.
      final long ddlDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
      int rounds = 0;
      // A fresh property and index per round, never a drop: dropping an index under a transaction the replica has in flight
      // closes its file beneath that transaction, a different matter from the window under test.
      while (refused.get() == 0 && System.nanoTime() < ddlDeadline) {
        final String property = "extra" + rounds;
        leaderDb.getSchema().getType(TYPE_NAME).createProperty(property, Type.LONG);
        leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, property);
        rounds++;
      }
      assertThat(refused.get()).as("replica inserts refused during %d leader DDL round(s)", rounds).isGreaterThan(0);
      // The indexes that stay, so the final comparison has something to compare.
      leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, "id");
      leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, "tag");
      leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, "id", "tag");

      // The window must have let inserts through as well as refused them, or the run proves nothing.
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (acknowledged.get() < MIN_ACKNOWLEDGED && System.nanoTime() < deadline)
        Thread.sleep(50);
      stop.set(true);
      writer.get(120, TimeUnit.SECONDS);
      assertThat(acknowledged.get()).as("inserts acknowledged to the replica's writer").isGreaterThanOrEqualTo(MIN_ACKNOWLEDGED);
    } finally {
      stop.set(true);
      pool.shutdownNow();
    }

    waitForReplicationIsCompleted(replicaIndex);
    final long expected = SEED + acknowledged.get();
    // Every node holds the same records and, in every index, the same entries: two entries claiming one page version
    // spliced on apply, or a replica insert landing while the index is built, used to leave the nodes disagreeing.
    // (An insert prepared by the replica before it applied the new index is a separate matter, see #8686.)
    final Database leaderCheck = getServerDatabase(leaderIndex, getDatabaseName());
    final Database replicaCheck = getServerDatabase(replicaIndex, getDatabaseName());
    awaitValue(expected, () -> replicaCheck.countType(TYPE_NAME, true));
    assertThat(leaderCheck.countType(TYPE_NAME, true)).as("records on the leader (refused %d times)", refused.get())
        .isEqualTo(expected);
    assertThat(replicaCheck.countType(TYPE_NAME, true)).as("records on the replica").isEqualTo(expected);
    for (final Index leaderIndexDef : leaderCheck.getSchema().getType(TYPE_NAME).getAllIndexes(true)) {
      final Index replicaIndexDef = replicaCheck.getSchema().getIndexByName(leaderIndexDef.getName());
      assertThat(replicaIndexDef.countEntries()).as("index %s: replica vs leader", leaderIndexDef.getName())
          .isEqualTo(leaderIndexDef.countEntries());
    }
    assertClusterConsistency();
  }
}
