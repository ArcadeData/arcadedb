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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12358: a follower applying a replicated index compaction deleted the superseded index file BEFORE the full
 * schema reload that stops referencing it. The previous schema generation stays published for the whole reload (issue #7963),
 * so every query reading that index meanwhile failed with {@code IllegalArgumentException: File with id N was not found},
 * which a client receives as a bad request. A follower now keeps the file until the new generation is published, and a read
 * that still reaches a retired file is an {@code IndexException}, which plans the statement again on the new generation.
 * <p>
 * The many other indexed types are what makes the reload take long enough for the readers to meet it on every round.
 */
class Issue12358FollowerReadsDuringReplicatedCompactionIT extends BaseRaftHATest {
  private static final int OTHER_TYPES = 120;
  private static final int ROUNDS      = 6;
  private static final int PER_ROUND   = 4_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 30_000L);
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void aFollowerKeepsAnsweringIndexReadsWhileItAppliesACompaction() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = (leader + 1) % getServerCount();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());

    leaderDb.transaction(() -> {
      final DocumentType doc = leaderDb.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(2).create();
      doc.createProperty("name", Type.STRING);
      // Small pages: a compaction merges only an index of 2 pages or more, and every round must have one to apply
      leaderDb.getSchema().buildTypeIndex("Doc", new String[] { "name" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
          .withPageSize(4096).create();
      for (int i = 0; i < OTHER_TYPES; i++) {
        final DocumentType other = leaderDb.getSchema().createDocumentType("Other" + i);
        other.createProperty("k", Type.STRING);
        leaderDb.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Other" + i, "k");
      }
    });
    insert(leaderDb, 0);
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final Database followerDb = getServerDatabase(follower, getDatabaseName());
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong reads = new AtomicLong();
    final List<String> failures = new CopyOnWriteArrayList<>();
    final Thread reader = new Thread(() -> {
      while (!stop.get())
        try (final ResultSet rs = followerDb.query("sql", "SELECT count(*) AS c FROM Doc WHERE name = ?", "name-17")) {
          rs.next();
          reads.incrementAndGet();
        } catch (final RuntimeException e) {
          failures.add(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }, "issue12358-reader");
    reader.start();

    int replaced = 0;
    try {
      final TypeIndex index = (TypeIndex) leaderDb.getSchema().getIndexByName("Doc[name]");
      for (int round = 1; round <= ROUNDS; round++) {
        final Set<Integer> before = fileIdsOf(followerDb);
        insert(leaderDb, round);
        index.scheduleCompaction();
        index.compact();
        for (int i = 0; i < getServerCount(); i++)
          waitForReplicationIsCompleted(i);
        if (!fileIdsOf(followerDb).equals(before))
          ++replaced;
      }
    } finally {
      stop.set(true);
      reader.join();
    }

    assertThat(replaced).as("every compaction must have replaced the follower's index files").isEqualTo(ROUNDS);
    assertThat(reads.get()).as("the follower must have been read throughout").isPositive();
    assertThat(failures).as("index reads on the follower while it applied %d compactions", ROUNDS).isEmpty();
  }

  private static Set<Integer> fileIdsOf(final Database db) {
    final Set<Integer> ids = new HashSet<>();
    for (final IndexInternal bucketIndex : ((TypeIndex) db.getSchema().getIndexByName("Doc[name]")).getIndexesOnBuckets())
      ids.add(bucketIndex.getFileId());
    return ids;
  }

  private static void insert(final Database db, final int round) {
    db.transaction(() -> {
      for (int i = 0; i < PER_ROUND; i++)
        db.newDocument("Doc").set("name", "name-" + (i % 500), "round", round).save();
    });
  }
}
