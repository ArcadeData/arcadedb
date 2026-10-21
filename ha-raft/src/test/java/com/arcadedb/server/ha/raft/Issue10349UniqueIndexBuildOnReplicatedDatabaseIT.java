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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10349: {@code CREATE INDEX ... UNIQUE} on a populated type stayed quadratic on a replicated database, where the sorted
 * build is refused. The record-at-a-time build of a large type now checks the uniqueness of its keys once, over the finished index,
 * instead of looking each key up in the index it is filling at every commit.
 * <p>
 * What a replicated database must still give: the index on every node with every entry, and the live index refusing duplicates
 * afterwards. The refusal of a duplicate by the build itself is covered by the engine test of the same issue: on a replicated
 * database a build that fails after its batches were written leaves the leader's copy quarantined, whichever batch met the
 * duplicate, which is the schema session's own handling and not what this test is about.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10349UniqueIndexBuildOnReplicatedDatabaseIT extends BaseRaftHATest {
  // ABOVE arcadedb.index.buildSortedUniqueMinRecords (10,000), the size the build takes the deferred check from
  private static final int RECORDS = 12_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aLargeUniqueIndexIsOnEveryNodeWithEveryEntry() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    populate(leader);
    assertClusterConsistency();

    leader.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();
    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      final TypeIndex index = withResyncRetry(serverIndex, db -> db.getSchema().getType("Work").getIndexByProperties("id"));
      assertThat(index).as("server %d", serverIndex).isNotNull();
      assertThat(index.isUnique()).as("server %d", serverIndex).isTrue();
      assertThat(index.countEntries()).as("server %d", serverIndex).isEqualTo(RECORDS);
      assertThat(index.get(new Object[] { key(RECORDS / 2) }).hasNext()).as("server %d", serverIndex).isTrue();
    }

    // The index is live: a duplicate is refused on the leader, a new key is accepted and reaches the replicas
    assertThatThrownBy(() -> leader.transaction(() -> leader.newDocument("Work").set("id", key(7)).save()))
        .isInstanceOf(DuplicatedKeyException.class);
    leader.transaction(() -> leader.newDocument("Work").set("id", "fresh").save());
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final long entries = withResyncRetry(i, db -> db.getSchema().getType("Work").getIndexByProperties("id").countEntries());
      assertThat(entries).as("server %d", i).isEqualTo(RECORDS + 1);
    }
  }

  private static void populate(final Database database) {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Work").withTotalBuckets(3).create();
    type.createProperty("id", Type.STRING);
    for (int from = 0; from < RECORDS; from += 2_000) {
      final int first = from;
      database.transaction(() -> {
        for (int i = first; i < Math.min(RECORDS, first + 2_000); i++)
          database.newDocument("Work").set("id", key(i)).save();
      });
    }
  }

  /** Ids in a scrambled order: the keys never arrive sorted. */
  private static String key(final int i) {
    return "https://openalex.org/W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL));
  }
}
