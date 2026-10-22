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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10506: on a three node cluster a {@code CREATE INDEX ... UNIQUE} over a large type holding a duplicate key is refused by
 * the pass over the finished index (issue #10349), after every batch of the build was committed. The refusal used to quarantine
 * the leader's database (UNPUBLISHED_SCHEMA_CHANGE) and keep a partially built sub-index the replicas never had until the resync.
 * Now it costs the caller an error and nothing else: no node is quarantined, no node has the index, and the same index is built
 * once the duplicate is gone.
 */
class Issue10506RefusedUniqueIndexBuildIT extends BaseRaftHATest {
  // ABOVE arcadedb.index.buildSortedUniqueMinRecords (10,000), the size the build takes the deferred check from
  private static final int RECORDS = 12_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aRefusedUniqueIndexBuildQuarantinesNoNodeAndLeavesNoIndex() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    populate(leader);
    // In ANOTHER bucket than the record already holding the key: the build of each bucket commits, and only the pass over the
    // finished index meets the duplicate. In the same bucket the build of that bucket refuses it before it commits anything
    final String duplicatedKey = key(RECORDS / 2);
    final String twinBucket = otherBucketThanTheOneHolding(leader, duplicatedKey);
    leader.transaction(() -> leader.newDocument("Work").set("id", duplicatedKey).save(twinBucket));
    assertClusterConsistency();

    assertThatThrownBy(() -> leader.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close())
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getRaftPlugin(i).getRaftHAServer().getStateMachine().quarantineCause(getDatabaseName()))
          .as("server %d is not quarantined by a refused CREATE INDEX", i).isNull();
    assertThat(findLeaderIndex()).as("the leader keeps its leadership").isEqualTo(leaderIndex);

    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      final Object index = withResyncRetry(serverIndex, db -> db.getSchema().getType("Work").getIndexByProperties("id"));
      assertThat(index).as("server %d has no index from the refused build", serverIndex).isNull();
    }

    // The duplicate gone, the same statement builds the index on every node
    leader.transaction(() -> leader.command("sql", "DELETE FROM Work WHERE id = ? LIMIT 1", duplicatedKey).close());
    leader.command("sql", "CREATE INDEX ON Work (id) UNIQUE").close();
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final long entries = withResyncRetry(i, db -> db.getSchema().getType("Work").getIndexByProperties("id").countEntries());
      assertThat(entries).as("server %d", i).isEqualTo(RECORDS);
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

  private static String otherBucketThanTheOneHolding(final Database database, final String key) {
    final int holder = database.query("sql", "SELECT FROM Work WHERE id = ?", key).nextIfAvailable().getIdentity().get()
        .getBucketId();
    for (final Bucket bucket : database.getSchema().getType("Work").getBuckets(false))
      if (bucket.getFileId() != holder)
        return bucket.getName();
    throw new IllegalStateException("type Work has a single bucket");
  }

  /** Ids in a scrambled order: the keys never arrive sorted. */
  private static String key(final int i) {
    return "https://openalex.org/W" + (1_000_000_000L + ((i * 2_654_435_761L) & 0xFFFFFFFFL));
  }
}
