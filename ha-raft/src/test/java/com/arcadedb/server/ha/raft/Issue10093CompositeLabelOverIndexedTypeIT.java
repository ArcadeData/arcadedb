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
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #10093: a composite openCypher label type over an indexed type ({@code CREATE (m:V1:NewLabel {id: ...})}, where {@code V1} has an
 * index on {@code id}) was reported "configured differently" between the leader and its followers by the database comparison that ends
 * every test of the fixture, whoever created it. The two sides printed the same JSON, so the difference was in something the JSON does not
 * carry: the sub-indexes the super type's index grew for the new buckets. The comparison is NOT overridden here, so the fixture's
 * end-of-test check is what fails if the type or its indexes differ again, and the assertions below name what must hold.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10093CompositeLabelOverIndexedTypeIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void aCompositeLabelOverAnIndexedTypeIsTheSameTypeOnEveryNode() {
    final int leader = requireLeaderIndex();
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());

    leaderDb.begin();
    leaderDb.command("opencypher", "CREATE (m:V1:Issue10093Label {id: 1004001})").close();
    leaderDb.commit();

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        final Database db = getServerDatabase(server, getDatabaseName());
        assertThat(db.getSchema().existsType("Issue10093Label~V1")).as("server %d has the composite type", server).isTrue();
        try (final ResultSet rs = db.query("sql", "SELECT FROM V1 WHERE id = ?", 1004001L)) {
          assertThat(rs.stream().count()).as("server %d finds the record through the index of V1", server).isEqualTo(1L);
        }
      });
    }

    // The index of the super type covers the buckets of the composite type, and does so with the same sub-indexes on every node
    final List<String> onLeader = subIndexesOfV1(getServerDatabase(leader, getDatabaseName()));
    assertThat(onLeader).as("the composite type's buckets are covered by the index of V1").isNotEmpty();
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      assertThat(subIndexesOfV1(db)).as("server %d against the leader", i).containsExactlyInAnyOrderElementsOf(onLeader);

      // Each sub-index is registered with the type that owns its bucket - the composite type - on every node, the leader that
      // created it and the followers that loaded the schema alike. The comparison that ends the test checks the same thing.
      final DocumentType composite = db.getSchema().getType("Issue10093Label~V1");
      final DocumentType v1 = db.getSchema().getType("V1");
      for (final Bucket bucket : composite.getBuckets(false)) {
        assertThat(composite.getPolymorphicBucketIndexByBucketId(bucket.getFileId(), null))
            .as("server %d: the composite type registers the sub-index of its bucket %s", i, bucket.getName()).hasSize(1);
        assertThat(v1.getPolymorphicBucketIndexByBucketId(bucket.getFileId(), null))
            .as("server %d: V1 does not register the sub-index of a bucket it does not own (%s)", i, bucket.getName()).isEmpty();
      }
    }
  }


  /** Every sub-index of the indexes on V1: its name, its bucket and the properties it covers. */
  private static List<String> subIndexesOfV1(final Database db) {
    final DocumentType v1 = db.getSchema().getType("V1");
    final List<String> result = new ArrayList<>();
    for (final TypeIndex typeIndex : v1.getAllIndexes(false))
      for (final Index sub : typeIndex.getIndexesOnBuckets())
        result.add(sub.getName() + "|" + sub.getAssociatedBucketId() + "|" + sub.getPropertyNames());
    return result;
  }
}
