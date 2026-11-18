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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bulk loads written with {@code MERGE (a)-[:R {uuid: ...}]->(b)} (how a graph library keeps a relationship unique by an id) run
 * several transactions at once, each storing hundreds of relationships. Two such transactions never create the same relationship,
 * so neither may be refused because of the other.
 * <p>
 * The {@code MergedEdgeRegistry} of #11482 keys a relationship by a hash taken to one of 16,384 stripes and a transaction is refused
 * when a stripe it created in was written since its begin. A batch of 500 relationships checks 500 stripes and marks about 3,000
 * (see {@code addMergedEdgeBetweenExistingNodes}), so any two batches that overlap in time share a stripe and the later one is
 * refused, whatever the relationships are. The tests use two batches on DISJOINT vertices, so that nothing but the registry can
 * refuse one for the other.
 */
class ConcurrentMergeBatchesTest {
  private static final int VERTICES_PER_BATCH = 1_000;
  private static final int BATCH              = 500;
  private static final String MERGE_BATCH = "UNWIND $rows AS r MATCH (a:N {id: r[0]}), (b:N {id: r[1]}) MERGE (a)-[e:R {uuid: r[2]}]->(b)";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/ConcurrentMergeBatchesTest").create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("R");
    database.getSchema().getType("N").createProperty("id", Long.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "N", "id");
    database.getSchema().getType("R").createProperty("uuid", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "R", "uuid");
    database.transaction(() -> database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:N {id: i})",
        Map.of("n", 4 * VERTICES_PER_BATCH)));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  /** The rows of one batch: {@code BATCH} relationships with their own uuids between random vertices of one range. */
  private static List<List<Object>> batch(final String prefix, final int firstVertex, final long seed) {
    final java.util.Random random = new java.util.Random(seed);
    final List<List<Object>> rows = new ArrayList<>(BATCH);
    for (int i = 0; i < BATCH; i++)
      rows.add(List.of((long) (firstVertex + random.nextInt(VERTICES_PER_BATCH)), (long) (firstVertex + random.nextInt(VERTICES_PER_BATCH)),
          prefix + i));
    return rows;
  }

  private long relationshipCount() {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:R]->() RETURN count(r) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /**
   * The second transaction began before the first one committed, as two writers of a bulk load do. They create different
   * relationships between different vertices: both commit.
   */
  @Test
  void twoBatchesOnDifferentVerticesDoNotRefuseEachOther() throws Exception {
    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      database.command("opencypher", MERGE_BATCH, Map.of("rows", batch("a", 0, 1)));

      other.submit(() -> {
        database.begin();
        database.command("opencypher", MERGE_BATCH, Map.of("rows", batch("b", VERTICES_PER_BATCH, 2)));
        database.commit();
      }).get(60, TimeUnit.SECONDS);

      // Refused with "A relationship this transaction created because a MERGE ... did not find it was created by a concurrent
      // transaction in the meantime" while nothing it created was created by the other one
      database.commit();
    } finally {
      other.shutdownNow();
    }

    assertThat(relationshipCount()).isEqualTo(2L * BATCH);
  }

  /** The same, with the later transaction beginning before the earlier one committed and many commits in between. */
  @Test
  void aBatchSurvivesManyCommitsOfOtherBatchesBetweenItsBeginAndItsCommit() throws Exception {
    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      database.command("opencypher", MERGE_BATCH, Map.of("rows", batch("a", 0, 1)));

      for (int round = 0; round < 6; round++) {
        final int vertices = VERTICES_PER_BATCH * (1 + round % 3);
        final int r = round;
        other.submit(() -> {
          database.begin();
          database.command("opencypher", MERGE_BATCH, Map.of("rows", batch("o" + r + "-", vertices, 10 + r)));
          database.commit();
        }).get(60, TimeUnit.SECONDS);
      }

      database.commit();
    } finally {
      other.shutdownNow();
    }

    assertThat(relationshipCount()).isEqualTo(7L * BATCH);
  }
}
