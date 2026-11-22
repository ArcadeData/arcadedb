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
import com.arcadedb.exception.ConcurrentModificationException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Two bulk loads written with {@code MERGE (a)-[:R {uuid: ...}]->(b)} that create different relationships between the SAME
 * vertices, as the batches of a load do when the vertices are picked at random: the existence check of each MERGE reads the edge
 * lists of its two vertices and finds nothing, and the other transaction appends to those lists meanwhile. That append does not
 * change what the check found (the relationship it looked for is another one), and the {@code MergedEdgeRegistry} key of the
 * relationship the MERGE then creates is what guards the same relationship being created twice. So neither transaction may be
 * refused for the edge lists it read.
 */
class ConcurrentMergeBatchesSharingVerticesTest {
  private static final int    VERTICES = 60;
  private static final String MERGE_BATCH = "UNWIND $rows AS r MATCH (a:N {id: r[0]}), (b:N {id: r[1]}) MERGE (a)-[e:R {uuid: r[2]}]->(b)";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/ConcurrentMergeBatchesSharingVerticesTest").create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("R");
    database.getSchema().getType("N").createProperty("id", Long.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "N", "id");
    database.getSchema().getType("R").createProperty("uuid", String.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "R", "uuid");
    database.transaction(() -> database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:N {id: i})", Map.of("n", VERTICES)));
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

  /** Every vertex is the start of one relationship and the end of another, so two batches share every vertex they touch. */
  private static List<List<Object>> ring(final String prefix, final int shift) {
    final List<List<Object>> rows = new ArrayList<>();
    for (int i = 0; i < VERTICES; i++)
      rows.add(List.of((long) i, (long) ((i + shift) % VERTICES), prefix + i));
    return rows;
  }

  private long relationshipCount() {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:R]->() RETURN count(r) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  @Test
  void batchesCreatingDifferentRelationshipsBetweenTheSameVerticesAreNotRefused() throws Exception {
    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      database.command("opencypher", MERGE_BATCH, Map.of("rows", ring("a", 1))).close();

      other.submit(() -> {
        database.begin();
        database.command("opencypher", MERGE_BATCH, Map.of("rows", ring("b", 2))).close();
        database.commit();
      }).get(60, TimeUnit.SECONDS);

      // Refused with "Edge list of vertex ... was read by this transaction before a concurrent transaction changed it"
      database.commit();
    } finally {
      other.shutdownNow();
    }

    assertThat(relationshipCount()).isEqualTo(2L * VERTICES);
  }

  @Test
  void aBatchCreatingARelationshipAnotherBatchCommittedIsStillRefused() throws Exception {
    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      database.command("opencypher", MERGE_BATCH, Map.of("rows", ring("same", 1))).close();

      other.submit(() -> {
        database.begin();
        database.command("opencypher", MERGE_BATCH, Map.of("rows", ring("same", 1))).close();
        database.commit();
      }).get(60, TimeUnit.SECONDS);

      // The relationship the other transaction committed is the one this MERGE did not find: a second one would be a duplicate
      assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      other.shutdownNow();
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(relationshipCount()).isEqualTo(VERTICES);
  }

  @Test
  void aRetryAfterTheRefusalFindsTheRelationshipsAndCreatesNoSecondOne() throws Exception {
    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      database.command("opencypher", MERGE_BATCH, Map.of("rows", ring("same", 1))).close();
      other.submit(() -> {
        database.begin();
        database.command("opencypher", MERGE_BATCH, Map.of("rows", ring("same", 1))).close();
        database.commit();
      }).get(60, TimeUnit.SECONDS);
      assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      other.shutdownNow();
      if (database.isTransactionActive())
        database.rollback();
    }

    database.transaction(() -> database.command("opencypher", MERGE_BATCH, Map.of("rows", ring("same", 1))).close());

    assertThat(relationshipCount()).isEqualTo(VERTICES);
  }
}
