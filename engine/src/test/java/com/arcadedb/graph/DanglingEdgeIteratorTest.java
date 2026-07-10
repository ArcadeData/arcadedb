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
package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #4689: iterating the edges of a vertex that has a dangling edge pointer
 * (the edge record was removed but the link still lives in the vertex's edge segment) must not throw
 * {@link java.util.NoSuchElementException}.
 * <p>
 * The untyped {@link EdgeIterator} (used by {@code getEdges(DIRECTION)} with no edge-type filter)
 * loads the edge record lazily. When the content is forced to load - under REPEATABLE_READ isolation,
 * or when the edge's bucket/type no longer resolves - a dangling pointer raised a RecordNotFoundException
 * that {@code next()} silently skipped, then threw NoSuchElementException once the segment ended, even
 * though {@code hasNext()} had returned {@code true}. A standard for-each loop (e.g. the Studio graph
 * serializer's "filter out not connected edges" pass) propagated that as an unhandled exception.
 */
class DanglingEdgeIteratorTest {
  private static final String DB_PATH = "./target/databases/danglingEdgeIterator";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void iteratingDanglingEdgeDoesNotThrowNoSuchElement() {
    final RID[] sourceVertex = new RID[1];
    final RID[] edgeRid = new RID[1];

    database.transaction(() -> {
      database.getSchema().createVertexType("User");
      database.getSchema().createEdgeType("Knows");
      final MutableVertex a = database.newVertex("User").set("name", "A").save();
      final MutableVertex b = database.newVertex("User").set("name", "B").save();
      final Edge e = a.newEdge("Knows", b);
      sourceVertex[0] = a.getIdentity();
      edgeRid[0] = e.getIdentity();
    });

    // Create a dangling edge pointer: remove the edge RECORD at bucket level, bypassing graph cleanup,
    // so the link survives in vertex A's edge segment while the edge record is gone.
    database.transaction(() ->
        database.getSchema().getBucketById(edgeRid[0].getBucketId()).deleteRecord(edgeRid[0]));

    // REPEATABLE_READ forces the edge content to load during iteration, exposing the dangling pointer.
    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);

    database.transaction(() -> {
      final Vertex a = database.lookupByRID(sourceVertex[0], true).asVertex();

      final Throwable thrown = catchThrowable(() -> {
        int count = 0;
        for (final Edge ignored : a.getEdges(Vertex.DIRECTION.OUT))
          count++;
        assertThat(count).as("the dangling edge must be skipped during iteration").isZero();
      });

      assertThat(thrown).as("iterating a dangling edge must not throw NoSuchElementException").isNull();
    });
  }
}
