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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link MatchEdgeByIndexStep}'s {@code openCursor()} always resolves through
 * {@code Database.lookupByKey()}, a full-key equality lookup. That lookup routes to
 * {@code TypeIndex.get(Object[])}, which drains and closes the real per-bucket LSM cursor synchronously, inside
 * its own try-with-resources, before ever returning - the object the step holds is an already-exhausted
 * {@code IndexCursorCollection}, whose {@code close()} is a documented no-op ({@code Cursor}: "The default is a
 * no-op for the implementations backed by an in-memory collection, which hold nothing").
 * <p>
 * This test pins that fact down: a compacted, non-unique composite index with many rows sharing one key, driven
 * only partway (mirroring a {@code LIMIT} or an early {@code ResultSet.close()}), never leaves a live series
 * cursor registered - with or without the step's {@code close()} override. The override is kept anyway as a
 * robustness/consistency measure (matching {@code FetchFromIndexStep}), not as a fix for a reproducible leak;
 * this test also pins down that the override remains idempotent and side-effect free on this path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MatchEdgeByIndexStepCloseTest extends TestHelper {
  private static final String EDGE_TYPE_NAME       = "TRANSFER";
  private static final int    DUPLICATE_KEY_EDGES  = 400;

  @BeforeEach
  @Override
  public void beforeTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Test
  void partialConsumptionNeverLeavesALiveSeriesCursorRegistered() {
    final LSMTreeIndexCompacted compacted = createCompactedFixture();

    // Many edges share the same (transactionId, date) key, so the equality seek would have plenty left to walk
    // if it were driven by a lazy series cursor - exactly the shape a LIMIT or an early close() abandons mid-scan.
    final ResultSet rs = database.query("opencypher",
        "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId='DUP' AND t.date=date('2026-01-01') RETURN t");
    assertThat(rs.hasNext()).as("precondition: the seek must find at least one of the duplicated-key edges").isTrue();
    rs.next();

    // The equality lookup already fully materialized and closed the real cursor before this step ever pulled a
    // row, so there is nothing left registered even mid-scan.
    assertThat(compacted.countActiveCursors())
        .as("a full-key equality lookup must never leave a live series cursor registered").isZero();

    rs.close();
    assertThat(compacted.countActiveCursors()).isZero();

    // idempotent: closing an already-closed result set must not fail or misbehave
    rs.close();
    assertThat(compacted.countActiveCursors()).isZero();
  }

  private LSMTreeIndexCompacted createCompactedFixture() {
    database.getSchema().createVertexType("Account");
    final EdgeType transfer = database.getSchema().buildEdgeType().withName(EDGE_TYPE_NAME).withTotalBuckets(1).create();
    transfer.createProperty("transactionId", Type.STRING);
    transfer.createProperty("date", Type.DATE);
    database.getSchema().buildTypeIndex(EDGE_TYPE_NAME, new String[] { "transactionId", "date" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(1024).create();

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Account").set("name", "A").save();
      final MutableVertex b = database.newVertex("Account").set("name", "B").save();
      for (int i = 0; i < DUPLICATE_KEY_EDGES; i++)
        a.newEdge(EDGE_TYPE_NAME, b).set("transactionId", "DUP").set("date", "2026-01-01").save();
    });

    final LSMTreeIndex index = bucketIndex();
    try {
      if (index.scheduleCompaction())
        index.compact();
    } catch (final IOException | InterruptedException e) {
      throw new IllegalStateException("cannot compact the fixture index", e);
    }

    final LSMTreeIndexCompacted compacted = index.getMutableIndex().getSubIndex();
    assertThat(compacted).as("the fixture must produce a compacted sub-index, otherwise there is nothing to pin down")
        .isNotNull();
    assertThat(compacted.countActiveCursors()).isZero();
    return compacted;
  }

  private LSMTreeIndex bucketIndex() {
    return (LSMTreeIndex) database.getSchema().getType(EDGE_TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
  }
}
