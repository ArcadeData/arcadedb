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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6136 (3): {@code autoFix} is a sum of repair ACTIONS of different kinds, and one kind was not in it at all.
 * <p>
 * After #6128 it counts records deleted plus dangling adjacency entries pruned. Both are genuine repairs and both are
 * genuine writes to distinct pages, so the number is not wrong - but an operator reading it as "how many broken
 * things were there" over-counts (one edge that is both listed in a chain and corrupt as a record contributes two),
 * and a rebuilt chain contributes NOTHING, being reported only in the warnings. So the one number an operator reads
 * to decide whether a run did anything mixed three kinds with different weights, one of them invisible.
 * <p>
 * {@code autoFix} is deliberately unchanged - existing readers and existing expectations depend on it, and
 * redefining it as "defects fixed" would need the arms to agree on what one defect IS across a dangling entry, a
 * corrupt record and a rebuilt chain, which they cannot without collapsing information the warnings carry. What is
 * added is the breakdown: {@code removedRecords}, {@code prunedDanglingEntries} and {@code reconnectedEdges}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseRepairBreakdownTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";
  private static final int    DEGREE      = 30;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // These tests inject on-disk corruption on purpose; the post-test check would (correctly) flag it.
    return false;
  }

  /**
   * A raw-deleted edge record: both of its adjacency entries are pruned and nothing is deleted (the record is
   * already gone), so the whole of {@code autoFix} is the prune arm. That is the commonest damage shape there is,
   * and until the breakdown existed the report could not say which arm produced the number.
   */
  @Test
  void aPrunedDanglingEntryIsReportedApartFromADeletedRecord() {
    createGraph();

    final RID edge = anyEdge();
    database.transaction(() -> database.getSchema().getBucketById(edge.getBucketId()).deleteRecord(edge));

    final Result row = runFix();

    assertThat((Long) row.getProperty("autoFix"))
        .as("one raw-deleted edge leaves a dangling entry on each side").isEqualTo(2L);
    assertThat((Long) row.getProperty("prunedDanglingEntries"))
        .as("both repairs were prunes").isEqualTo(2L);
    assertThat((Long) row.getProperty("removedRecords"))
        .as("the edge record was already gone, so nothing was deleted").isZero();
    assertThat((Long) row.getProperty("reconnectedEdges"))
        .as("no chain was rebuilt and no back-reference was missing").isZero();
    assertBreakdownAddsUp(row);
  }

  /**
   * A rebuilt chain: {@code reconnectedEdges} carries it, and {@code autoFix} deliberately still does not - which is
   * the whole point of adding a separate counter rather than folding it in and changing every existing number.
   */
  @Test
  void aRebuiltChainIsCountedAndIsStillNotPartOfAutoFix() {
    final RID hub = createGraph();

    final RID[] head = new RID[1];
    database.transaction(() -> head[0] = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk());
    assertThat(head[0]).as("the hub must have an in-edge list to break").isNotNull();
    corruptRecordTypeByte((DatabaseInternal) database, head[0]);

    final Result row = runFix();

    assertThat((Long) row.getProperty("reconnectedEdges"))
        .as("every entry rebuilt from a surviving edge record must be counted").isEqualTo(DEGREE);
    assertThat((Long) row.getProperty("autoFix"))
        .as("a rebuilt chain has never contributed to autoFix and must not start to")
        .isEqualTo((Long) row.getProperty("removedRecords") + (Long) row.getProperty("prunedDanglingEntries"));
    assertBreakdownAddsUp(row);

    database.transaction(() -> assertThat(hub.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
        .as("the adjacency must actually be back").isEqualTo(DEGREE));
  }

  /** A clean database reports the breakdown as zeroes rather than omitting the keys. */
  @Test
  void theBreakdownIsAlwaysPresent() {
    createGraph();

    final Result row = runFix();

    assertThat((Long) row.getProperty("removedRecords")).isZero();
    assertThat((Long) row.getProperty("prunedDanglingEntries")).isZero();
    assertThat((Long) row.getProperty("reconnectedEdges")).isZero();
    assertBreakdownAddsUp(row);
  }

  /** The invariant the breakdown promises: {@code autoFix} is exactly its two constituent arms. */
  private static void assertBreakdownAddsUp(final Result row) {
    assertThat((Long) row.getProperty("autoFix"))
        .as("autoFix is documented as removedRecords + prunedDanglingEntries")
        .isEqualTo((Long) row.getProperty("removedRecords") + (Long) row.getProperty("prunedDanglingEntries"));
  }

  private Result runFix() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
      return rs.next();
    }
  }

  private RID anyEdge() {
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = database.iterateType(EDGE_TYPE, false).next().getIdentity());
    return holder[0];
  }

  /** A hub with {@link #DEGREE} incoming edges. */
  private RID createGraph() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });

    final RID[] holder = new RID[1];
    database.transaction(() -> {
      holder[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity();
      for (int i = 0; i < DEGREE; i++)
        database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, holder[0].asVertex(true));
    });
    return holder[0];
  }
}
