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
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Duplicate lightweight edges: what an application gets if it creates one anyway, and the two places that notice.
 * <p>
 * A lightweight edge is the triple (type, out, in), so two of them over the same ordered pair are one edge stored
 * twice. Enforcement is opt-in because it is O(degree), so this state is reachable by design and its behaviour is
 * documented rather than prevented.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LightweightEdgeDuplicateTest extends TestHelper {

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This fixture deliberately builds the state the checker is being asked to report.
    return false;
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("V").create();
      database.getSchema().buildEdgeType().withName("Follows").withLightweight(true).create();
    });
  }

  @Test
  void checkDatabaseCountsDuplicatesWithoutCallingThemCorruption() {
    final RID a = newVertex(0);
    final RID b = newVertex(1);

    database.transaction(() -> {
      final MutableVertex source = database.lookupByRID(a, true).asVertex().modify();
      source.newEdge("Follows", b);
      source.newEdge("Follows", b);
    });

    try (final ResultSet rs = database.command("sql", "check database")) {
      final Result row = rs.next();

      // Counted once per duplicated EDGE, not once per list entry: every edge appears in exactly one OUT list, so
      // walking the OUT side alone gives the number an operator wants.
      assertThat(row.<Number>getProperty("duplicateLightEdges").longValue()).isEqualTo(1);

      // ...and NOT as damage. A duplicate reads fine; it just yields the edge twice.
      assertThat(row.<Number>getProperty("invalidLinks").longValue()).isZero();
      assertThat(row.<Number>getProperty("totalErrors").longValue()).isZero();
      assertThat(row.<java.util.Collection<?>>getProperty("warnings")).isEmpty();
      assertThat(row.<java.util.Collection<?>>getProperty("corruptedRecords")).isEmpty();
    }
  }

  @Test
  void checkDatabaseReportsNoneWhenThereAreNoDuplicates() {
    final RID a = newVertex(0);
    final RID b = newVertex(1);
    final RID c = newVertex(2);

    database.transaction(() -> {
      final MutableVertex source = database.lookupByRID(a, true).asVertex().modify();
      source.newEdge("Follows", b);
      source.newEdge("Follows", c);
    });

    try (final ResultSet rs = database.command("sql", "check database")) {
      assertThat(rs.next().<Number>getProperty("duplicateLightEdges").longValue()).isZero();
    }
  }

  /**
   * Deleting a duplicated lightweight edge removes one copy. This is the wart the documentation has to state: the
   * delete is not idempotent-complete, it must be repeated once per copy.
   */
  @Test
  void deletingADuplicatedEdgeRemovesOneCopy() {
    final RID a = newVertex(0);
    final RID b = newVertex(1);

    database.transaction(() -> {
      final MutableVertex source = database.lookupByRID(a, true).asVertex().modify();
      source.newEdge("Follows", b);
      source.newEdge("Follows", b);
    });

    assertThat(outDegree(a)).isEqualTo(2);

    database.transaction(() -> database.lookupByRID(a, true).asVertex()
        .getEdges(Vertex.DIRECTION.OUT).iterator().next().delete());

    assertThat(outDegree(a)).isEqualTo(1);
    assertThat(inDegree(b)).isEqualTo(1);

    database.transaction(() -> database.lookupByRID(a, true).asVertex()
        .getEdges(Vertex.DIRECTION.OUT).iterator().next().delete());

    assertThat(outDegree(a)).isZero();
    assertThat(inDegree(b)).isZero();
  }

  /**
   * GraphBatch already sorts by source before connecting, so ordering within a source group by (type, destination)
   * makes duplicates adjacent and detection free.
   */
  @Test
  void graphBatchCountsDuplicatesBufferedInOneFlush() {
    final RID a = newVertex(0);
    final RID b = newVertex(1);
    final RID c = newVertex(2);

    final long[] duplicates = new long[1];
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(a, "Follows", b);
      batch.newEdge(a, "Follows", c);
      batch.newEdge(a, "Follows", b);   // duplicate of the first
      batch.flush();
      duplicates[0] = batch.getDuplicateLightEdges();
    }

    assertThat(duplicates[0]).isEqualTo(1);
    assertThat(outDegree(a)).as("detection reports, it does not silently drop edges").isEqualTo(3);
  }

  @Test
  void graphBatchRaisesOnADuplicateWhenTheTypeDeclaresUnique() {
    database.transaction(
        () -> database.getSchema().buildEdgeType().withName("Rated").withLightweight(true).withUnique(true).create());

    final RID a = newVertex(0);
    final RID b = newVertex(1);

    assertThatThrownBy(() -> {
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        batch.newEdge(a, "Rated", b);
        batch.newEdge(a, "Rated", b);
        batch.flush();
      }
    }).isInstanceOf(DuplicatedKeyException.class);
  }

  // ---------------------------------------------------------------- helpers

  private RID newVertex(final int id) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("V");
      v.set("id", id);
      v.save();
      rid[0] = v.getIdentity();
    });
    return rid[0];
  }

  private long outDegree(final RID vertex) {
    return degree(vertex, Vertex.DIRECTION.OUT);
  }

  private long inDegree(final RID vertex) {
    return degree(vertex, Vertex.DIRECTION.IN);
  }

  private long degree(final RID vertex, final Vertex.DIRECTION direction) {
    final long[] n = new long[1];
    database.transaction(() -> n[0] = database.lookupByRID(vertex, true).asVertex().countEdges(direction));
    return n[0];
  }
}
