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

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locstat (issue #687): {@code CHECK DATABASE} reported 14 edges failing with
 * {@code error on loading (error: ClassCastException)}. That happens in {@link GraphDatabaseChecker}
 * when a vertex adjacency-list entry points to a RID that resolves to a record which is NOT an edge:
 * {@code edgeRID.asEdge(true)} = {@code (Edge) lookupByRID(...)} throws {@link ClassCastException}.
 * <p>
 * This test reproduces that exact category (a vertex's IN list holds a pointer to a plain vertex) and
 * documents what plain check vs. fix mode do with the *pointed-to* record, since the escalation asks
 * whether {@code CHECK DATABASE FIX} handles these safely.
 */
class Issue687ClassCastEdgeCheckTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";

  // The corrupted DB is created on purpose; the automatic post-test integrity check would trip on the
  // check-only variant that intentionally leaves the corruption in place.
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * Plain CHECK (no fix): the dangling pointer to a non-edge must be reported as
   * "error on loading (ClassCastException)" and nothing must be mutated.
   */
  @Test
  void classCastOnEdgeLoadIsReportedAndReadOnly() {
    final int degree = 20;
    final RID hubRid = createHub(degree);
    final RID bystander = injectClassCastEntry(hubRid);

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);

    @SuppressWarnings("unchecked")
    final Collection<String> warnings = (Collection<String>) stats.get("warnings");
    assertThat(warnings).anyMatch(w -> w.contains(bystander.toString()) && w.contains("error on loading"));
    // The cast failure is the ClassCastException category (JDK message contains "cast").
    assertThat(warnings).anyMatch(w -> w.contains(bystander.toString()) && w.toLowerCase().contains("cast"));

    // The pointed-to record is a valid vertex, not a corrupt edge, so it must NOT be flagged for deletion:
    // it is an INVALID LINK (a bad edge-list pointer), not a corrupted record.
    @SuppressWarnings("unchecked")
    final Collection<RID> corrupted = (Collection<RID>) stats.get("corruptedRecords");
    assertThat(corrupted).doesNotContain(bystander);
    assertThat((Long) stats.get("invalidLinks")).isGreaterThanOrEqualTo(1L);

    // READ-ONLY: the bystander vertex is untouched and the bad entry is still in the hub's IN list.
    database.transaction(() -> assertThat(bystander.asVertex(true)).isNotNull());
  }

  /**
   * FIX mode must repair the adjacency list (drop the dangling entry) WITHOUT deleting the innocent
   * bystander record the bad entry pointed at, and must preserve the hub's real edges.
   */
  @Test
  void classCastOnEdgeLoadFixBehavior() {
    final int degree = 20;
    final RID hubRid = createHub(degree);
    final RID bystander = injectClassCastEntry(hubRid);

    new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);

    // FIX must remove the dangling LIST entry but must NOT destroy the pointed-to record: a
    // ClassCastException means the RID resolves to a valid record of another type (here a vertex),
    // and raw-deleting it bypasses graph-aware cleanup (leaving that vertex's own edges dangling).
    assertThat(existsAsVertex(bystander)).as("FIX must not delete the non-edge record the bad entry pointed at").isTrue();

    // The hub's real edges must be preserved...
    assertThat(countInEdges(hubRid)).as("real edges must be preserved by FIX").isEqualTo(degree);

    // ...and the dangling pointer must be gone, so a follow-up check is clean.
    final Map<String, Object> verify = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);
    assertThat((Long) verify.get("totalWarnings")).as("database must be clean after FIX").isEqualTo(0L);
  }

  /** Injects one corrupt IN-list entry on the hub: edgeRID = a plain vertex (not an edge). Returns that vertex RID. */
  private RID injectClassCastEntry(final RID hubRid) {
    final RID[] bystander = new RID[1];
    database.transaction(() -> {
      // A perfectly valid vertex that has no business being referenced as an edge.
      bystander[0] = database.newVertex(VERTEX_TYPE).set("role", "bystander").save().getIdentity();

      final VertexInternal hub = (VertexInternal) hubRid.asVertex(true);
      // Second field is the connected (outgoing) vertex of an IN entry; asEdge on the first field fails first.
      ((DatabaseInternal) database).getGraphEngine()
          .getOrCreateEdgeList(hub, Vertex.DIRECTION.IN)
          .add(bystander[0], hub.getIdentity());
    });
    return bystander[0];
  }

  private boolean existsAsVertex(final RID rid) {
    final boolean[] exists = new boolean[1];
    database.transaction(() -> {
      try {
        rid.asVertex(true);
        exists[0] = true;
      } catch (final Exception e) {
        exists[0] = false;
      }
    });
    return exists[0];
  }

  private long countInEdges(final RID hubRid) {
    final long[] count = new long[1];
    database.transaction(() -> count[0] = hubRid.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE));
    return count[0];
  }

  /** Creates the hub plus {@code degree} sources, each with one edge source -> hub (hub's IN list). */
  private RID createHub(final int degree) {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE, 1);
      database.getSchema().createEdgeType(EDGE_TYPE, 1);
    });

    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());

    database.transaction(() -> {
      for (int i = 0; i < degree; i++)
        database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, hub[0]);
    });
    return hub[0];
  }
}
