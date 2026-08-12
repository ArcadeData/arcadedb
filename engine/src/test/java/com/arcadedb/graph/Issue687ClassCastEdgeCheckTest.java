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
 * This test reproduces that exact category (a vertex adjacency list holds a pointer to a plain vertex) and
 * documents what plain check vs. fix mode do with the *pointed-to* record, since the escalation asks
 * whether {@code CHECK DATABASE FIX} handles these safely. {@code checkIncomingEdges} (IN list) and
 * {@code checkOutgoingEdges} (OUT list) carry separate, independently maintained copies of the same
 * {@link ClassCastException} handler, so both directions are exercised here.
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

  /**
   * {@code checkOutgoingEdges} carries its own copy of the {@code ClassCastException} handler (a separate
   * catch block, mirroring {@code checkIncomingEdges}). Same corruption shape, OUT list this time: nothing
   * here proves the OUT-side handler behaves the same as the IN-side one already covered above.
   */
  @Test
  void classCastOnOutgoingEdgeLoadIsReportedAndReadOnly() {
    final int degree = 20;
    final RID hubRid = createHubWithOutgoingEdges(degree);
    final RID bystander = injectClassCastEntryOnOutList(hubRid);

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);

    @SuppressWarnings("unchecked")
    final Collection<String> warnings = (Collection<String>) stats.get("warnings");
    assertThat(warnings).anyMatch(w -> w.contains(bystander.toString()) && w.contains("error on loading"));
    assertThat(warnings).anyMatch(w -> w.contains(bystander.toString()) && w.toLowerCase().contains("cast"));

    @SuppressWarnings("unchecked")
    final Collection<RID> corrupted = (Collection<RID>) stats.get("corruptedRecords");
    assertThat(corrupted).doesNotContain(bystander);
    assertThat((Long) stats.get("invalidLinks")).isGreaterThanOrEqualTo(1L);

    database.transaction(() -> assertThat(bystander.asVertex(true)).isNotNull());
  }

  /**
   * Same FIX-mode guarantee as {@link #classCastOnEdgeLoadFixBehavior()}, exercised through the OUT-list
   * handler instead of the IN-list one.
   */
  @Test
  void classCastOnOutgoingEdgeLoadFixBehavior() {
    final int degree = 20;
    final RID hubRid = createHubWithOutgoingEdges(degree);
    final RID bystander = injectClassCastEntryOnOutList(hubRid);

    new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);

    assertThat(existsAsVertex(bystander)).as("FIX must not delete the non-edge record the bad entry pointed at").isTrue();
    assertThat(countOutEdges(hubRid)).as("real edges must be preserved by FIX").isEqualTo(degree);

    final Map<String, Object> verify = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);
    assertThat((Long) verify.get("totalWarnings")).as("database must be clean after FIX").isEqualTo(0L);
  }

  /**
   * Compound-corruption regression for the catch-scope point raised in review: a GENUINELY corrupt edge
   * (its {@code getIn()} does not match the vertex whose list it is in) shares that vertex's adjacency
   * list with a SEPARATE, merely-dangling entry (a plain vertex where an edge RID is expected). Resolving
   * the mismatch triggers the "CHECK ALL INCOMING EDGES" rescan, which walks every entry in the list -
   * including the dangling one - and hits its raw {@code asEdge(true)} call, throwing a
   * {@link ClassCastException} for THAT entry while the mismatched edge is still being processed.
   * <p>
   * That stray exception must NOT be caught by the handler meant for {@code edgeRID} resolving to a
   * non-edge (it is a different RID that failed, mid-rescan): the mismatched edge is real corruption and
   * must still end up in {@code corruptedRecords}, while the dangling entry's bystander record must still
   * never be flagged corrupt. See {@code asEdgeOrDanglingEntry}/{@code DanglingEdgeListEntryException} in
   * {@link GraphDatabaseChecker}.
   */
  @Test
  void classCastDuringSiblingRescanDoesNotMaskAGenuinelyCorruptEdge() {
    final RID[] ids = buildCompoundCorruptionScenario();
    final RID mismatchedEdge = ids[1];
    final RID bystander = ids[2];

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);

    @SuppressWarnings("unchecked")
    final Collection<RID> corrupted = (Collection<RID>) stats.get("corruptedRecords");
    assertThat(corrupted).as("the genuinely mismatched edge must still be flagged corrupt despite the sibling cast failure")
        .contains(mismatchedEdge);
    assertThat(corrupted).as("the dangling entry's bystander record must never be flagged corrupt").doesNotContain(bystander);
  }

  /**
   * Same compound-corruption scenario as {@link #classCastDuringSiblingRescanDoesNotMaskAGenuinelyCorruptEdge()},
   * exercised through the OUT-list handler ({@code checkOutgoingEdges}, the "CHECK ALL OUT EDGES" rescan at
   * {@code nextEntry.getFirst().asEdge(true).getOut()}) instead of the IN-list one. The root cause here was
   * literally two independently-maintained copies of the same handler, so the compound case needs its own
   * coverage on each side - a fix mirrored to only one copy must fail this test.
   */
  @Test
  void classCastDuringSiblingRescanDoesNotMaskAGenuinelyCorruptEdgeOnOutList() {
    final RID[] ids = buildCompoundCorruptionScenarioOnOutList();
    final RID mismatchedEdge = ids[1];
    final RID bystander = ids[2];

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);

    @SuppressWarnings("unchecked")
    final Collection<RID> corrupted = (Collection<RID>) stats.get("corruptedRecords");
    assertThat(corrupted).as("the genuinely mismatched edge must still be flagged corrupt despite the sibling cast failure")
        .contains(mismatchedEdge);
    assertThat(corrupted).as("the dangling entry's bystander record must never be flagged corrupt").doesNotContain(bystander);
  }

  /**
   * Builds: a hub vertex whose IN list holds (1) a real edge from {@code source} to some OTHER vertex
   * (mismatched: {@code edge.getIn()} != hub, wired into the hub's list anyway) and (2) a dangling entry
   * pointing at a plain bystander vertex. Returns {hubRid, mismatchedEdgeRid, bystanderRid}.
   */
  private RID[] buildCompoundCorruptionScenario() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE, 1);
      database.getSchema().createEdgeType(EDGE_TYPE, 1);
    });

    final RID[] ids = new RID[3];
    database.transaction(() -> {
      final Vertex hub = database.newVertex(VERTEX_TYPE).set("name", "hub").save();
      final Vertex wrongTarget = database.newVertex(VERTEX_TYPE).set("role", "wrongTarget").save();
      final Vertex source = database.newVertex(VERTEX_TYPE).set("role", "source").save();

      // Genuine edge, but its IN endpoint is NOT the hub: this must stay flagged corrupt.
      final Edge mismatchedEdge = source.newEdge(EDGE_TYPE, wrongTarget);

      final VertexInternal hubInternal = (VertexInternal) hub.getIdentity().asVertex(true);
      final EdgeLinkedList hubInList = ((DatabaseInternal) database).getGraphEngine()
          .getOrCreateEdgeList(hubInternal, Vertex.DIRECTION.IN);
      hubInList.add(mismatchedEdge.getIdentity(), source.getIdentity());

      // A separate dangling entry: a plain vertex where an edge RID is expected.
      final Vertex bystander = database.newVertex(VERTEX_TYPE).set("role", "bystander").save();
      hubInList.add(bystander.getIdentity(), hub.getIdentity());

      ids[0] = hub.getIdentity();
      ids[1] = mismatchedEdge.getIdentity();
      ids[2] = bystander.getIdentity();
    });
    return ids;
  }

  /**
   * Same as {@link #buildCompoundCorruptionScenario()}, but wired into the hub's OUT list: a real edge FROM
   * some other vertex ({@code edge.getOut()} != hub) plus a separate dangling entry.
   */
  private RID[] buildCompoundCorruptionScenarioOnOutList() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE, 1);
      database.getSchema().createEdgeType(EDGE_TYPE, 1);
    });

    final RID[] ids = new RID[3];
    database.transaction(() -> {
      final Vertex hub = database.newVertex(VERTEX_TYPE).set("name", "hub").save();
      final Vertex wrongSource = database.newVertex(VERTEX_TYPE).set("role", "wrongSource").save();
      final Vertex target = database.newVertex(VERTEX_TYPE).set("role", "target").save();

      // Genuine edge, but its OUT endpoint is NOT the hub: this must stay flagged corrupt.
      final Edge mismatchedEdge = wrongSource.newEdge(EDGE_TYPE, target);

      final VertexInternal hubInternal = (VertexInternal) hub.getIdentity().asVertex(true);
      final EdgeLinkedList hubOutList = ((DatabaseInternal) database).getGraphEngine()
          .getOrCreateEdgeList(hubInternal, Vertex.DIRECTION.OUT);
      hubOutList.add(mismatchedEdge.getIdentity(), target.getIdentity());

      // A separate dangling entry: a plain vertex where an edge RID is expected.
      final Vertex bystander = database.newVertex(VERTEX_TYPE).set("role", "bystander").save();
      hubOutList.add(bystander.getIdentity(), hub.getIdentity());

      ids[0] = hub.getIdentity();
      ids[1] = mismatchedEdge.getIdentity();
      ids[2] = bystander.getIdentity();
    });
    return ids;
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

  /** Same injection as {@link #injectClassCastEntry(RID)}, but into the hub's OUT list. */
  private RID injectClassCastEntryOnOutList(final RID hubRid) {
    final RID[] bystander = new RID[1];
    database.transaction(() -> {
      bystander[0] = database.newVertex(VERTEX_TYPE).set("role", "bystander").save().getIdentity();

      final VertexInternal hub = (VertexInternal) hubRid.asVertex(true);
      // Second field is the connected (incoming) vertex of an OUT entry; asEdge on the first field fails first.
      ((DatabaseInternal) database).getGraphEngine()
          .getOrCreateEdgeList(hub, Vertex.DIRECTION.OUT)
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

  private long countOutEdges(final RID hubRid) {
    final long[] count = new long[1];
    database.transaction(() -> count[0] = hubRid.asVertex(true).countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE));
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

  /** Creates the hub plus {@code degree} targets, each with one edge hub -> target (hub's OUT list). */
  private RID createHubWithOutgoingEdges(final int degree) {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE, 1);
      database.getSchema().createEdgeType(EDGE_TYPE, 1);
    });

    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());

    database.transaction(() -> {
      final Vertex hubVertex = hub[0].asVertex(true);
      for (int i = 0; i < degree; i++)
        hubVertex.newEdge(EDGE_TYPE, database.newVertex(VERTEX_TYPE).set("i", i).save());
    });
    return hub[0];
  }
}
