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
package com.arcadedb.query.opencypher.traversal;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for TraversalPath methods handling deleted entities:
 * withDeleted, subPath, reversed, toAlternatingList, toMap, getMember, equals, hashCode,
 * and constructor preservation of deletion masks.
 */
class TraversalPathDeletedEntitiesTest extends TestHelper {
  private Vertex a;
  private Vertex b;
  private Vertex c;
  private Vertex d;
  private Edge   ab;
  private Edge   bc;
  private Edge   cd;

  private TraversalPath createPath() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    final RID[] rids = new RID[7];
    database.transaction(() -> {
      final MutableVertex va = database.newVertex("V").set("n", "a").save();
      final MutableVertex vb = database.newVertex("V").set("n", "b").save();
      final MutableVertex vc = database.newVertex("V").set("n", "c").save();
      final MutableVertex vd = database.newVertex("V").set("n", "d").save();
      rids[0] = va.getIdentity();
      rids[1] = vb.getIdentity();
      rids[2] = vc.getIdentity();
      rids[3] = vd.getIdentity();
      rids[4] = va.newEdge("E", vb).save().getIdentity();
      rids[5] = vb.newEdge("E", vc).save().getIdentity();
      rids[6] = vc.newEdge("E", vd).save().getIdentity();
    });
    a = database.lookupByRID(rids[0], true).asVertex();
    b = database.lookupByRID(rids[1], true).asVertex();
    c = database.lookupByRID(rids[2], true).asVertex();
    d = database.lookupByRID(rids[3], true).asVertex();
    ab = database.lookupByRID(rids[4], true).asEdge();
    bc = database.lookupByRID(rids[5], true).asEdge();
    cd = database.lookupByRID(rids[6], true).asEdge();

    final TraversalPath path = new TraversalPath(a);
    path.addStep(ab, b);
    path.addStep(bc, c);
    return path;
  }

  @Test
  void testWithDeletedIdentityWhenUnchanged() {
    final TraversalPath path = createPath();
    final TraversalPath result = path.withDeleted(v -> false, e -> false);
    assertThat(result).isSameAs(path);
  }

  @Test
  void testWithDeletedIncrementalMarking() {
    final TraversalPath path = createPath();
    final TraversalPath path1 = path.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> false);

    assertThat(path1).isNotSameAs(path);
    final List<Object> nodes1 = path1.getNodesOrDeleted();
    assertThat(nodes1).containsExactly(a, DeletedEntityMarker.INSTANCE, c);
    assertThat(path1.getRelationshipsOrDeleted()).containsExactly(ab, bc);

    // Incremental marking: mark edge ab deleted
    final TraversalPath path2 = path1.withDeleted(v -> false, e -> e.getIdentity().equals(ab.getIdentity()));
    assertThat(path2).isNotSameAs(path1);
    assertThat(path2.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE, c);
    final List<Object> rels2 = path2.getRelationshipsOrDeleted();
    assertThat(rels2.get(0)).isInstanceOf(DeletedEntityMarker.class);
    assertThat(((DeletedEntityMarker) rels2.get(0)).getRelationshipType()).isEqualTo("E");
    assertThat(rels2.get(1)).isSameAs(bc);

    // Re-running with same predicates returns path2 by identity
    final TraversalPath unchanged = path2.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> e.getIdentity().equals(ab.getIdentity()));
    assertThat(unchanged).isSameAs(path2);
  }

  @Test
  void testWithDeletedIndependenceFromAddStep() {
    final TraversalPath path = createPath();
    final TraversalPath pathWithDel = path.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> false);

    // Modifying original path after withDeleted does not affect pathWithDel
    path.addStep(cd, d);

    assertThat(path.length()).isEqualTo(3);
    assertThat(path.getVertices()).hasSize(4);

    assertThat(pathWithDel.length()).isEqualTo(2);
    assertThat(pathWithDel.getVertices()).hasSize(3);
    assertThat(pathWithDel.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE, c);
  }

  @Test
  void testSubPathPreservesDeletedMarkers() {
    final TraversalPath path = createPath();
    final TraversalPath pathWithDel = path.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> e.getIdentity().equals(ab.getIdentity()));

    final TraversalPath sub0to1 = pathWithDel.subPath(0, 1);
    assertThat(sub0to1.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE);
    assertThat(sub0to1.getRelationshipsOrDeleted().get(0)).isInstanceOf(DeletedEntityMarker.class);

    final TraversalPath sub1to2 = pathWithDel.subPath(1, 2);
    assertThat(sub1to2.getNodesOrDeleted()).containsExactly(DeletedEntityMarker.INSTANCE, c);
    assertThat(sub1to2.getRelationshipsOrDeleted()).containsExactly(bc);
  }

  @Test
  void testReversedPreservesDeletedMarkers() {
    final TraversalPath path = createPath();
    final TraversalPath pathWithDel = path.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> e.getIdentity().equals(ab.getIdentity()));

    final TraversalPath reversed = pathWithDel.reversed();
    assertThat(reversed.getNodesOrDeleted()).containsExactly(c, DeletedEntityMarker.INSTANCE, a);
    final List<Object> revRels = reversed.getRelationshipsOrDeleted();
    assertThat(revRels.get(0)).isSameAs(bc);
    assertThat(revRels.get(1)).isInstanceOf(DeletedEntityMarker.class);
  }

  @Test
  void testToAlternatingListWithDeletedMarkers() {
    final TraversalPath path = createPath();
    final TraversalPath pathWithDel = path.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> e.getIdentity().equals(bc.getIdentity()));

    final List<Object> alt = pathWithDel.toAlternatingList();
    assertThat(alt).hasSize(5);
    assertThat(alt.get(0)).isSameAs(a);
    assertThat(alt.get(1)).isSameAs(ab);
    assertThat(alt.get(2)).isInstanceOf(DeletedEntityMarker.class);
    assertThat(alt.get(3)).isInstanceOf(DeletedEntityMarker.class);
    assertThat(alt.get(4)).isSameAs(c);
  }

  @Test
  void testToMapAndGetMember() {
    final TraversalPath path = createPath();
    final TraversalPath pathWithDel = path.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> false);

    final Map<String, Object> map = pathWithDel.toMap();
    assertThat(map.get(TraversalPath.TYPE_KEY)).isEqualTo(TraversalPath.PATH_TYPE);
    assertThat(map.get(TraversalPath.LENGTH_KEY)).isEqualTo(2);

    @SuppressWarnings("unchecked")
    final List<Object> nodes = (List<Object>) map.get(TraversalPath.NODES_KEY);
    assertThat(nodes).containsExactly(a, DeletedEntityMarker.INSTANCE, c);

    @SuppressWarnings("unchecked")
    final List<Object> memberNodes = (List<Object>) pathWithDel.getMember(TraversalPath.NODES_KEY);
    assertThat(memberNodes).containsExactly(a, DeletedEntityMarker.INSTANCE, c);
    assertThat(pathWithDel.getMember(TraversalPath.LENGTH_KEY)).isEqualTo(2);
    assertThat(pathWithDel.getMember(TraversalPath.TYPE_KEY)).isEqualTo(TraversalPath.PATH_TYPE);
  }

  @Test
  void testEqualsAndHashCodeRemainEqualBeforeAndAfterMarkingDeleted() {
    final TraversalPath path = createPath();
    final TraversalPath pathWithDel = path.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> e.getIdentity().equals(ab.getIdentity()));

    assertThat(pathWithDel).isEqualTo(path);
    assertThat(path).isEqualTo(pathWithDel);
    assertThat(pathWithDel.hashCode()).isEqualTo(path.hashCode());
  }

  @Test
  void testConstructorsPreserveDeletedMarkers() {
    final TraversalPath path = createPath();
    final TraversalPath pathWithDel = path.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> false);

    // Constructor extending by edge and vertex
    final TraversalPath extended = new TraversalPath(pathWithDel, cd, d);
    assertThat(extended.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE, c, d);
    assertThat(extended.getRelationshipsOrDeleted()).containsExactly(ab, bc, cd);

    // Constructor extending by another path
    final TraversalPath extPath = new TraversalPath(c);
    extPath.addStep(cd, d);
    final TraversalPath extPathWithDel = extPath.withDeleted(v -> v.getIdentity().equals(d.getIdentity()), e -> false);

    final TraversalPath concatenated = new TraversalPath(pathWithDel, extPathWithDel);
    assertThat(concatenated.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE, c, DeletedEntityMarker.INSTANCE);

    // Constructor extending by another path where the shared join vertex is marked deleted in extension
    final TraversalPath extPath2 = new TraversalPath(c);
    extPath2.addStep(cd, d);
    final TraversalPath extPath2WithDel = extPath2.withDeleted(v -> v.getIdentity().equals(c.getIdentity()), e -> false);
    final TraversalPath concatenatedJoinDeleted = new TraversalPath(path, extPath2WithDel);
    assertThat(concatenatedJoinDeleted.getNodesOrDeleted()).containsExactly(a, b, DeletedEntityMarker.INSTANCE, d);
  }

  @Test
  void testAddStepThrowsOnMarkedPath() {
    final TraversalPath path = createPath();
    final TraversalPath markedVertices = path.withDeleted(v -> v.getIdentity().equals(b.getIdentity()), e -> false);
    assertThatThrownBy(() -> markedVertices.addStep(cd, d))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot call addStep() on a TraversalPath carrying deleted entity markers");

    final TraversalPath markedEdges = path.withDeleted(v -> false, e -> e.getIdentity().equals(ab.getIdentity()));
    assertThatThrownBy(() -> markedEdges.addStep(cd, d))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot call addStep() on a TraversalPath carrying deleted entity markers");
  }
}
