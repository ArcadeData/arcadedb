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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.opencypher.executor.DeletedReferences.Deletions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the way a {@link TraversalPath} reads the nodes and relationships a statement deleted (issue #12548):
 * {@code withDeletions}, and what the derived paths ({@code subPath}, {@code reversed}, {@code withRecord}, the constructors)
 * keep of it.
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

  /** What a statement deleted, as a set of records the test fills in whenever it likes. */
  private static Deletions deleting(final Set<RID> deleted) {
    return (final Document record) -> deleted.contains(record.getIdentity());
  }

  private static Set<RID> rids(final Document... records) {
    final Set<RID> set = new HashSet<>();
    for (final Document record : records)
      set.add(record.getIdentity());
    return set;
  }

  @Test
  void aPathWithNoDeletionCheckReadsItsRecords() {
    final TraversalPath path = createPath();
    assertThat(path.getDeletions()).isNull();
    assertThat(path.getNodesOrDeleted()).containsExactly(a, b, c);
    assertThat(path.getRelationshipsOrDeleted()).containsExactly(ab, bc);
  }

  @Test
  void withDeletionsOfTheSameCheckReturnsThePathItself() {
    final TraversalPath path = createPath();
    final Deletions deletions = deleting(rids());
    final TraversalPath checked = path.withDeletions(deletions);

    assertThat(checked).isNotSameAs(path);
    assertThat(checked.withDeletions(deletions)).isSameAs(checked);
  }

  @Test
  void theCheckIsAskedWhenThePathIsRead() {
    final TraversalPath path = createPath();
    final Set<RID> deleted = rids();
    final TraversalPath checked = path.withDeletions(deleting(deleted));

    assertThat(checked.getNodesOrDeleted()).containsExactly(a, b, c);

    // Deleted after the path was handed on: read as deleted all the same
    deleted.add(b.getIdentity());
    deleted.add(ab.getIdentity());

    assertThat(checked.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE, c);
    final List<Object> relationships = checked.getRelationshipsOrDeleted();
    assertThat(relationships.get(0)).isInstanceOf(DeletedEntityMarker.class);
    assertThat(((DeletedEntityMarker) relationships.get(0)).getRelationshipType()).isEqualTo("E");
    assertThat(relationships.get(1)).isSameAs(bc);
  }

  @Test
  void theOriginalPathIsNotChangedByTheCopy() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids(b)));

    assertThat(path.getNodesOrDeleted()).containsExactly(a, b, c);
    assertThat(checked.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE, c);

    // The copy holds lists of its own: extending the original cannot change what the copy reads
    path.addStep(cd, d);
    assertThat(path.length()).isEqualTo(3);
    assertThat(checked.length()).isEqualTo(2);
    assertThat(checked.getVertices()).hasSize(3);
    assertThat(checked.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE, c);
  }

  @Test
  void twoChecksAreBothAsked() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids(a))).withDeletions(deleting(rids(c, bc)));

    assertThat(checked.getNodesOrDeleted()).containsExactly(DeletedEntityMarker.INSTANCE, b, DeletedEntityMarker.INSTANCE);
    final List<Object> relationships = checked.getRelationshipsOrDeleted();
    assertThat(relationships.get(0)).isSameAs(ab);
    assertThat(relationships.get(1)).isInstanceOf(DeletedEntityMarker.class);
  }

  @Test
  void subPathOfAPartiallyDeletedPathKeepsOnlyWhatItsPartDeleted() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids(b, ab)));

    // Only some entries of the whole path are deleted: each part reads its own
    final TraversalPath first = checked.subPath(0, 1);
    assertThat(first.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE);
    assertThat(first.getRelationshipsOrDeleted().get(0)).isInstanceOf(DeletedEntityMarker.class);

    final TraversalPath second = checked.subPath(1, 2);
    assertThat(second.getNodesOrDeleted()).containsExactly(DeletedEntityMarker.INSTANCE, c);
    assertThat(second.getRelationshipsOrDeleted()).containsExactly(bc);

    // A part that holds nothing deleted reads its records
    final TraversalPath lastNode = checked.subPath(2, 2);
    assertThat(lastNode.getNodesOrDeleted()).containsExactly(c);
    assertThat(lastNode.getRelationshipsOrDeleted()).isEmpty();
  }

  @Test
  void reversedOfAPartiallyDeletedPathReadsTheSameEntriesDeleted() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids(b, ab)));

    final TraversalPath reversed = checked.reversed();
    assertThat(reversed.getNodesOrDeleted()).containsExactly(c, DeletedEntityMarker.INSTANCE, a);
    final List<Object> relationships = reversed.getRelationshipsOrDeleted();
    assertThat(relationships.get(0)).isSameAs(bc);
    assertThat(relationships.get(1)).isInstanceOf(DeletedEntityMarker.class);
  }

  @Test
  void withRecordKeepsTheDeletionCheckAndReplacesOnlyTheMemberAskedFor() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids(b)));

    // The writer replaced the snapshot of a with a newer one of the same record
    final MutableVertex newerA = a.modify();
    final TraversalPath replaced = checked.withRecord(a.getIdentity(), newerA);

    assertThat(replaced).isNotSameAs(checked);
    assertThat(replaced.getDeletions()).isSameAs(checked.getDeletions());
    final List<Object> nodes = replaced.getNodesOrDeleted();
    assertThat(nodes.get(0)).isSameAs(newerA);
    assertThat(nodes.get(1)).isSameAs(DeletedEntityMarker.INSTANCE);
    assertThat(nodes.get(2)).isSameAs(c);
  }

  @Test
  void toAlternatingListMarksTheDeletedEntries() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids(b, bc)));

    final List<Object> alternating = checked.toAlternatingList();
    assertThat(alternating).hasSize(5);
    assertThat(alternating.get(0)).isSameAs(a);
    assertThat(alternating.get(1)).isSameAs(ab);
    assertThat(alternating.get(2)).isInstanceOf(DeletedEntityMarker.class);
    assertThat(alternating.get(3)).isInstanceOf(DeletedEntityMarker.class);
    assertThat(alternating.get(4)).isSameAs(c);
  }

  @Test
  @SuppressWarnings("unchecked")
  void toMapAndGetMemberMarkTheDeletedEntries() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids(b)));

    final Map<String, Object> map = checked.toMap();
    assertThat(map.get(TraversalPath.TYPE_KEY)).isEqualTo(TraversalPath.PATH_TYPE);
    assertThat(map.get(TraversalPath.LENGTH_KEY)).isEqualTo(2);
    assertThat((List<Object>) map.get(TraversalPath.NODES_KEY)).containsExactly(a, DeletedEntityMarker.INSTANCE, c);
    assertThat((List<Object>) map.get(TraversalPath.RELATIONSHIPS_KEY)).containsExactly(ab, bc);

    assertThat((List<Object>) checked.getMember(TraversalPath.NODES_KEY)).containsExactly(a, DeletedEntityMarker.INSTANCE, c);
    assertThat(checked.getMember(TraversalPath.LENGTH_KEY)).isEqualTo(2);
    assertThat(checked.getMember(TraversalPath.TYPE_KEY)).isEqualTo(TraversalPath.PATH_TYPE);
  }

  @Test
  void equalityAndHashCodeStayOnTheRecords() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids(b, ab)));

    assertThat(checked).isEqualTo(path);
    assertThat(path).isEqualTo(checked);
    assertThat(checked.hashCode()).isEqualTo(path.hashCode());
  }

  @Test
  void theConstructorsKeepTheDeletionCheck() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids(b, cd)));

    // Extending by an edge and a vertex
    final TraversalPath extended = new TraversalPath(checked, cd, d);
    assertThat(extended.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE, c, d);
    assertThat(extended.getRelationshipsOrDeleted().get(2)).isInstanceOf(DeletedEntityMarker.class);

    // Extending by another path, which carries a check of its own
    final TraversalPath tail = new TraversalPath(c);
    tail.addStep(cd, d);
    final TraversalPath checkedTail = tail.withDeletions(deleting(rids(d)));
    final TraversalPath concatenated = new TraversalPath(checked, checkedTail);
    assertThat(concatenated.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE, c, DeletedEntityMarker.INSTANCE);

    // Extending a path no DELETE saw by one that was
    final TraversalPath plainBase = new TraversalPath(new TraversalPath(a), ab, b);
    final TraversalPath joined = new TraversalPath(plainBase, new TraversalPath(b).withDeletions(deleting(rids(b))));
    assertThat(joined.getNodesOrDeleted()).containsExactly(a, DeletedEntityMarker.INSTANCE);
  }

  @Test
  void addStepIsRefusedOnAPathThatReadsDeletions() {
    final TraversalPath path = createPath();
    final TraversalPath checked = path.withDeletions(deleting(rids()));

    assertThatThrownBy(() -> checked.addStep(cd, d)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot call addStep() on a TraversalPath carrying deleted entity checks");
  }
}
