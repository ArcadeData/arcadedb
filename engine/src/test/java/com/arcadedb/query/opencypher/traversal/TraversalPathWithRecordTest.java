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
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10513: a path holds the records its MATCH read, and a later write of the same row produces another instance of them.
 * {@link TraversalPath#withRecord} is what puts the written instance in the path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TraversalPathWithRecordTest extends TestHelper {

  private Vertex   a;
  private Vertex   b;
  private Vertex   c;
  private Edge     ab;
  private Edge     bc;

  private TraversalPath path() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    final RID[] rids = new RID[5];
    database.transaction(() -> {
      final MutableVertex va = database.newVertex("V").set("n", "a").save();
      final MutableVertex vb = database.newVertex("V").set("n", "b").save();
      final MutableVertex vc = database.newVertex("V").set("n", "c").save();
      rids[0] = va.getIdentity();
      rids[1] = vb.getIdentity();
      rids[2] = vc.getIdentity();
      rids[3] = va.newEdge("E", vb).save().getIdentity();
      rids[4] = vb.newEdge("E", vc).save().getIdentity();
    });
    // read back after the commit, as a MATCH does: the records of a path are immutable snapshots, which modify() turns into
    // another instance (inside the transaction the lookup would answer with the mutable record the cache holds)
    a = database.lookupByRID(rids[0], true).asVertex();
    b = database.lookupByRID(rids[1], true).asVertex();
    c = database.lookupByRID(rids[2], true).asVertex();
    ab = database.lookupByRID(rids[3], true).asEdge();
    bc = database.lookupByRID(rids[4], true).asEdge();
    final TraversalPath path = new TraversalPath(a);
    path.addStep(ab, b);
    path.addStep(bc, c);
    return path;
  }

  @Test
  void aNodeStoredUnderTheIdentityIsReplaced() {
    final TraversalPath path = path();
    final MutableVertex written = b.modify().set("n", "B");

    final TraversalPath live = path.withRecord(b.getIdentity(), written);

    assertThat(live).isNotSameAs(path);
    assertThat(live.getVertices().get(1)).isSameAs(written);
    assertThat(live.getVertices().get(0)).isSameAs(a);
    assertThat(live.getVertices().get(2)).isSameAs(c);
    assertThat(live.getEdges()).containsExactly(ab, bc);
  }

  @Test
  void theOriginalPathIsNotModified() {
    final TraversalPath path = path();

    path.withRecord(b.getIdentity(), b.modify().set("n", "B"));

    assertThat(path.getVertices().get(1)).isSameAs(b);
  }

  @Test
  void aRelationshipStoredUnderTheIdentityIsReplaced() {
    final TraversalPath path = path();
    final MutableEdge written = bc.modify().set("w", 1);

    final TraversalPath live = path.withRecord(bc.getIdentity(), written);

    assertThat(live.getEdges().get(1)).isSameAs(written);
    assertThat(live.getEdges().get(0)).isSameAs(ab);
    assertThat(live.getVertices()).containsExactly(a, b, c);
  }

  @Test
  void aPathWithoutTheRecordIsReturnedAsIs() {
    final TraversalPath path = path();
    final MutableVertex[] other = new MutableVertex[1];
    database.transaction(() -> other[0] = database.newVertex("V").set("n", "x").save());

    assertThat(path.withRecord(other[0].getIdentity(), other[0])).isSameAs(path);
  }

  @Test
  void aMemberThatAlreadyIsTheReplacementIsLeftAlone() {
    final TraversalPath path = path();

    assertThat(path.withRecord(a.getIdentity(), a)).isSameAs(path);
  }

  @Test
  void aNodeVisitedTwiceIsReplacedEverywhere() {
    final TraversalPath path = path();
    path.addStep(bc, b);
    final MutableVertex written = b.modify().set("n", "B");

    final TraversalPath live = path.withRecord(b.getIdentity(), written);

    assertThat(live.getVertices().get(1)).isSameAs(written);
    assertThat(live.getVertices().get(3)).isSameAs(written);
    assertThat(live.getVertices().get(0)).isSameAs(a);
    assertThat(live.getVertices().get(2)).isSameAs(c);
  }

  @Test
  void aZeroLengthPathHoldsOneNode() {
    path();
    final TraversalPath single = new TraversalPath(a);
    final MutableVertex written = a.modify().set("n", "A");

    assertThat(single.withRecord(a.getIdentity(), written).getStartVertex()).isSameAs(written);
  }
}
