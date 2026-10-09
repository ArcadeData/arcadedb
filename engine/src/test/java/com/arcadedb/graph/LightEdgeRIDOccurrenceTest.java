/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Occurrence of a lightweight edge among the entries sharing its (type, out, in) triple (issue #9573).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LightEdgeRIDOccurrenceTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
  }

  private List<LightEdgeRID> identities(final RID vertex, final Vertex.DIRECTION direction) {
    final List<LightEdgeRID> out = new ArrayList<>();
    final Iterator<Edge> it = database.lookupByRID(vertex, true).asVertex().getEdges(direction, "K").iterator();
    while (it.hasNext())
      out.add((LightEdgeRID) it.next().getIdentity());
    return out;
  }

  @Test
  void twinsAreNumberedAlikeFromBothEndsAndLoneEdgesHaveNoTwin() {
    final RID[] ids = new RID[3];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      final MutableVertex c = database.newVertex("P").save();
      a.newLightEdge("K", b);
      a.newLightEdge("K", b);
      a.newLightEdge("K", c);
      ids[0] = a.getIdentity();
      ids[1] = b.getIdentity();
      ids[2] = c.getIdentity();
    });

    final List<LightEdgeRID> fromSource = identities(ids[0], Vertex.DIRECTION.OUT);
    assertThat(fromSource).hasSize(3);
    final List<LightEdgeRID> toB = new ArrayList<>();
    final List<LightEdgeRID> toC = new ArrayList<>();
    for (final LightEdgeRID r : fromSource)
      (r.getInRID().equals(ids[1]) ? toB : toC).add(r);

    assertThat(toC).hasSize(1);
    assertThat(toC.get(0).getTwinCount()).isEqualTo(1);
    assertThat(toB).hasSize(2);
    assertThat(toB.get(0).getTwinCount()).isEqualTo(2);
    assertThat(toB.get(0).getOccurrence()).isNotEqualTo(toB.get(1).getOccurrence());

    // read again from the target's incoming list: the same numbers, so a path walking the edge back is recognised
    final List<LightEdgeRID> fromTarget = identities(ids[1], Vertex.DIRECTION.IN);
    assertThat(fromTarget).hasSize(2);
    assertThat(fromTarget.get(0).getOccurrence()).isEqualTo(toB.get(0).getOccurrence());
    assertThat(LightEdgeRID.isSameEdge(toB.get(0), fromTarget.get(0))).isTrue();
    assertThat(LightEdgeRID.isSameEdge(toB.get(0), fromTarget.get(1))).isFalse();
    assertThat(LightEdgeRID.isSameEdge(toB.get(1), fromTarget.get(0))).isFalse();
    assertThat(LightEdgeRID.isSameEdge(toB.get(1), fromTarget.get(1))).isTrue();
  }

  @Test
  void theUnfilteredIteratorNumbersTwinsToo() {
    final RID[] ids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      a.newLightEdge("K", b);
      a.newLightEdge("K", b);
      ids[0] = a.getIdentity();
      ids[1] = b.getIdentity();
    });
    final List<LightEdgeRID> out = new ArrayList<>();
    for (final Edge edge : database.lookupByRID(ids[0], true).asVertex().getEdges(Vertex.DIRECTION.OUT))
      out.add((LightEdgeRID) edge.getIdentity());
    final List<LightEdgeRID> in = new ArrayList<>();
    for (final Edge edge : database.lookupByRID(ids[1], true).asVertex().getEdges(Vertex.DIRECTION.IN))
      in.add((LightEdgeRID) edge.getIdentity());
    assertThat(out).hasSize(2);
    assertThat(in).hasSize(2);
    assertThat(out.get(0).getTwinCount()).isEqualTo(2);
    assertThat(out.get(0).getOccurrence()).isNotEqualTo(out.get(1).getOccurrence());
    for (final LightEdgeRID fromSource : out)
      assertThat(in.stream().filter(fromTarget -> LightEdgeRID.isSameEdge(fromSource, fromTarget)).count()).isEqualTo(1L);
  }

  @Test
  void anEntryThatIsNoLongerInTheListKeepsTheTripleIdentity() {
    final RID[] ids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      a.newLightEdge("K", b);
      a.newLightEdge("K", b);
      ids[0] = a.getIdentity();
      ids[1] = b.getIdentity();
    });
    final List<LightEdgeRID> before = identities(ids[0], Vertex.DIRECTION.OUT);
    final LightEdgeRID first = before.get(0);
    final LightEdgeRID second = before.get(1);

    // the list changes under the query: both copies go
    database.transaction(() -> {
      final Iterator<Edge> it = database.lookupByRID(ids[0], true).asVertex().getEdges(Vertex.DIRECTION.OUT, "K").iterator();
      while (it.hasNext()) {
        it.next();
        it.remove();
      }
    });

    assertThat(second.getTwinCount()).isEqualTo(0);
    assertThat(second.getOccurrence()).isEqualTo(0);
    // unresolvable: falls back to the triple identity, as before the occurrence existed
    assertThat(LightEdgeRID.isSameEdge(first, second)).isTrue();
  }

  @Test
  @Tag("slow")
  void aHubWithManyLightEdgesStillAnswers() {
    final int degree = 20_000;
    final RID[] hub = new RID[1];
    database.transaction(() -> {
      final MutableVertex h = database.newVertex("P").save();
      for (int i = 0; i < degree; i++)
        h.newLightEdge("K", database.newVertex("P").save());
      // two twins on the striped list of the hub
      final MutableVertex twinTarget = database.newVertex("P").save();
      h.newLightEdge("K", twinTarget);
      h.newLightEdge("K", twinTarget);
      hub[0] = h.getIdentity();
    });
    final List<LightEdgeRID> hubEdges = identities(hub[0], Vertex.DIRECTION.OUT);
    assertThat(hubEdges).hasSize(degree + 2);
    final RID twinTargetRID = hubEdges.stream().map(LightEdgeRID::getInRID)
        .filter(rid -> hubEdges.stream().filter(e -> e.getInRID().equals(rid)).count() == 2).findFirst().orElseThrow();
    final List<LightEdgeRID> twins = hubEdges.stream().filter(e -> e.getInRID().equals(twinTargetRID)).toList();
    assertThat(twins.get(0).getTwinCount()).isEqualTo(2);
    assertThat(twins.get(0).getOccurrence()).isNotEqualTo(twins.get(1).getOccurrence());
    final List<LightEdgeRID> incoming = identities(twinTargetRID, Vertex.DIRECTION.IN);
    assertThat(incoming).hasSize(2);
    // read from the striped list of the hub and from the plain list of the target, each copy is the same relationship as
    // exactly one copy on the other side
    for (final LightEdgeRID fromHub : twins)
      assertThat(incoming.stream().filter(fromTarget -> LightEdgeRID.isSameEdge(fromHub, fromTarget)).count()).isEqualTo(1L);
    // a leaf reaches the hub and walks back over the same edge: it must not be mistaken for a twin
    final List<LightEdgeRID> out = hubEdges.stream().filter(e -> !e.getInRID().equals(twinTargetRID)).toList();
    assertThat(out).hasSize(degree);
    final LightEdgeRID last = out.get(degree - 1);
    final List<LightEdgeRID> in = identities(last.getInRID(), Vertex.DIRECTION.IN);
    assertThat(in).hasSize(1);
    assertThat(LightEdgeRID.isSameEdge(last, in.get(0))).isTrue();
    assertThat(last.getTwinCount()).isEqualTo(1);
  }
}
