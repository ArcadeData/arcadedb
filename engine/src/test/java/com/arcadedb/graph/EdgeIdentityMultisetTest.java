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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The removable edge set a backtracking search holds its walked edges in (issue #9983): parallel lightweight twins
 * share their RID and must still be held, tested and released one by one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EdgeIdentityMultisetTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
  }

  private List<RID> edges(final RID vertex, final Vertex.DIRECTION direction) {
    final List<RID> found = new ArrayList<>();
    for (final Edge edge : database.lookupByRID(vertex, true).asVertex().getEdges(direction, "K"))
      found.add(edge.getIdentity());
    return found;
  }

  @Test
  void twinsAreHeldAndReleasedOneByOne() {
    final RID[] ids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      a.newLightEdge("K", b);
      a.newLightEdge("K", b);
      ids[0] = a.getIdentity();
      ids[1] = b.getIdentity();
    });

    final List<RID> out = edges(ids[0], Vertex.DIRECTION.OUT);
    final List<RID> in = edges(ids[1], Vertex.DIRECTION.IN);
    assertThat(out).hasSize(2);
    assertThat(out.get(0)).isEqualTo(out.get(1)); // the RID cannot tell the twins apart

    final EdgeIdentityMultiset set = new EdgeIdentityMultiset();
    assertThat(set.isEmpty()).isTrue();
    set.add(out.get(0));
    assertThat(set.contains(out.get(0))).isTrue();
    assertThat(set.contains(out.get(1))).isFalse();
    // the same edge read off the other endpoint's list is the same edge
    assertThat(set.contains(in.get(0))).isTrue();
    assertThat(set.contains(in.get(1))).isFalse();

    set.add(out.get(1));
    assertThat(set.contains(in.get(1))).isTrue();

    // removing a twin that is not held removes nothing; removing each held one releases only it
    final EdgeIdentityMultiset single = new EdgeIdentityMultiset();
    single.add(out.get(0));
    assertThat(single.remove(out.get(1))).isFalse();
    assertThat(single.contains(out.get(0))).isTrue();

    assertThat(set.remove(out.get(0))).isTrue();
    assertThat(set.contains(out.get(0))).isFalse();
    assertThat(set.contains(out.get(1))).isTrue();
    assertThat(set.remove(in.get(1))).isTrue();
    assertThat(set.isEmpty()).isTrue();
    assertThat(set.remove(out.get(1))).isFalse();
  }

  @Test
  void threeTwinsReleasedInAnotherOrderThroughTheOtherEndpoint() {
    final RID[] ids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      for (int i = 0; i < 3; i++)
        a.newLightEdge("K", b);
      ids[0] = a.getIdentity();
      ids[1] = b.getIdentity();
    });

    final List<RID> out = edges(ids[0], Vertex.DIRECTION.OUT);
    final List<RID> in = edges(ids[1], Vertex.DIRECTION.IN);
    assertThat(out).hasSize(3);

    final EdgeIdentityMultiset set = new EdgeIdentityMultiset();
    for (final RID edge : out)
      set.add(edge);
    for (final RID edge : in)
      assertThat(set.contains(edge)).isTrue();

    // released by objects other than the ones added (the same edges read off the target's list), middle one first
    assertThat(set.remove(in.get(1))).isTrue();
    assertThat(set.contains(out.get(1))).isFalse();
    assertThat(set.contains(out.get(0))).isTrue();
    assertThat(set.contains(out.get(2))).isTrue();
    assertThat(set.remove(in.get(1))).isFalse();

    assertThat(set.remove(in.get(2))).isTrue();
    assertThat(set.contains(out.get(2))).isFalse();
    assertThat(set.contains(out.get(0))).isTrue();

    assertThat(set.remove(in.get(0))).isTrue();
    assertThat(set.isEmpty()).isTrue();
  }

  @Test
  void recordBackedEdgesAreAMultiset() {
    final RID[] edge = new RID[1];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      edge[0] = a.newEdge("K", b).getIdentity();
    });

    final EdgeIdentityMultiset set = new EdgeIdentityMultiset();
    set.add(edge[0]);
    set.add(edge[0]);
    assertThat(set.remove(edge[0])).isTrue();
    // added twice, so still held after one removal
    assertThat(set.contains(edge[0])).isTrue();
    assertThat(set.remove(new RID(edge[0].getBucketId(), edge[0].getPosition()))).isTrue();
    assertThat(set.contains(edge[0])).isFalse();
    assertThat(set.isEmpty()).isTrue();
  }
}
