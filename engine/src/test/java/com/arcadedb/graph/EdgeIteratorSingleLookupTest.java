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
import com.arcadedb.exception.RecordNotFoundException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An edge walk looks the edge record up once. {@code IteratorFilterBase.hasNext()} resolves the RID of the next edge to
 * validate it, and {@code EdgeIteratorFilter.next()} used to resolve the same RID again: two read-locked lookups, each
 * with a transaction cache probe and a type lookup, for every edge a type-filtered {@code getEdges()} returns. The
 * record the validation resolved is now the one handed out.
 */
class EdgeIteratorSingleLookupTest extends TestHelper {
  private static final int NEIGHBORS = 500;

  private RID source;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub");
      database.getSchema().createVertexType("Leaf");
      database.getSchema().createEdgeType("Likes");
      database.getSchema().createEdgeType("Hates");
      final MutableVertex hub = database.newVertex("Hub").set("name", "hub").save();
      source = hub.getIdentity();
      for (int i = 0; i < NEIGHBORS; i++) {
        final MutableVertex leaf = database.newVertex("Leaf").set("n", i).save();
        hub.newEdge(i % 5 == 0 ? "Hates" : "Likes", leaf, "w", i);
      }
    });
  }

  private long reads() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }

  @Test
  void anEdgeWalkLooksEachEdgeUpOnce() {
    final Vertex hub = source.asVertex();
    final long before = reads();
    final Iterator<Edge> it = hub.getEdges(Vertex.DIRECTION.OUT, "Likes", "Hates").iterator();
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    final long lookups = reads() - before;

    assertThat(count).isEqualTo(NEIGHBORS);
    // ONE LOOKUP PER EDGE, PLUS THE EDGE-LIST CHUNKS: NEVER THE TWO PER EDGE OF THE VALIDATE-THEN-RELOAD PATTERN
    assertThat(lookups).as("lookups").isLessThan(NEIGHBORS + NEIGHBORS / 4);
  }

  @Test
  void theHandedOutEdgeIsTheOneBehindTheEntry() {
    final Set<Integer> seen = new HashSet<>();
    for (final Edge e : source.asVertex().getEdges(Vertex.DIRECTION.OUT, "Likes", "Hates")) {
      assertThat(e.getOut()).isEqualTo(source);
      assertThat(e.getIn().asVertex().getInteger("n")).isEqualTo(e.getInteger("w"));
      assertThat(e.getTypeName()).isEqualTo(e.getInteger("w") % 5 == 0 ? "Hates" : "Likes");
      assertThat(seen.add(e.getInteger("w"))).isTrue();
    }
    assertThat(seen).hasSize(NEIGHBORS);
  }

  @Test
  void aTypeFilterStillSkipsTheOtherEdgeTypes() {
    final List<Integer> hates = new ArrayList<>();
    for (final Edge e : source.asVertex().getEdges(Vertex.DIRECTION.OUT, "Hates"))
      hates.add(e.getInteger("w"));
    assertThat(hates).hasSize(NEIGHBORS / 5).allMatch(n -> n % 5 == 0);
  }

  @Test
  void repeatedHasNextAndPlainNextHandOutEachEdgeOnce() {
    final Iterator<Edge> it = source.asVertex().getEdges(Vertex.DIRECTION.OUT, "Likes").iterator();
    final Set<RID> seen = new HashSet<>();
    // NO hasNext() BEFORE THE FIRST next(), AND SEVERAL BEFORE THE OTHERS
    seen.add(it.next().getIdentity());
    int count = 1;
    while (it.hasNext()) {
      assertThat(it.hasNext()).isTrue();
      assertThat(seen.add(it.next().getIdentity())).isTrue();
      count++;
    }
    assertThat(count).isEqualTo(NEIGHBORS - NEIGHBORS / 5);
    assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void theIncomingSideReturnsTheEdgeOfTheHub() {
    final Edge out = source.asVertex().getEdges(Vertex.DIRECTION.OUT, "Likes", "Hates").iterator().next();
    final Iterator<Edge> in = out.getIn().asVertex().getEdges(Vertex.DIRECTION.IN, "Likes", "Hates").iterator();
    assertThat(in.hasNext()).isTrue();
    final Edge e = in.next();
    assertThat(e.getIdentity()).isEqualTo(out.getIdentity());
    assertThat(e.getOut()).isEqualTo(source);
    assertThat(in.hasNext()).isFalse();
  }

  @Test
  void anEdgeDeletedAfterHasNextStillFailsTheCaller() {
    // THE RECORD hasNext() KEPT MUST NOT HIDE A DELETE: WITH OR WITHOUT THE KEPT RECORD THE CALLER GETS RecordNotFoundException
    database.begin();
    try {
      final Vertex hub = source.asVertex();
      final Edge first = hub.getEdges(Vertex.DIRECTION.OUT, "Likes", "Hates").iterator().next();
      final Iterator<Edge> it = hub.getEdges(Vertex.DIRECTION.OUT, "Likes", "Hates").iterator();
      assertThat(it.hasNext()).isTrue();
      database.deleteRecord(first);
      assertThatThrownBy(() -> it.next().getInteger("w")).isInstanceOf(RecordNotFoundException.class);
    } finally {
      database.rollback();
    }
  }
}
