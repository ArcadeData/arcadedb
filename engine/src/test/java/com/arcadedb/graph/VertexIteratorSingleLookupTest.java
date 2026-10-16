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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.utility.ResettableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A vertex or edge walk looks each record up once. {@code IteratorFilterBase.hasNext()} resolves the RID of the next
 * neighbour to validate it, and {@code VertexIteratorFilter.next()} used to resolve the same RID again: two read-locked
 * lookups, each with a transaction cache probe and a type lookup, for every vertex a traversal visits. The record the
 * validation resolved is now the one handed out. {@code EdgeIteratorFilter.next()} had the same double lookup on the edge
 * record and was left out of the first fix (#10163), which is why both iterators are measured from one list here.
 */
class VertexIteratorSingleLookupTest extends TestHelper {
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
        hub.newEdge(i % 5 == 0 ? "Hates" : "Likes", leaf);
      }
    });
  }

  private long reads() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }

  /** The two filtered iterators over one adjacency list: both validate the entry in hasNext() and hand it out in next(). */
  enum Walk {
    VERTICES(hub -> hub.getVertices(Vertex.DIRECTION.OUT, "Likes", "Hates").iterator()),
    EDGES(hub -> hub.getEdges(Vertex.DIRECTION.OUT, "Likes", "Hates").iterator());

    private final Function<Vertex, Iterator<? extends Identifiable>> iterator;

    Walk(final Function<Vertex, Iterator<? extends Identifiable>> iterator) {
      this.iterator = iterator;
    }
  }

  @ParameterizedTest
  @EnumSource(Walk.class)
  void aWalkLooksEachEntryUpOnce(final Walk walk) {
    final Vertex hub = source.asVertex();
    final long before = reads();
    final Iterator<? extends Identifiable> it = walk.iterator.apply(hub);
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    final long lookups = reads() - before;

    assertThat(count).isEqualTo(NEIGHBORS);
    // ONE LOOKUP PER ENTRY, PLUS THE EDGE-LIST CHUNKS: NEVER THE TWO PER ENTRY OF THE VALIDATE-THEN-RELOAD PATTERN
    assertThat(lookups).as("lookups").isLessThan(NEIGHBORS + NEIGHBORS / 4);
  }

  @Test
  void theHandedOutEdgeIsTheOneBehindTheEntry() {
    final Set<Integer> seen = new HashSet<>();
    int hates = 0;
    for (final Edge e : source.asVertex().getEdges(Vertex.DIRECTION.OUT, "Likes", "Hates")) {
      assertThat(e.getOut()).isEqualTo(source);
      final Vertex leaf = e.getInVertex();
      assertThat(seen.add(leaf.getInteger("n"))).isTrue();
      if (e.getTypeName().equals("Hates")) {
        assertThat(leaf.getInteger("n") % 5).isEqualTo(0);
        hates++;
      }
    }
    assertThat(seen).hasSize(NEIGHBORS);
    assertThat(hates).isEqualTo(NEIGHBORS / 5);
  }

  @Test
  void repeatedHasNextAndPlainNextHandOutEachEdgeOnce() {
    final Iterator<Edge> it = source.asVertex().getEdges(Vertex.DIRECTION.OUT, "Likes").iterator();
    final Set<RID> seen = new HashSet<>();
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
  void theHandedOutVertexIsTheOneBehindTheEntry() {
    final Set<Integer> seen = new HashSet<>();
    for (final Vertex v : source.asVertex().getVertices(Vertex.DIRECTION.OUT, "Likes", "Hates")) {
      assertThat(v.getTypeName()).isEqualTo("Leaf");
      assertThat(seen.add(v.getInteger("n"))).isTrue();
    }
    assertThat(seen).hasSize(NEIGHBORS);
  }

  @Test
  void aTypeFilterStillSkipsTheOtherEdgeTypes() {
    final List<Integer> hates = new ArrayList<>();
    for (final Vertex v : source.asVertex().getVertices(Vertex.DIRECTION.OUT, "Hates"))
      hates.add(v.getInteger("n"));
    assertThat(hates).hasSize(NEIGHBORS / 5).allMatch(n -> n % 5 == 0);
  }

  @Test
  void repeatedHasNextAndPlainNextHandOutEachNeighbourOnce() {
    final Iterator<Vertex> it = source.asVertex().getVertices(Vertex.DIRECTION.OUT, "Likes").iterator();
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

  /** A reset forgets the entry hasNext() peeked: the walk restarts from the first entry and hands each one out once. */
  @ParameterizedTest
  @EnumSource(Walk.class)
  void aResetAfterAPeekWalksEveryEntryOnce(final Walk walk) {
    final Iterator<? extends Identifiable> it = walk.iterator.apply(source.asVertex());
    it.next();
    assertThat(it.hasNext()).isTrue();
    ((ResettableIterator<?>) it).reset();

    final Set<RID> seen = new HashSet<>();
    while (it.hasNext())
      assertThat(seen.add(it.next().getIdentity())).isTrue();
    assertThat(seen).hasSize(NEIGHBORS);
  }

  @Test
  void theIncomingSideReturnsTheHub() {
    final RID leaf = source.asVertex().getVertices(Vertex.DIRECTION.OUT, "Likes", "Hates").iterator().next().getIdentity();
    final Iterator<Vertex> in = leaf.asVertex().getVertices(Vertex.DIRECTION.IN, "Likes", "Hates").iterator();
    assertThat(in.hasNext()).isTrue();
    assertThat(in.next().getIdentity()).isEqualTo(source);
    assertThat(in.hasNext()).isFalse();
  }
}
