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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The census of the lightweight edges of one edge list (issue #10008): built on the second question of an iteration,
 * numbering twins as their own walk of the list would, skipping its second pass for a list without twins, and keeping
 * the numbering of the edges read before the list changed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LightEdgeCensusTest extends TestHelper {
  private RID hub;
  private RID twinTarget;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
  }

  @Test
  @SuppressWarnings("deprecation")
  void aListWithoutTwinsIsCountedOnceAndNeverNumbered() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      for (int i = 0; i < 10; i++)
        a.newLightEdge("K", database.newVertex("P").save());
      hub = a.getIdentity();
    });

    final List<LightEdgeRID> edges = read();
    assertThat(edges).hasSize(10);
    final LightEdgeCensus census = edges.getFirst().getCensus();
    assertThat(census).isNotNull();
    // one iteration, one census
    for (final LightEdgeRID edge : edges)
      assertThat(edge.getCensus()).isSameAs(census);

    // the first question walks alone, the second counts the list
    assertThat(edges.get(0).getTwinCount()).isEqualTo(1);
    assertThat(census.isBuilt()).isFalse();
    assertThat(edges.get(1).getTwinCount()).isEqualTo(1);
    assertThat(census.isBuilt()).isTrue();
    assertThat(census.hasTwins()).isFalse();
    for (final LightEdgeRID edge : edges)
      assertThat(edge.getOccurrence()).isZero();
  }

  @Test
  void twinsAreNumberedAsTheirOwnWalkNumbersThem() {
    buildTwins();
    final List<LightEdgeRID> censused = read();
    for (final LightEdgeRID edge : censused)
      edge.getOccurrence();
    assertThat(censused.getFirst().getCensus().hasTwins()).isTrue();

    // the same entries, each resolved by its own walk: a fresh iteration asked one question each, through rebuilt RIDs
    // that remember their origin but share no census
    final List<LightEdgeRID> alone = read();
    for (int i = 0; i < alone.size(); i++) {
      final LightEdgeRID single = alone.get(i);
      final LightEdgeRID walking = new LightEdgeRID(database, single.getBucketId(), single.getOutRID(), single.getInRID());
      walking.readFrom(originSegment(single), originPosition(single), Vertex.DIRECTION.OUT, null);
      assertThat(walking.getOccurrence()).as("entry %d", i).isEqualTo(censused.get(i).getOccurrence());
      assertThat(walking.getTwinCount()).isEqualTo(censused.get(i).getTwinCount());
    }

    final Set<Integer> occurrences = new HashSet<>();
    for (final LightEdgeRID edge : censused)
      if (edge.getInRID().equals(twinTarget)) {
        assertThat(edge.getTwinCount()).isEqualTo(3);
        occurrences.add(edge.getOccurrence());
      }
    assertThat(occurrences).containsExactlyInAnyOrder(0, 1, 2);
  }

  /**
   * Edges read before a twin is deleted keep the numbering of the list they were read from: their origin names the
   * entry in THAT list, and after the delete the entries behind it shift back, so a walk made now would find another
   * entry at the same position.
   */
  @Test
  void edgesReadBeforeTheListChangedKeepItsNumbering() {
    buildTwins();
    final List<LightEdgeRID> before = read();
    // two questions on edges without twins build the census, before the change
    int asked = 0;
    for (final LightEdgeRID edge : before)
      if (!edge.getInRID().equals(twinTarget) && asked++ < 2)
        edge.getOccurrence();
    assertThat(before.getFirst().getCensus().isBuilt()).isTrue();

    database.transaction(() -> {
      for (final Edge edge : database.lookupByRID(hub, true).asVertex().getEdges(Vertex.DIRECTION.OUT))
        if (edge.getIn().equals(twinTarget)) {
          edge.delete();
          break;
        }
    });

    final Set<Integer> occurrences = new HashSet<>();
    for (final LightEdgeRID edge : before)
      if (edge.getInRID().equals(twinTarget))
        occurrences.add(edge.getOccurrence());
    assertThat(occurrences).containsExactlyInAnyOrder(0, 1, 2);

    // a new iteration reads the list as it is now
    final Set<Integer> after = new HashSet<>();
    for (final LightEdgeRID edge : read())
      if (edge.getInRID().equals(twinTarget)) {
        assertThat(edge.getTwinCount()).isEqualTo(2);
        after.add(edge.getOccurrence());
      }
    assertThat(after).containsExactlyInAnyOrder(0, 1);
  }

  /**
   * The keys of a DISTINCT compare by occurrence, so a hash set of them holds one key per relationship whatever order the
   * keys arrive in and whatever each edge had resolved before: edges read off the source's list and off the target's,
   * and one rebuilt from its endpoints (no list, the first twin).
   */
  @Test
  void distinctKeysAreStableInEveryInsertionOrder() {
    buildTwins();
    for (long seed = 0; seed < 12; seed++) {
      final List<LightEdgeRID> edges = new ArrayList<>(read());
      for (final Edge edge : database.lookupByRID(twinTarget, true).asVertex().getEdges(Vertex.DIRECTION.IN))
        edges.add((LightEdgeRID) edge.getIdentity());
      final LightEdgeRID first = edges.getFirst();
      edges.add(new LightEdgeRID(database, first.getBucketId(), hub, twinTarget));
      // some resolved before they meet, some not
      for (int i = 0; i < edges.size(); i += 3)
        edges.get(i).getOccurrence();
      Collections.shuffle(edges, new Random(seed));

      final Set<Object> keys = new HashSet<>();
      for (final LightEdgeRID edge : edges)
        keys.add(edge.distinctKey());
      // 4 + 3 single edges, and 3 twins: the IN side and the rebuilt edge add none
      assertThat(keys).as("seed %d", seed).hasSize(10);
    }
  }

  /** A list that cannot be read is given up on once: every later edge walks alone instead of failing the same walk. */
  @Test
  void aCensusThatCannotCountTheListGivesUpOnce() {
    buildTwins();
    final LightEdgeRID edge = read().getFirst();
    final RID missing = database.newRID(hub.getBucketId(), 1_000_000L);
    final LightEdgeCensus census = new LightEdgeCensus((DatabaseInternal) database, missing, Vertex.DIRECTION.OUT);
    assertThat(census.resolve(edge, edge.getOriginSegment(), edge.getOriginPosition())).isFalse();
    assertThat(census.resolve(edge, edge.getOriginSegment(), edge.getOriginPosition())).isFalse();
    assertThat(census.hasFailed()).isTrue();
    assertThat(census.isBuilt()).isFalse();
    assertThat(census.resolve(edge, edge.getOriginSegment(), edge.getOriginPosition())).isFalse();
    assertThat(census.hasFailed()).isTrue();
  }

  @SuppressWarnings("deprecation")
  private void buildTwins() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      for (int i = 0; i < 4; i++)
        a.newLightEdge("K", database.newVertex("P").save());
      for (int i = 0; i < 3; i++) {
        a.newLightEdge("K", b);
        a.newLightEdge("K", database.newVertex("P").save());
      }
      hub = a.getIdentity();
      twinTarget = b.getIdentity();
    });
  }

  private List<LightEdgeRID> read() {
    final List<LightEdgeRID> edges = new ArrayList<>();
    for (final Edge edge : database.lookupByRID(hub, true).asVertex().getEdges(Vertex.DIRECTION.OUT))
      edges.add((LightEdgeRID) edge.getIdentity());
    return edges;
  }

  private static RID originSegment(final LightEdgeRID edge) {
    return edge.getOriginSegment();
  }

  private static int originPosition(final LightEdgeRID edge) {
    return edge.getOriginPosition();
  }
}
