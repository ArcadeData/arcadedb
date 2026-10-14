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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
      walking.readFrom(originSegment(single), originPosition(single), Vertex.DIRECTION.OUT, -1, null);
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
   * A twin added after the list was counted: an edge read off the list at a position the census recorded for another
   * entry, or for none, is not answered from the census - it would be told it has no twin and taken for the entry the
   * census counted. It walks the list as it is now.
   */
  @Test
  @SuppressWarnings("deprecation")
  void anEntryAddedAfterTheCountIsNotAnsweredFromIt() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      a.newLightEdge("K", b);
      for (int i = 0; i < 4; i++)
        a.newLightEdge("K", database.newVertex("P").save());
      hub = a.getIdentity();
      twinTarget = b.getIdentity();
    });
    final List<LightEdgeRID> before = read();
    for (final LightEdgeRID edge : before)
      edge.getOccurrence();
    final LightEdgeCensus census = before.getFirst().getCensus();
    assertThat(census.isBuilt()).isTrue();
    assertThat(census.hasTwins()).isFalse();

    // a twin of the a->b edge, written after the count
    database.transaction(() -> database.lookupByRID(hub, true).asVertex().modify().newLightEdge("K",
        database.lookupByRID(twinTarget, true).asVertex()));

    // every edge of the list as it is now, asked through the census of the old read
    final List<LightEdgeRID> twins = new ArrayList<>();
    for (final LightEdgeRID now : read())
      if (now.getInRID().equals(twinTarget)) {
        final LightEdgeRID throughOldCensus = new LightEdgeRID(database, now.getBucketId(), now.getOutRID(), now.getInRID());
        throughOldCensus.readFrom(now.getOriginSegment(), now.getOriginPosition(), Vertex.DIRECTION.OUT,
            now.getOriginChunkUsed(), census);
        twins.add(throughOldCensus);
      }
    assertThat(twins).hasSize(2);
    for (final LightEdgeRID twin : twins)
      assertThat(twin.getTwinCount()).isEqualTo(2);
    assertThat(LightEdgeRID.isSameEdge(twins.get(0), twins.get(1))).isFalse();
    assertThat(twins.get(0).getOccurrence()).isNotEqualTo(twins.get(1).getOccurrence());
  }

  /**
   * A twin removed after the count shifts the entries after it back, so another twin of the same triple can sit at a
   * position the census counted. An edge read off the chunk as it is now carries the chunk's new size and is not answered
   * from the census: it walks the list, and the two twins left are told apart.
   */
  @Test
  void aTwinShiftedOntoACountedPositionIsNotAnsweredFromTheCount() {
    buildTwins();
    final List<LightEdgeRID> before = read();
    for (final LightEdgeRID edge : before)
      edge.getOccurrence();
    final LightEdgeCensus census = before.getFirst().getCensus();
    assertThat(census.hasTwins()).isTrue();

    database.transaction(() -> {
      for (final Edge edge : database.lookupByRID(hub, true).asVertex().getEdges(Vertex.DIRECTION.OUT))
        if (edge.getIn().equals(twinTarget)) {
          edge.delete();
          break;
        }
    });

    final List<LightEdgeRID> twins = new ArrayList<>();
    for (final LightEdgeRID now : read())
      if (now.getInRID().equals(twinTarget)) {
        final LightEdgeRID throughOldCensus = new LightEdgeRID(database, now.getBucketId(), now.getOutRID(), now.getInRID());
        throughOldCensus.readFrom(now.getOriginSegment(), now.getOriginPosition(), Vertex.DIRECTION.OUT,
            now.getOriginChunkUsed(), census);
        twins.add(throughOldCensus);
      }
    assertThat(twins).hasSize(2);
    final Set<Integer> occurrences = new HashSet<>();
    for (final LightEdgeRID twin : twins) {
      assertThat(twin.getTwinCount()).isEqualTo(2);
      occurrences.add(twin.getOccurrence());
    }
    assertThat(occurrences).containsExactlyInAnyOrder(0, 1);
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
    assertThat(census.resolve(edge, edge.getOriginSegment(), edge.getOriginPosition(), edge.getOriginChunkUsed())).isFalse();
    assertThat(census.resolve(edge, edge.getOriginSegment(), edge.getOriginPosition(), edge.getOriginChunkUsed())).isFalse();
    assertThat(census.hasFailed()).isTrue();
    assertThat(census.isBuilt()).isFalse();
    assertThat(census.resolve(edge, edge.getOriginSegment(), edge.getOriginPosition(), edge.getOriginChunkUsed())).isFalse();
    assertThat(census.hasFailed()).isTrue();
  }

  /**
   * {@link LightEdgeRID#isSameEdge} takes shortcuts from whatever each side has resolved: on random twin layouts, read
   * from both ends and compared in random order, it must agree with the occurrences counted by hand off the lists.
   */
  @Test
  @SuppressWarnings("deprecation")
  void isSameEdgeAgreesWithTheOccurrencesOnRandomTwinLayouts() {
    for (long seed = 0; seed < 10; seed++) {
      final Random random = new Random(seed);
      final RID[] neighbors = new RID[4];
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").save();
        final MutableVertex[] targets = new MutableVertex[neighbors.length];
        for (int i = 0; i < targets.length; i++) {
          targets[i] = database.newVertex("P").save();
          neighbors[i] = targets[i].getIdentity();
        }
        for (int i = 0; i < 24; i++)
          a.newLightEdge("K", targets[random.nextInt(targets.length)]);
        hub = a.getIdentity();
      });

      // the edges and the occurrence each has in the list it was read from, counted in iteration order
      final List<LightEdgeRID> edges = new ArrayList<>();
      final List<Integer> truth = new ArrayList<>();
      final Map<RID, Integer> outSeen = new HashMap<>();
      for (final LightEdgeRID edge : read()) {
        edges.add(edge);
        truth.add(outSeen.merge(edge.getInRID(), 1, Integer::sum) - 1);
      }
      for (final RID neighbor : neighbors) {
        int inSeen = 0;
        for (final Edge edge : database.lookupByRID(neighbor, true).asVertex().getEdges(Vertex.DIRECTION.IN)) {
          edges.add((LightEdgeRID) edge.getIdentity());
          truth.add(inSeen++);
        }
      }

      final List<int[]> pairs = new ArrayList<>();
      for (int i = 0; i < edges.size(); i++)
        for (int j = 0; j < edges.size(); j++)
          if (i != j && edges.get(i).equals(edges.get(j)))
            pairs.add(new int[] { i, j });
      Collections.shuffle(pairs, random);
      for (final int[] pair : pairs) {
        final boolean expected = truth.get(pair[0]).equals(truth.get(pair[1]));
        // both orders, asked back to back: whichever side the first call resolved, the second one agrees
        assertThat(LightEdgeRID.isSameEdge(edges.get(pair[0]), edges.get(pair[1])))
            .as("seed %d, %d vs %d", seed, pair[0], pair[1]).isEqualTo(expected);
        assertThat(LightEdgeRID.isSameEdge(edges.get(pair[1]), edges.get(pair[0])))
            .as("seed %d, %d vs %d", seed, pair[1], pair[0]).isEqualTo(expected);
      }
    }
  }

  /** The rows of one iteration resolved by many threads at once: one census, the numbering of a single thread. */
  @Test
  @SuppressWarnings("deprecation")
  void concurrentResolutionsShareOneCensus() throws Exception {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      for (int i = 0; i < 200; i++)
        a.newLightEdge("K", i % 10 == 0 ? b : database.newVertex("P").save());
      hub = a.getIdentity();
    });
    final List<Integer> expected = new ArrayList<>();
    for (final LightEdgeRID edge : read())
      expected.add(edge.getOccurrence());

    final List<LightEdgeRID> edges = read();
    final ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < 8; t++) {
        final long seed = t;
        futures.add(pool.submit(() -> {
          final List<LightEdgeRID> shuffled = new ArrayList<>(edges);
          Collections.shuffle(shuffled, new Random(seed));
          for (final LightEdgeRID edge : shuffled)
            edge.getOccurrence();
        }));
      }
      for (final Future<?> future : futures)
        future.get(60, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    for (int i = 0; i < edges.size(); i++)
      assertThat(edges.get(i).getOccurrence()).as("edge %d", i).isEqualTo(expected.get(i));
    assertThat(edges.getFirst().getCensus().isBuilt()).isTrue();
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
