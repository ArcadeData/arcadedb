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
package com.arcadedb.remote;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12191: {@link RemoteVertex} paged the neighbourhood of a vertex with SKIP over a list that changes, so deleting the
 * edges (or the vertices) while iterating removed only part of them and a concurrent delete made it skip live edges.
 */
class Issue12191RemoteVertexPagingIT extends BaseGraphServerTest {
  private static final int LEAVES    = 4_500;
  private static final int PAGE_SIZE = 1_000;

  private RemoteDatabase database;

  @Override
  protected String getDatabaseName() {
    return "issue12191";
  }

  @BeforeEach
  void openAndPopulate() {
    database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    database.command("sql", "create vertex type V12191");
    database.command("sql", "create edge type E12191");
    database.command("sql", "create vertex V12191 set name = 'hub'");
    final StringBuilder leaves = new StringBuilder("INSERT INTO V12191 (name, i) VALUES ");
    for (int i = 0; i < LEAVES; i++)
      leaves.append(i > 0 ? ", " : "").append("('leaf', ").append(i).append(')');
    database.command("sql", leaves.toString());
    database.command("sql",
        "create edge E12191 from (select from V12191 where name = 'hub') to (select from V12191 where name = 'leaf')");
    assertThat(database.countType("E12191", true)).isEqualTo(LEAVES);
  }

  @AfterEach
  void closeDatabase() {
    if (database != null)
      database.close();
  }

  private Vertex hub() {
    return database.query("sql", "select from V12191 where name = 'hub'").next().getVertex().get();
  }

  @Test
  void deletingTheEdgesWhileIteratingDeletesThemAll() {
    int n = 0;
    for (final Edge e : hub().getEdges(Vertex.DIRECTION.OUT)) {
      e.delete();
      ++n;
    }
    assertThat(n).isEqualTo(LEAVES);
    assertThat(database.countType("E12191", true)).isZero();
  }

  @Test
  void deletingTheVerticesWhileIteratingDeletesThemAll() {
    int n = 0;
    for (final Vertex v : hub().getVertices(Vertex.DIRECTION.OUT)) {
      assertThat(v.getString("name")).isEqualTo("leaf");
      v.delete();
      ++n;
    }
    assertThat(n).isEqualTo(LEAVES);
    assertThat(database.countType("V12191", true)).isEqualTo(1);
  }

  @Test
  void edgesDeletedByAnotherClientDoNotHideLiveOnes() {
    final RemoteVertex remoteHub = new RemoteVertex(hub(), database);
    final Iterator<Edge> it = remoteHub.getEdgesPaged(Vertex.DIRECTION.OUT, PAGE_SIZE).iterator();

    final Set<RID> yielded = new HashSet<>();
    final List<RID> firstPage = new ArrayList<>();
    for (int i = 0; i < PAGE_SIZE; i++) {
      final RID rid = it.next().getIdentity();
      firstPage.add(rid);
      yielded.add(rid);
    }

    // another client deletes 100 edges this iteration already read
    final RemoteDatabase other = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      for (int i = 0; i < 100; i++)
        other.command("sql", "delete from " + firstPage.get(i));
    } finally {
      other.close();
    }

    while (it.hasNext())
      yielded.add(it.next().getIdentity());

    assertThat(yielded).hasSize(LEAVES);
    assertThat(database.countType("E12191", true)).isEqualTo(LEAVES - 100);
  }

  @Test
  void bothDirectionsAndTypeFilterYieldEveryEdgeOnce() {
    final Vertex hub = hub();
    final Set<RID> edges = new HashSet<>();
    int n = 0;
    for (final Edge e : hub.getEdges(Vertex.DIRECTION.BOTH, "E12191")) {
      edges.add(e.getIdentity());
      ++n;
    }
    assertThat(n).isEqualTo(LEAVES);
    assertThat(edges).hasSize(LEAVES);

    final Vertex leaf = database.query("sql", "select from V12191 where name = 'leaf' and i = 7").next().getVertex().get();
    final List<Vertex> back = new ArrayList<>();
    leaf.getVertices(Vertex.DIRECTION.IN).forEach(back::add);
    assertThat(back).extracting(Vertex::getIdentity).containsExactly(hub.getIdentity());
  }

  @Test
  void bothDirectionsReturnASelfLoopTwiceAsTheEmbeddedApiDoes() {
    database.command("sql", "create edge E12191 from (select from V12191 where name = 'hub') to (select from V12191 where name = 'hub')");
    final Vertex hub = hub();
    final RID hubRid = hub.getIdentity();

    // the embedded API reads the outgoing list, then the incoming one, so a self loop sits in both
    final int[] embedded = new int[2];
    final Database embeddedDatabase = getServerDatabase(0, getDatabaseName());
    embeddedDatabase.transaction(() -> {
      final Vertex embeddedHub = embeddedDatabase.lookupByRID(hubRid, true).asVertex();
      for (final Edge e : embeddedHub.getEdges(Vertex.DIRECTION.BOTH))
        if (e.getOut().equals(e.getIn()))
          ++embedded[0];
      for (final Vertex v : embeddedHub.getVertices(Vertex.DIRECTION.BOTH))
        if (v.getIdentity().equals(hubRid))
          ++embedded[1];
    });
    assertThat(embedded).containsExactly(2, 2);

    final RemoteVertex remoteHub = new RemoteVertex(hub, database);
    int selfLoopEdges = 0;
    int total = 0;
    for (final Edge e : remoteHub.getEdgesPaged(Vertex.DIRECTION.BOTH, PAGE_SIZE)) {
      if (e.getOut().equals(e.getIn()))
        ++selfLoopEdges;
      ++total;
    }
    int selfNeighbours = 0;
    for (final Vertex v : remoteHub.getVerticesPaged(Vertex.DIRECTION.BOTH, PAGE_SIZE))
      if (v.getIdentity().equals(hubRid))
        ++selfNeighbours;

    assertThat(selfLoopEdges).isEqualTo(embedded[0]);
    assertThat(selfNeighbours).isEqualTo(embedded[1]);
    assertThat(total).isEqualTo(LEAVES + 2);
  }

  @Test
  void lightweightEdgesArePagedWithoutRepeatingOrLosingAny() {
    database.command("sql", "create vertex V12191 set name = 'lightHub'");
    final Database embeddedDatabase = getServerDatabase(0, getDatabaseName());
    final RID[] hubRid = new RID[1];
    embeddedDatabase.transaction(() -> {
      final MutableVertex lightHub = embeddedDatabase.query("sql", "select from V12191 where name = 'lightHub'").next().getVertex()
          .get().modify();
      hubRid[0] = lightHub.getIdentity();
      for (int i = 0; i < 5; i++) {
        final MutableVertex target = embeddedDatabase.newVertex("V12191").set("name", "lightLeaf").save();
        lightHub.newLightEdge("E12191", target);
        // a twin: a second lightweight edge with the same type and endpoints
        if (i == 2)
          lightHub.newLightEdge("E12191", target);
      }
      // and record-backed edges after them
      for (int i = 0; i < 3; i++)
        lightHub.newEdge("E12191", embeddedDatabase.newVertex("V12191").set("name", "recordLeaf").save());
    });

    final RemoteVertex remoteHub = new RemoteVertex(database.lookupByRID(hubRid[0]).asVertex(), database);
    for (final int pageSize : new int[] { 1, 2, 4, 100 }) {
      int edges = 0;
      for (final Edge ignored : remoteHub.getEdgesPaged(Vertex.DIRECTION.OUT, pageSize))
        ++edges;
      final Map<String, Integer> neighbours = new HashMap<>();
      for (final Vertex v : remoteHub.getVerticesPaged(Vertex.DIRECTION.OUT, pageSize))
        neighbours.merge(v.getString("name"), 1, Integer::sum);

      assertThat(edges).as("edges at page size %d", pageSize).isEqualTo(9);
      assertThat(neighbours).as("neighbours at page size %d", pageSize).containsEntry("lightLeaf", 6).containsEntry("recordLeaf", 3);
    }
  }

  @Test
  void lightweightEdgesDeletedBetweenPagesDoNotHideLiveOnes() {
    // the lightweight edges of a bucket share one RID, so the cursor resumes by the far endpoint of the last one read (the list
    // is ordered by it): deleting an edge already read, as another client can, does not move what comes next
    final int pageSize = 2;
    for (final boolean vertices : new boolean[] { false, true }) {
      final LightweightHub fixture = createLightweightHub(Vertex.DIRECTION.OUT, -1);
      final RemoteVertex remoteHub = new RemoteVertex(database.lookupByRID(fixture.hub).asVertex(), database);

      final Iterator<?> it = vertices ?
          remoteHub.getVerticesPaged(Vertex.DIRECTION.OUT, pageSize).iterator() :
          remoteHub.getEdgesPaged(Vertex.DIRECTION.OUT, pageSize).iterator();
      final List<RID> read = new ArrayList<>();
      for (int i = 0; i < pageSize; i++)
        read.add(farEndpoint(it.next(), Vertex.DIRECTION.OUT));

      deleteLightweightEdge(fixture.hub, read.getFirst(), Vertex.DIRECTION.OUT);

      while (it.hasNext())
        read.add(farEndpoint(it.next(), Vertex.DIRECTION.OUT));

      assertThat(read).as("%s of the lightweight hub", vertices ? "vertices" : "edges")
          .containsExactlyInAnyOrderElementsOf(fixture.leaves);
    }
  }

  @Test
  void incomingLightweightEdgesDeletedBetweenPagesDoNotHideLiveOnes() {
    final int pageSize = 3;
    for (final boolean vertices : new boolean[] { false, true }) {
      final LightweightHub fixture = createLightweightHub(Vertex.DIRECTION.IN, -1);
      final RemoteVertex remoteHub = new RemoteVertex(database.lookupByRID(fixture.hub).asVertex(), database);

      final Iterator<?> it = vertices ?
          remoteHub.getVerticesPaged(Vertex.DIRECTION.IN, pageSize).iterator() :
          remoteHub.getEdgesPaged(Vertex.DIRECTION.IN, pageSize).iterator();
      final List<RID> read = new ArrayList<>();
      for (int i = 0; i < pageSize; i++)
        read.add(farEndpoint(it.next(), Vertex.DIRECTION.IN));

      deleteLightweightEdge(fixture.hub, read.getFirst(), Vertex.DIRECTION.IN);

      while (it.hasNext())
        read.add(farEndpoint(it.next(), Vertex.DIRECTION.IN));

      assertThat(read).as("%s of the lightweight hub", vertices ? "vertices" : "edges")
          .containsExactlyInAnyOrderElementsOf(fixture.leaves);
    }
  }

  @Test
  void aTwinLightweightEdgeIsNotSkippedWhenAnEdgeAlreadyReadIsDeleted() {
    // two lightweight edges of the same type and endpoints: the page ends between them, so the cursor skips the one already
    // read and still returns the other, whatever happens to the edges before them
    final int pageSize = 3;
    for (final boolean vertices : new boolean[] { false, true }) {
      final LightweightHub fixture = createLightweightHub(Vertex.DIRECTION.OUT, 2);
      final RemoteVertex remoteHub = new RemoteVertex(database.lookupByRID(fixture.hub).asVertex(), database);

      final Iterator<?> it = vertices ?
          remoteHub.getVerticesPaged(Vertex.DIRECTION.OUT, pageSize).iterator() :
          remoteHub.getEdgesPaged(Vertex.DIRECTION.OUT, pageSize).iterator();
      final List<RID> read = new ArrayList<>();
      for (int i = 0; i < pageSize; i++)
        read.add(farEndpoint(it.next(), Vertex.DIRECTION.OUT));

      // the third of the edges read is the first of the twins: the leaf before it is not
      deleteLightweightEdge(fixture.hub, read.getFirst(), Vertex.DIRECTION.OUT);

      while (it.hasNext())
        read.add(farEndpoint(it.next(), Vertex.DIRECTION.OUT));

      final List<RID> expected = new ArrayList<>(fixture.leaves);
      expected.add(fixture.leaves.get(2));
      assertThat(read).as("%s of the lightweight hub", vertices ? "vertices" : "edges")
          .containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  @Test
  void lightweightAndRecordEdgesOfSeveralTypesAreWalkedWholeAtEveryPageSize() {
    // lightweight and record-backed edges of two types, whose records sit in several buckets, with a twin, walked at page sizes
    // that end a page inside a bucket, at its end and across buckets: every edge is read once, in both directions
    database.command("sql", "create edge type E12191B");
    for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
      final MixedHub fixture = createMixedHub(direction);
      final RemoteVertex remoteHub = new RemoteVertex(database.lookupByRID(fixture.hub).asVertex(), database);

      for (final int pageSize : new int[] { 1, 2, 3, 5, 7, 100 }) {
        final List<String> edges = new ArrayList<>();
        for (final Edge e : remoteHub.getEdgesPaged(direction, pageSize))
          edges.add(describe(e, direction));
        final List<RID> neighbours = new ArrayList<>();
        for (final Vertex v : remoteHub.getVerticesPaged(direction, pageSize))
          neighbours.add(v.getIdentity());

        assertThat(edges).as("%s edges at page size %d", direction, pageSize).containsExactlyInAnyOrderElementsOf(fixture.edges);
        assertThat(neighbours).as("%s neighbours at page size %d", direction, pageSize)
            .containsExactlyInAnyOrderElementsOf(fixture.neighbours);
      }
    }
  }

  @Test
  void anEdgeAlreadyReadDeletedBetweenPagesDoesNotHideLiveOnesAmongSeveralTypes() {
    database.command("sql", "create edge type E12191B");
    for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
      for (final int pageSize : new int[] { 2, 3, 5, 7 }) {
        final MixedHub fixture = createMixedHub(direction);
        final RemoteVertex remoteHub = new RemoteVertex(database.lookupByRID(fixture.hub).asVertex(), database);

        final Iterator<Edge> it = remoteHub.getEdgesPaged(direction, pageSize).iterator();
        final List<String> read = new ArrayList<>();
        Edge first = null;
        for (int i = 0; i < pageSize; i++) {
          final Edge e = it.next();
          if (first == null)
            first = e;
          read.add(describe(e, direction));
        }
        // the first edge in RID order is a lightweight one of the type created first: the lightweight edges of a bucket sort
        // before its records, and the buckets of the first type before those of the second
        assertThat(first.getIdentity().getPosition()).isNegative();
        assertThat(first.getTypeName()).isEqualTo("E12191");
        deleteLightweightEdge(fixture.hub, direction == Vertex.DIRECTION.OUT ? first.getIn() : first.getOut(), direction);

        while (it.hasNext())
          read.add(describe(it.next(), direction));

        assertThat(read).as("%s edges at page size %d", direction, pageSize).containsExactlyInAnyOrderElementsOf(fixture.edges);
      }
    }
  }

  private static String describe(final Edge edge, final Vertex.DIRECTION direction) {
    return edge.getIdentity() + " -> " + (direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut());
  }

  private record MixedHub(RID hub, List<String> edges, List<RID> neighbours) {
  }

  /**
   * A hub with, for each of two edge types, six lightweight edges to five leaves (the one to the third twice) and three
   * record-backed edges, outgoing or incoming. The edges are described as the client tells them from each other: the RID, which
   * does not tell lightweight edges apart, and the vertex at their other end.
   */
  private MixedHub createMixedHub(final Vertex.DIRECTION direction) {
    final Database embeddedDatabase = getServerDatabase(0, getDatabaseName());
    final RID[] hub = new RID[1];
    final List<String> edges = new ArrayList<>();
    final List<RID> neighbours = new ArrayList<>();
    embeddedDatabase.transaction(() -> {
      final MutableVertex hubVertex = embeddedDatabase.newVertex("V12191").set("name", "mixedHub").save();
      hub[0] = hubVertex.getIdentity();
      final List<MutableVertex> leaves = new ArrayList<>();
      for (int i = 0; i < 6; i++)
        leaves.add(embeddedDatabase.newVertex("V12191").set("name", "mixedLeaf").save());
      // in RID order, which is the order the lightweight edges of the hub are read in: the first leaf has no twin
      leaves.sort(Comparator.comparing(MutableVertex::getIdentity));

      for (final String type : new String[] { "E12191", "E12191B" })
        for (int i = 0; i < leaves.size(); i++) {
          final MutableVertex from = direction == Vertex.DIRECTION.OUT ? hubVertex : leaves.get(i);
          final MutableVertex to = direction == Vertex.DIRECTION.OUT ? leaves.get(i) : hubVertex;
          final int lightweight = i < 5 ? (i == 2 ? 2 : 1) : 0;
          for (int t = 0; t < lightweight; t++) {
            final Edge edge = from.newLightEdge(type, to);
            edges.add(describe(edge, direction));
            neighbours.add(leaves.get(i).getIdentity());
          }
          if (i >= 3) {
            final Edge edge = from.newEdge(type, to);
            edges.add(describe(edge, direction));
            neighbours.add(leaves.get(i).getIdentity());
          }
        }
    });
    return new MixedHub(hub[0], edges, neighbours);
  }

  /** The vertex at the other side of an edge or, for a vertex, the vertex itself. */
  private static RID farEndpoint(final Object element, final Vertex.DIRECTION direction) {
    if (element instanceof Vertex vertex)
      return vertex.getIdentity();
    final Edge edge = (Edge) element;
    return direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
  }

  private record LightweightHub(RID hub, List<RID> leaves) {
  }

  /**
   * A hub with eight lightweight edges to (or, for IN, from) eight leaves, the one at {@code twinOf} twice when it is not
   * negative. The leaves are listed in RID order, which is the order the edges of the hub are read in.
   */
  private LightweightHub createLightweightHub(final Vertex.DIRECTION direction, final int twinOf) {
    final Database embeddedDatabase = getServerDatabase(0, getDatabaseName());
    final RID[] hub = new RID[1];
    final List<RID> leaves = new ArrayList<>();
    embeddedDatabase.transaction(() -> {
      final MutableVertex hubVertex = embeddedDatabase.newVertex("V12191").set("name", "lightHub").save();
      hub[0] = hubVertex.getIdentity();
      final List<MutableVertex> leafVertices = new ArrayList<>();
      for (int i = 0; i < 8; i++)
        leafVertices.add(embeddedDatabase.newVertex("V12191").set("name", "lightLeaf").save());
      leafVertices.sort(Comparator.comparing(MutableVertex::getIdentity));

      for (int i = 0; i < leafVertices.size(); i++) {
        final MutableVertex leaf = leafVertices.get(i);
        leaves.add(leaf.getIdentity());
        final int times = i == twinOf ? 2 : 1;
        for (int t = 0; t < times; t++)
          if (direction == Vertex.DIRECTION.OUT)
            hubVertex.newLightEdge("E12191", leaf);
          else
            leaf.newLightEdge("E12191", hubVertex);
      }
    });
    return new LightweightHub(hub[0], leaves);
  }

  /** Deletes one lightweight edge between the hub and {@code far}, as another client does with the database embedded. */
  private void deleteLightweightEdge(final RID hub, final RID far, final Vertex.DIRECTION direction) {
    final Database embeddedDatabase = getServerDatabase(0, getDatabaseName());
    embeddedDatabase.transaction(() -> {
      final Vertex hubVertex = embeddedDatabase.lookupByRID(hub, true).asVertex();
      for (final Edge edge : hubVertex.getEdges(direction, "E12191")) {
        final RID otherEnd = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
        if (otherEnd.equals(far)) {
          edge.delete();
          return;
        }
      }
      throw new AssertionError("No lightweight edge between " + hub + " and " + far);
    });
  }
}
