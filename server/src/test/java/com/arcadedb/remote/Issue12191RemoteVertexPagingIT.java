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
}
