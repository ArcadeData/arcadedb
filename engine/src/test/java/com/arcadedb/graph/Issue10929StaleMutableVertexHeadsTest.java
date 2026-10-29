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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10929: a {@link MutableVertex} handed out by {@code modify()} is a copy of the record with the edge-list heads it
 * was copied with. An edge appended in the same transaction through another handle of the vertex moves the head on the
 * instance the transaction holds, not on that copy, so the copy
 * <ul>
 * <li>answered the neighbours it had before the append (its accessors did not read the heads from the transaction's
 * instance, as {@link ImmutableVertex}'s do), and</li>
 * <li>wrote its stale heads back on {@code save()}, unlinking every edge appended since from the vertex while the edge
 * record and the far endpoint's entry survived.</li>
 * </ul>
 * A {@code save()} is a property write: it must never change which edges a vertex has.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10929StaleMutableVertexHeadsTest extends TestHelper {
  private RID a;
  private RID b;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E");
    database.transaction(() -> {
      a = database.newVertex("V").set("id", "a").save().getIdentity();
      b = database.newVertex("V").set("id", "b").save().getIdentity();
    });
  }

  @Test
  void staleModifyHandleReadsTheEdgesAppendedThroughAnotherHandle() {
    database.transaction(() -> {
      final Vertex immutableA = database.lookupByRID(a, true).asVertex();
      final MutableVertex staleA = immutableA.modify();

      database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);

      assertThat(immutableA.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(1L);
      assertThat(staleA.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(1L);
      assertThat(staleA.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1L);
      assertThat(rids(staleA.getVertices(Vertex.DIRECTION.OUT))).containsExactly(b);
      assertThat(rids(staleA.getVertices(Vertex.DIRECTION.OUT, "E"))).containsExactly(b);
      assertThat(rids(staleA.getVertices())).containsExactly(b);
      assertThat(count(staleA.getEdges())).isEqualTo(1);
      assertThat(count(staleA.getEdges(Vertex.DIRECTION.OUT, "E"))).isEqualTo(1);
      assertThat(staleA.getConnectedVertexRIDs(Vertex.DIRECTION.OUT)).containsExactly(b);
      assertThat(staleA.isConnectedTo(b)).isTrue();
      assertThat(staleA.isConnectedTo(b, Vertex.DIRECTION.OUT)).isTrue();
      assertThat(staleA.isConnectedTo(b, Vertex.DIRECTION.OUT, "E")).isTrue();
    });
  }

  @Test
  void savingAStaleModifyHandleKeepsTheEdgesAppendedThroughAnotherHandle() {
    database.transaction(() -> {
      final MutableVertex staleA = database.lookupByRID(a, true).asVertex().modify();
      final MutableVertex staleB = database.lookupByRID(b, true).asVertex().modify();

      database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);

      staleA.set("x", 1).save();
      staleB.set("y", 2).save();

      assertThat(database.lookupByRID(a, true).asVertex().countEdges(Vertex.DIRECTION.OUT)).isEqualTo(1L);
      assertThat(database.lookupByRID(b, true).asVertex().countEdges(Vertex.DIRECTION.IN)).isEqualTo(1L);
    });

    assertConnectedOnBothSides(1);
    assertThat(database.lookupByRID(a, true).asVertex().getInteger("x")).isEqualTo(1);
    assertThat(database.lookupByRID(b, true).asVertex().getInteger("y")).isEqualTo(2);
    assertCheckDatabaseIsClean();
  }

  @Test
  void savingAStaleModifyHandleKeepsTheEdgesAppendedThroughTheEngineApi() {
    database.transaction(() -> {
      final MutableVertex staleA = database.lookupByRID(a, true).asVertex().modify();

      database.lookupByRID(a, true).asVertex().newEdge("E", b);

      staleA.set("x", 1).save();
    });

    assertConnectedOnBothSides(1);
    assertCheckDatabaseIsClean();
  }

  /**
   * Enough edges for the head of each list to move to a new segment more than once while the stale handle is held: every
   * move is a head the copy does not know.
   */
  @Test
  void savingAStaleModifyHandleKeepsEveryHeadTheListMovedTo() {
    final int edges = 500;
    database.transaction(() -> {
      final MutableVertex staleA = database.lookupByRID(a, true).asVertex().modify();
      final MutableVertex staleB = database.lookupByRID(b, true).asVertex().modify();
      final RID headBefore = staleA.getOutEdgesHeadChunk();

      for (int i = 0; i < edges; i++)
        database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);

      final VertexInternal held = (VertexInternal) database.lookupByRID(a, true).asVertex();
      assertThat(held.getOutEdgesHeadChunk()).isNotEqualTo(headBefore);

      staleA.set("x", 1).save();
      staleB.set("y", 2).save();
      assertThat(staleA.getOutEdgesHeadChunk()).isEqualTo(held.getOutEdgesHeadChunk());
    });

    assertConnectedOnBothSides(edges);
    assertCheckDatabaseIsClean();
  }

  /** The handle stays usable after the save: a further append goes to it and a further save keeps it. */
  @Test
  void aStaleHandleSavedTwiceAroundFurtherAppendsLosesNothing() {
    database.transaction(() -> {
      final MutableVertex staleA = database.lookupByRID(a, true).asVertex().modify();

      database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);
      staleA.set("x", 1).save();
      database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b);
      staleA.newEdge("E", b);
      staleA.set("x", 2).save();
    });

    assertConnectedOnBothSides(3);
    assertThat(database.lookupByRID(a, true).asVertex().getInteger("x")).isEqualTo(2);
    assertCheckDatabaseIsClean();
  }

  /** A head the handle sets ITSELF is a deliberate edge-list write, and the save keeps it. */
  @Test
  void anExplicitHeadAssignmentOnTheHandleIsStillWritten() {
    database.transaction(() -> database.command("sql", "CREATE EDGE E FROM ? TO ?", a, b));

    database.begin();
    try {
      final MutableVertex handle = database.lookupByRID(a, true).asVertex().modify();
      database.command("sql", "UPDATE ? SET z = 1", a);
      handle.setOutEdgesHeadChunk(null);
      handle.save();
      assertThat(((VertexInternal) database.lookupByRID(a, true).asVertex()).getOutEdgesHeadChunk()).isNull();
    } finally {
      database.rollback();
    }

    assertConnectedOnBothSides(1);
  }

  private void assertConnectedOnBothSides(final int expected) {
    database.transaction(() -> {
      final Vertex reloadedA = database.lookupByRID(a, true).asVertex();
      final Vertex reloadedB = database.lookupByRID(b, true).asVertex();
      assertThat(reloadedA.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(expected);
      assertThat(count(reloadedA.getEdges(Vertex.DIRECTION.OUT, "E"))).isEqualTo(expected);
      assertThat(reloadedB.countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(expected);
      assertThat(count(reloadedB.getEdges(Vertex.DIRECTION.IN, "E"))).isEqualTo(expected);
      assertThat(database.countType("E", false)).isEqualTo(expected);
    });
  }

  private void assertCheckDatabaseIsClean() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result report = rs.next();
      assertThat(report.<Long>getProperty("edgesMissingOutReference")).isZero();
      final Object warnings = report.getProperty("warnings");
      if (warnings != null)
        assertThat((Iterable<?>) warnings).isEmpty();
      assertThat(report.<Long>getProperty("totalErrors")).isZero();
    }
  }

  private static List<RID> rids(final Iterable<Vertex> vertices) {
    final List<RID> result = new ArrayList<>();
    for (final Vertex v : vertices)
      result.add(v.getIdentity());
    return result;
  }

  private static int count(final Iterable<?> iterable) {
    int n = 0;
    for (final Object ignored : iterable)
      n++;
    return n;
  }
}
