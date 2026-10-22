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
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10520: deleting a vertex deletes each self-loop once by having the INCOMING walk skip the self-loops the OUTGOING
 * walk deleted (#10369). The skip used to be granted for every self-loop entry whenever the outgoing walk "did not end cut
 * short", which also covered an outgoing list that could not be opened at all, and an outgoing list that simply did not hold
 * the entry: in both cases nothing was deleted on the way out, the incoming walk passed the self-loop over, and the edge
 * record outlived the vertex at both of its ends. The skip is now derived from what the outgoing walk deleted.
 */
class Issue10520SelfLoopOfDamagedVertexTest extends TestHelper {

  /** The reporter's repro: the OUT head points at a record that is not an edge segment, the IN list is intact. */
  @Test
  void aSelfLoopSurvivesNotAnUndecodableOutgoingList() {
    createSchema();
    final RID[] rids = createVertexWithSelfLoop("E");
    final RID vertex = rids[0];

    final RID[] junk = new RID[1];
    database.transaction(() -> junk[0] = database.newDocument("Junk").save().getIdentity());
    database.transaction(() -> {
      final MutableVertex v = vertex.asVertex(true).modify();
      v.setOutEdgesHeadChunk(junk[0]);
      v.save();
    });

    final long events = deleteCountingEdgeEvents(vertex, "E");

    database.transaction(() -> {
      assertThat(database.existsRecord(vertex)).as("the vertex is deleted, as a corrupt vertex must stay deletable").isFalse();
      assertThat(database.countType("E", false)).as("the IN walk deletes the self-loop the OUT walk could not reach").isZero();
    });
    assertThat(events).as("deleted once, by the IN walk").isEqualTo(1);
  }

  /** The control: the same damage on the IN side was already fine, and must stay so. */
  @Test
  void aSelfLoopSurvivesNotAnUndecodableIncomingList() {
    createSchema();
    final RID[] rids = createVertexWithSelfLoop("E");
    final RID vertex = rids[0];

    final RID[] junk = new RID[1];
    database.transaction(() -> junk[0] = database.newDocument("Junk").save().getIdentity());
    database.transaction(() -> {
      final MutableVertex v = vertex.asVertex(true).modify();
      v.setInEdgesHeadChunk(junk[0]);
      v.save();
    });

    final long events = deleteCountingEdgeEvents(vertex, "E");

    database.transaction(() -> assertThat(database.countType("E", false)).isZero());
    assertThat(events).isEqualTo(1);
  }

  /**
   * The second precondition in the issue: the OUT entry of the self-loop is gone while its record and its IN entry stay, which
   * is what the repair pass's own prune leaves. The OUT walk reads its whole list and deletes nothing, so the IN walk must not
   * skip the entry.
   */
  @Test
  void aSelfLoopListedOnlyAsIncomingIsDeletedWithItsVertex() {
    createSchema();
    final RID[] rids = createVertexWithSelfLoop("E");
    final RID vertex = rids[0];
    final RID edge = rids[1];

    removeOutgoingEntry(vertex, edge);

    final long events = deleteCountingEdgeEvents(vertex, "E");

    database.transaction(() -> {
      assertThat(database.existsRecord(vertex)).isFalse();
      assertThat(database.existsRecord(edge)).as("the self-loop record must not outlive its vertex").isFalse();
    });
    assertThat(events).isEqualTo(1);
    assertIntegrityClean();
  }

  /**
   * The skip is per deleted entry, not per neighbour: one self-loop listed on both sides is deleted once by the OUT walk and
   * skipped by the IN walk, while a second one listed on the IN side only is still deleted by the IN walk.
   */
  @Test
  void onlyTheSelfLoopsTheOutgoingWalkDeletedAreSkipped() {
    createSchema();
    final RID[] first = createVertexWithSelfLoop("E");
    final RID vertex = first[0];
    final RID[] second = new RID[1];
    database.transaction(() -> second[0] = vertex.asVertex().modify().newEdge("E", vertex).getIdentity());

    removeOutgoingEntry(vertex, second[0]);

    final long events = deleteCountingEdgeEvents(vertex, "E");

    database.transaction(() -> {
      assertThat(database.existsRecord(first[1])).isFalse();
      assertThat(database.existsRecord(second[0])).isFalse();
      assertThat(database.countType("E", false)).isZero();
    });
    assertThat(events).as("each self-loop is deleted, and reported, exactly once").isEqualTo(2);
    assertIntegrityClean();
  }

  /** A lightweight self-loop has no record to orphan, but the IN-only entry must still be deleted and reported once. */
  @Test
  void aLightweightSelfLoopListedOnlyAsIncomingIsDeletedOnce() {
    createSchema();
    database.transaction(
        () -> database.getSchema().buildEdgeType().withName("LIGHT").withTotalBuckets(1).withLightweight(true).create());
    final RID[] rids = createVertexWithSelfLoop("LIGHT");
    final RID vertex = rids[0];

    database.transaction(() -> {
      final VertexInternal v = (VertexInternal) vertex.asVertex();
      final Edge light = v.getEdges(Vertex.DIRECTION.OUT, "LIGHT").iterator().next();
      ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunkForWrite(v, Vertex.DIRECTION.OUT).removeEdge(light);
    });
    database.transaction(() -> {
      assertThat(vertex.asVertex().countEdges(Vertex.DIRECTION.OUT, "LIGHT")).isZero();
      assertThat(vertex.asVertex().countEdges(Vertex.DIRECTION.IN, "LIGHT")).isEqualTo(1);
    });

    final long events = deleteCountingEdgeEvents(vertex, "LIGHT");

    database.transaction(() -> assertThat(database.existsRecord(vertex)).isFalse());
    assertThat(events).as("the IN walk deletes the lightweight self-loop the OUT walk never saw").isEqualTo(1);
  }

  private void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1);
      database.getSchema().createEdgeType("E", 1);
      database.getSchema().createDocumentType("Junk", 1);
    });
  }

  /** @return {vertex, self-loop edge} */
  private RID[] createVertexWithSelfLoop(final String edgeType) {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("V").save();
      rids[0] = v.getIdentity();
      rids[1] = v.newEdge(edgeType, v).getIdentity();
    });
    database.transaction(() -> {
      assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, edgeType)).isEqualTo(1);
      assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.IN, edgeType)).isEqualTo(1);
    });
    return rids;
  }

  /** Takes the edge out of the vertex's OUT list only, leaving the record and the IN entry in place. */
  private void removeOutgoingEntry(final RID vertex, final RID edge) {
    database.transaction(() -> ((DatabaseInternal) database).getGraphEngine()
        .getEdgeHeadChunkForWrite((VertexInternal) vertex.asVertex(), Vertex.DIRECTION.OUT).removeEdgeRID(edge));
    database.transaction(() -> {
      assertThat(database.existsRecord(edge)).isTrue();
      for (final Edge e : vertex.asVertex().getEdges(Vertex.DIRECTION.OUT))
        assertThat(e.getIdentity()).isNotEqualTo(edge);
      boolean listedAsIncoming = false;
      for (final Edge e : vertex.asVertex().getEdges(Vertex.DIRECTION.IN))
        listedAsIncoming |= e.getIdentity().equals(edge);
      assertThat(listedAsIncoming).as("the IN entry stays").isTrue();
    });
  }

  private long deleteCountingEdgeEvents(final RID vertex, final String edgeType) {
    final AtomicLong events = new AtomicLong();
    final BeforeRecordDeleteListener probe = record -> {
      if (record instanceof Edge)
        events.incrementAndGet();
      return true;
    };
    database.getSchema().getType(edgeType).getEvents().registerListener(probe);
    try {
      database.transaction(() -> vertex.asVertex(true).delete());
    } finally {
      database.getSchema().getType(edgeType).getEvents().unregisterListener(probe);
    }
    return events.get();
  }

  private void assertIntegrityClean() {
    try (final ResultSet rs = database.command("sql", "check database")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(longProperty(row, "autoFix")).as("check database autoFix: " + row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "totalErrors")).as("check database totalErrors: " + row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "totalWarnings")).as("check database totalWarnings: " + row.toJSON()).isEqualTo(0L);
      }
    }
  }

  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value == null ? 0L : ((Number) value).longValue();
  }
}
