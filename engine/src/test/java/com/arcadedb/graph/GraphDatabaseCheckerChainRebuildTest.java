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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CHECK DATABASE repair of unreadable edge chains: when a chunk of a vertex's edge linked list is lost or
 * corrupted (e.g. after a replication incident), the edge RECORDS usually survive - each edge stores its own
 * out/in vertex RIDs - so the adjacency list can be REBUILT from them instead of amputated.
 * <p>
 * Before this feature the checker did worse than amputate:
 * <ul>
 *   <li>a MID-CHAIN unreadable chunk escaped through {@code Iterator.hasNext()} in the walk loop, the vertex
 *   itself was flagged corrupted, and fix mode DELETED THE VERTEX outright;</li>
 *   <li>an unreadable HEAD chunk nulled the vertex's head pointer, silently losing the whole adjacency
 *   (queries like {@code both().size()} then return 0 instead of failing).</li>
 * </ul>
 * With the rebuild, fix mode reconnects every surviving edge record through the existing
 * {@code reconnectEdges} machinery and a follow-up check reports a clean database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphDatabaseCheckerChainRebuildTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";

  private int savedThreshold;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
  }

  /**
   * MID-CHAIN corruption on the classic layout: walking the IN list of the hub throws when the iterator hops
   * onto the deleted chunk. Fix mode must rebuild the full adjacency from the surviving edge records - and it
   * must NOT delete the hub vertex (the pre-rebuild behavior).
   */
  @Test
  void midChainCorruptionRebuildsAdjacencyAndKeepsVertex() {
    final int degree = 500;
    final RID hubRid = createHub(degree);

    // BREAK THE CHAIN IN THE MIDDLE: the head chunk is the newest; its "previous" is an older, mid-chain chunk.
    final RID[] midChunk = new RID[1];
    database.transaction(() -> {
      final RID head = ((VertexInternal) hubRid.asVertex(true)).getInEdgesHeadChunk();
      final EdgeSegment headChunk = (EdgeSegment) ((DatabaseInternal) database).lookupByRID(head, true);
      midChunk[0] = headChunk.getPreviousRID();
      assertThat(midChunk[0]).as("the hub degree must span more than one chunk").isNotNull();
    });
    deleteRecordLowLevel(midChunk[0]);

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);
    assertThat((Long) stats.get("invalidLinks")).isGreaterThanOrEqualTo(0L); // stats map is populated

    // THE HUB SURVIVED THE FIX...
    database.transaction(() -> assertThat(hubRid.asVertex(true)).isNotNull());
    // ...WITH ITS FULL ADJACENCY REBUILT FROM THE EDGE RECORDS.
    assertRepaired(hubRid, degree);
  }

  /**
   * HEAD-CHUNK corruption on the classic layout: {@code getEdgeHeadChunk} cannot load the list at all. Fix
   * mode must rebuild instead of just nulling the head pointer (which silently reported 0 edges).
   */
  @Test
  void headChunkCorruptionRebuildsAdjacency() {
    final int degree = 500;
    final RID hubRid = createHub(degree);

    final RID[] headChunk = new RID[1];
    database.transaction(() -> headChunk[0] = ((VertexInternal) hubRid.asVertex(true)).getInEdgesHeadChunk());
    deleteRecordLowLevel(headChunk[0]);

    new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);

    assertRepaired(hubRid, degree);
  }

  /**
   * The super-node shape: the hub is promoted to the striped layout, then a mid-chain chunk of the
   * generation-0 (pre-promotion) chain is lost. Fix mode must rebuild the full adjacency.
   */
  @Test
  void promotedSuperNodeChainCorruptionRebuildsAdjacency() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);

    final int degree = 2_000;
    final RID hubRid = createHub(degree);

    final RID[] gen0MidChunk = new RID[1];
    database.transaction(() -> {
      final RID head = ((VertexInternal) hubRid.asVertex(true)).getInEdgesHeadChunk();
      final Record headRecord = ((DatabaseInternal) database).lookupByRID(head, true);
      assertThat(headRecord).as("the hub must have been promoted to the striped layout").isInstanceOf(StripeDirectory.class);
      final StripeDirectory directory = (StripeDirectory) headRecord;
      // Generation 0 is the single pre-promotion classic chain: with a 256 threshold it spans several chunks.
      final RID gen0Head = directory.getHead(0, 0);
      final EdgeSegment gen0HeadChunk = (EdgeSegment) ((DatabaseInternal) database).lookupByRID(gen0Head, true);
      gen0MidChunk[0] = gen0HeadChunk.getPreviousRID();
      assertThat(gen0MidChunk[0]).as("the generation-0 chain must span more than one chunk").isNotNull();
    });
    deleteRecordLowLevel(gen0MidChunk[0]);

    new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);

    database.transaction(() -> assertThat(hubRid.asVertex(true)).isNotNull());
    assertRepaired(hubRid, degree);
  }

  /**
   * Scan-order regression: when the hub sits AFTER its sources in the bucket, the sources are checked first
   * and each one probes the hub's broken IN list ({@code isConnectedTo}). Before the rebuild the probe failure
   * was blamed on the EDGE record, which fix mode then deleted - permanently losing valid edges. The probe
   * failure must instead register the hub for a rebuild and leave every edge record alone.
   */
  @Test
  void probeFailureOnBrokenNeighbourChainMustNotDeleteValidEdges() {
    final int degree = 500;
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE, 1);
      database.getSchema().createEdgeType(EDGE_TYPE, 1);
    });

    // SOURCES FIRST so they occupy the low positions of the bucket and are scanned BEFORE the hub.
    final RID[] sources = new RID[degree];
    database.transaction(() -> {
      for (int i = 0; i < degree; i++)
        sources[i] = database.newVertex(VERTEX_TYPE).set("i", i).save().getIdentity();
    });

    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());

    final int batch = 500;
    for (int from = 0; from < degree; from += batch) {
      final int start = from;
      final int end = Math.min(from + batch, degree);
      database.transaction(() -> {
        for (int i = start; i < end; i++)
          sources[i].asVertex(true).newEdge(EDGE_TYPE, hub[0]);
      });
    }

    final RID[] midChunk = new RID[1];
    database.transaction(() -> {
      final RID head = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
      final EdgeSegment headChunk = (EdgeSegment) ((DatabaseInternal) database).lookupByRID(head, true);
      midChunk[0] = headChunk.getPreviousRID();
      assertThat(midChunk[0]).as("the hub degree must span more than one chunk").isNotNull();
    });
    deleteRecordLowLevel(midChunk[0]);

    new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);

    database.transaction(() -> assertThat(hub[0].asVertex(true)).isNotNull());
    assertRepaired(hub[0], degree);
  }

  /** Creates the hub plus {@code degree} sources, each with one edge source -> hub (hub's IN list). */
  private RID createHub(final int degree) {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE, 1);
      database.getSchema().createEdgeType(EDGE_TYPE, 1);
    });

    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());

    final int batch = 500;
    for (int b = 0; b < (degree + batch - 1) / batch; b++) {
      final int from = b * batch;
      final int to = Math.min(from + batch, degree);
      database.transaction(() -> {
        for (int i = from; i < to; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, hub[0]);
      });
    }
    return hub[0];
  }

  private void deleteRecordLowLevel(final RID rid) {
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
  }

  /** The full adjacency is available again, the traversal query works, and a follow-up check is clean. */
  private void assertRepaired(final RID hubRid, final int degree) {
    database.transaction(() -> {
      final Vertex hub = hubRid.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)).isEqualTo(degree);

      try (final ResultSet rs = database.query("sql",
          "SELECT name, both().size() AS degree FROM " + VERTEX_TYPE + " ORDER BY degree DESC LIMIT 1")) {
        final Result top = rs.next();
        assertThat((String) top.getProperty("name")).isEqualTo("hub");
        assertThat(((Number) top.getProperty("degree")).intValue()).isEqualTo(degree);
      }
    });

    final Map<String, Object> verify = new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, false, 0);
    assertThat((Long) verify.get("totalWarnings")).as("the database must be clean after the repair").isEqualTo(0L);
    assertThat((Long) verify.get("totalCorruptedRecords")).isEqualTo(0L);
  }
}
