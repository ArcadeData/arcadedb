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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9715: an edge list may hold an empty segment between two non-empty ones (a GraphBatch prepends a new head over the
 * head a delete emptied). The (edge, vertex) entry iterator stopped at the first empty segment it reached, so a Graph
 * Analytical View built from it missed every edge past that segment, and CHECK DATABASE reported those edges as missing from
 * their outgoing vertex's list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9715EmptyEdgeSegmentTest extends TestHelper {
  private static final String COUNT = "MATCH (a:P {name: 'A'})-[:K]->(b:P) RETURN count(*) AS n";

  private RID   a;
  private RID[] targets;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("K");
    database.transaction(() -> {
      a = database.newVertex("P").set("name", "A").save().getIdentity();
      targets = new RID[4];
      for (int i = 0; i < targets.length; i++)
        targets[i] = database.newVertex("P").set("name", "T" + i).save().getIdentity();
    });

    // one edge per batch, then the newest one deleted: the head segment is left empty
    for (int i = 0; i < 2; i++)
      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(a, "K", targets[i]);
      }
    database.transaction(() -> {
      for (final Edge e : vertexA().getEdges(Vertex.DIRECTION.OUT, "K"))
        if (e.getIn().equals(targets[1]))
          e.delete();
    });
  }

  @Test
  void anEmptySegmentLeftBetweenTwoBatchesHidesNoEdge() throws Exception {
    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(a, "K", targets[2]);
      batch.newEdge(a, "K", targets[3]);
    }
    assertEveryReaderSeesThreeEdges();
  }

  @Test
  void aTransactionAfterTheDeleteHidesNoEdgeEither() throws Exception {
    database.transaction(() -> {
      vertexA().modify().newEdge("K", targets[2]);
      vertexA().modify().newEdge("K", targets[3]);
    });
    assertEveryReaderSeesThreeEdges();
  }

  private void assertEveryReaderSeesThreeEdges() throws Exception {
    assertThat(vertexA().countEdges(Vertex.DIRECTION.OUT, "K")).isEqualTo(3);
    assertThat(count()).as("MATCH without a view").isEqualTo(3);

    // the iterator the view build and CHECK DATABASE read the list through
    final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine()
        .getEdgeHeadChunk((VertexInternal) vertexA(), Vertex.DIRECTION.OUT);
    int entries = 0;
    for (final Iterator<Pair<RID, RID>> it = list.entryIterator(); it.hasNext(); it.next())
      ++entries;
    assertThat(entries).as("entries of the OUT list").isEqualTo(3);
    int typedEntries = 0;
    for (final Iterator<Pair<RID, RID>> it = list.entryIterator("K"); it.hasNext(); it.next())
      ++typedEntries;
    assertThat(typedEntries).as("entries of type K of the OUT list").isEqualTo(3);

    final Map<String, Object> check = new DatabaseChecker(database).setVerboseLevel(0).check();
    assertThat(check.get("edgesMissingOutReference")).isEqualTo(0L);
    assertThat(check.get("edgesMissingInReference")).isEqualTo(0L);
    checkDatabaseIntegrity();

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getEdgeCount()).isEqualTo(3);
    assertThat(count()).as("MATCH with the view").isEqualTo(3);

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
    assertThat(count()).as("MATCH once the view is dropped").isEqualTo(3);
  }

  private Vertex vertexA() {
    return database.lookupByRID(a, true).asVertex();
  }

  private long count() {
    try (final ResultSet rs = database.query("opencypher", COUNT)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
