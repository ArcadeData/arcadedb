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
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final String COUNT     = "MATCH (a:P {name: 'A'})-[:K]->(b:P) RETURN count(*) AS n";
  // every matched row materialized, so no count push-down can answer it from a counter
  private static final String ROW_COUNT = "MATCH (a:P {name: 'A'})-[r:K]->(b:P) WITH a, r, b RETURN count(r) AS n";

  private RID   a;
  private RID[] targets;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("K");
    database.transaction(() -> {
      a = database.newVertex("P").set("name", "A").save().getIdentity();
      targets = new RID[7];
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
    assertEveryReaderSeesEdges(3);
  }

  @Test
  void consecutiveEmptySegmentsHideNoEdge() throws Exception {
    // a second batch-and-delete round empties the next head too: two empty segments in a row between non-empty ones
    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(a, "K", targets[2]);
      batch.newEdge(a, "K", targets[3]);
    }
    database.transaction(() -> {
      for (final Edge e : vertexA().getEdges(Vertex.DIRECTION.OUT, "K"))
        if (e.getIn().equals(targets[2]) || e.getIn().equals(targets[3]))
          e.delete();
    });
    try (final GraphBatch batch = database.batch().build()) {
      for (int i = 4; i < 7; i++)
        batch.newEdge(a, "K", targets[i]);
    }
    assertThat(longestRunOfEmptySegments()).as("empty segments in a row in the OUT list").isGreaterThanOrEqualTo(2);
    assertEveryReaderSeesEdges(4);
  }

  /**
   * CHECK DATABASE FIX prunes a dangling entry through the iterator's remove(). A hasNext() between next() and remove() can
   * already have stepped over the empty segment into an older one: the entry must still be taken out of the segment it came
   * from, and the older segment left alone.
   */
  @Test
  void removeAfterHasNextSteppedOverAnEmptySegmentPrunesTheReturnedEntry() throws Exception {
    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(a, "K", targets[2]);
      batch.newEdge(a, "K", targets[3]);
    }

    // the head segment holds two entries: the second one returned becomes dangling, its edge record deleted behind the list
    Iterator<Pair<RID, RID>> it = outList().entryIterator();
    it.next();
    final RID dangling = it.next().getFirst();
    final RID danglingTarget = database.lookupByRID(dangling, true).asEdge().getIn();
    database.transaction(() -> database.getSchema().getBucketById(dangling.getBucketId()).deleteRecord(dangling));

    database.transaction(() -> {
      final Iterator<Pair<RID, RID>> walk = outList().entryIterator();
      walk.next();
      assertThat(walk.next().getFirst()).isEqualTo(dangling);
      assertThat(walk.hasNext()).as("the oldest segment, past the empty one").isTrue();
      walk.remove();
      assertThat(walk.next().getSecond()).isEqualTo(targets[0]);
      assertThat(walk.hasNext()).isFalse();
    });

    // the dangling entry is gone, every other one is still there
    final Set<RID> neighbours = new HashSet<>();
    for (it = outList().entryIterator(); it.hasNext(); )
      neighbours.add(it.next().getSecond());
    final Set<RID> expected = new HashSet<>(List.of(targets[0], targets[2], targets[3]));
    expected.remove(danglingTarget);
    assertThat(neighbours).isEqualTo(expected);
    // only the list walked is pruned: the far vertex's IN list still names the deleted record, for CHECK DATABASE to report
    assertThat(vertexA().countEdges(Vertex.DIRECTION.OUT, "K")).isEqualTo(2);

    // which the fix prunes, leaving the database clean for the integrity check after the test
    database.command("sql", "CHECK DATABASE FIX").close();
    assertEveryReaderSeesEdges(2);
  }

  private EdgeLinkedList outList() {
    return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) vertexA(), Vertex.DIRECTION.OUT);
  }

  @Test
  void aTransactionAfterTheDeleteHidesNoEdgeEither() throws Exception {
    database.transaction(() -> {
      vertexA().modify().newEdge("K", targets[2]);
      vertexA().modify().newEdge("K", targets[3]);
    });
    assertEveryReaderSeesEdges(3);
  }

  private int longestRunOfEmptySegments() {
    int longest = 0;
    int run = 0;
    for (EdgeSegment segment = (EdgeSegment) database.lookupByRID(((VertexInternal) vertexA()).getOutEdgesHeadChunk(), true);
         segment != null; segment = segment.getPrevious()) {
      run = segment.isEmpty() ? run + 1 : 0;
      longest = Math.max(longest, run);
    }
    return longest;
  }

  private void assertEveryReaderSeesEdges(final int expected) throws Exception {
    assertThat(vertexA().countEdges(Vertex.DIRECTION.OUT, "K")).isEqualTo(expected);
    assertThat(count()).as("MATCH without a view").isEqualTo(expected);

    // the iterator the view build and CHECK DATABASE read the list through
    final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine()
        .getEdgeHeadChunk((VertexInternal) vertexA(), Vertex.DIRECTION.OUT);
    int entries = 0;
    for (final Iterator<Pair<RID, RID>> it = list.entryIterator(); it.hasNext(); it.next())
      ++entries;
    assertThat(entries).as("entries of the OUT list").isEqualTo(expected);
    int typedEntries = 0;
    for (final Iterator<Pair<RID, RID>> it = list.entryIterator("K"); it.hasNext(); it.next())
      ++typedEntries;
    assertThat(typedEntries).as("entries of type K of the OUT list").isEqualTo(expected);

    final Map<String, Object> check = new DatabaseChecker(database).setVerboseLevel(0).check();
    assertThat(check.get("edgesMissingOutReference")).isEqualTo(0L);
    assertThat(check.get("edgesMissingInReference")).isEqualTo(0L);
    checkDatabaseIntegrity();

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getEdgeCount()).isEqualTo(expected);
    assertThat(count()).as("MATCH with the view").isEqualTo(expected);

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
    assertThat(count()).as("MATCH once the view is dropped").isEqualTo(expected);
  }

  private Vertex vertexA() {
    return database.lookupByRID(a, true).asVertex();
  }

  /** The count of matched edges, asserted equal whether a counter or the matched rows answer it. */
  private long count() {
    final long counted = count(COUNT);
    assertThat(count(ROW_COUNT)).as(ROW_COUNT).isEqualTo(counted);
    return counted;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
