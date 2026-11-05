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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.utility.Pair;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #11012: a full scan of the edge lists (the build of a Graph Analytical View) read every segment as a record and handed
 * out every entry as a pair of RIDs, about 400 bytes of garbage per edge. {@link EdgeListScanner} reads the segments where they
 * lie and hands the entries out as primitives: the same entries in the same order, without allocating per entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11012EdgeListScannerTest extends TestHelper {
  private static final int VERTICES = 200;

  @Test
  void theScannerHandsOutTheEntriesTheRecordWalkDoesInTheSameOrder() {
    final List<RID> vertices = createGraph();

    final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
    database.transaction(() -> assertSameEntriesAsTheRecordWalk(scanner, vertices));

    // Not vacuous: the lists span several segments, all read in place
    assertThat(scanner.getSegmentsReadInPlace()).isGreaterThan(2L * VERTICES);
    assertThat(scanner.getSegmentsReadAsRecords()).isZero();
  }

  @Test
  void aSegmentTheTransactionWroteIsReadAsTheTransactionHoldsIt() {
    final List<RID> vertices = createGraph();

    database.transaction(() -> {
      final Vertex first = vertices.get(0).asVertex();
      for (int i = 0; i < 30; i++)
        first.newEdge("KNOWS", vertices.get(1 + i).asVertex());
      // The edge appended last is not committed: only the transaction's copy of the segment holds it
      final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
      assertSameEntriesAsTheRecordWalk(scanner, vertices);
      assertThat(scanner.getSegmentsReadAsRecords()).isPositive();
    });
  }

  @Test
  void aReadListenerSeesTheSegmentsAsTheRecordWalkReadsThem() {
    final List<RID> vertices = createGraph();

    final AtomicInteger reads = new AtomicInteger();
    final AfterRecordReadListener listener = record -> {
      reads.incrementAndGet();
      return record;
    };
    database.getEvents().registerListener(listener);
    try {
      final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
      database.transaction(() -> assertSameEntriesAsTheRecordWalk(scanner, vertices));
      assertThat(scanner.getSegmentsReadInPlace()).isZero();
      assertThat(reads.get()).isPositive();
    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  @Test
  void aStripedSuperNodeListIsWalkedWhole() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
    final List<RID> vertices = createGraph();

    final VertexInternal hub = (VertexInternal) vertices.get(0).asVertex();
    assertThat(database.lookupByRID(hub.getOutEdgesHeadChunk(), true)).isInstanceOf(StripeDirectory.class);

    final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
    database.transaction(() -> assertSameEntriesAsTheRecordWalk(scanner, vertices));
  }

  /**
   * Under REPEATABLE_READ the scanner reads the pages the transaction pinned, as the record walk does: an edge committed by
   * another thread after the transaction read the list is seen by neither, and a list read after the commit is seen alike by
   * both.
   */
  @Test
  void underRepeatableReadTheScannerReadsThePinnedPagesAsTheRecordWalkDoes() throws Exception {
    final List<RID> vertices = createGraph();
    final RID hub = vertices.get(1);
    final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final List<String> before = recordWalk(hub);
      assertThat(scan(scanner, hub)).isEqualTo(before);

      // ANOTHER THREAD APPENDS TO THE SAME LIST AND COMMITS
      final Thread writer = new Thread(() -> database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          hub.asVertex().newEdge("KNOWS", vertices.get(10 + i).asVertex());
      }));
      writer.start();
      writer.join();

      // THE PINNED PAGES STILL ANSWER: THE SCANNER SEES WHAT THE RECORD WALK SEES, AND WHAT IT SAW BEFORE
      assertThat(recordWalk(hub)).isEqualTo(before);
      assertThat(scan(scanner, hub)).isEqualTo(before);
    } finally {
      database.rollback();
    }

    database.transaction(() -> {
      final List<String> after = recordWalk(hub);
      assertThat(after).hasSize(41 + 5);
      assertThat(scan(scanner, hub)).isEqualTo(after);
    });
  }

  private List<String> recordWalk(final RID rid) {
    final List<String> entries = new ArrayList<>();
    final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine()
        .getEdgeHeadChunk((VertexInternal) database.lookupByRID(rid, true), Vertex.DIRECTION.OUT);
    final Iterator<Pair<RID, RID>> it = list.entryIterator();
    while (it.hasNext()) {
      final Pair<RID, RID> entry = it.next();
      entries.add(entry.getFirst() + ">" + entry.getSecond());
    }
    return entries;
  }

  private static List<String> scan(final EdgeListScanner scanner, final RID rid) {
    final List<String> entries = new ArrayList<>();
    scanner.scan(rid.getBucketId(), rid.getPosition(), Vertex.DIRECTION.OUT,
        (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> entries.add(
            "#" + edgeBucketId + ":" + edgePosition + ">#" + vertexBucketId + ":" + vertexPosition));
    return entries;
  }

  /** A visitor that walks the far end's list with the same scanner does not disturb the walk it is called from. */
  @Test
  void aVisitorMayScanAnotherListWithTheSameScanner() {
    final List<RID> vertices = createGraph();
    final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);

    database.transaction(() -> {
      for (final RID rid : vertices.subList(0, 20)) {
        final List<String> expected = new ArrayList<>();
        scanner.scan(rid.getBucketId(), rid.getPosition(), Vertex.DIRECTION.OUT,
            (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> expected.add(vertexBucketId + ":" + vertexPosition));

        final List<String> scanned = new ArrayList<>();
        final long[] nested = new long[1];
        scanner.scan(rid.getBucketId(), rid.getPosition(), Vertex.DIRECTION.OUT,
            (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> {
              scanned.add(vertexBucketId + ":" + vertexPosition);
              scanner.scan(vertexBucketId, vertexPosition, Vertex.DIRECTION.IN, (a, b, c, d) -> ++nested[0]);
            });

        assertThat(scanned).isEqualTo(expected);
        assertThat(nested[0]).isPositive();
      }
    });
  }

  @Test
  void aPositionThatHoldsNoVertexVisitsNothing() {
    final List<RID> vertices = createGraph();
    final RID deleted = vertices.get(5);
    database.transaction(() -> deleted.asVertex().delete());

    final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
    final AtomicInteger visited = new AtomicInteger();
    database.transaction(() -> {
      scanner.scan(deleted.getBucketId(), deleted.getPosition(), Vertex.DIRECTION.OUT, (a, b, c, d) -> visited.incrementAndGet());
      scanner.scan(deleted.getBucketId(), 1_000_000L, Vertex.DIRECTION.OUT, (a, b, c, d) -> visited.incrementAndGet());
    });
    assertThat(visited.get()).isZero();
  }

  @Test
  void theRecordPositionsOfABucketAreTheRecordsAScanHandsOut() {
    final List<RID> vertices = createGraph();
    database.transaction(() -> {
      // A deleted record leaves a hole, a new one of this transaction is on a page only the transaction holds
      vertices.get(3).asVertex().delete();
      vertices.get(77).asVertex().delete();
      database.newVertex("Person").set("id", -1).save();

      for (final Bucket bucket : database.getSchema().getType("Person").getBuckets(false)) {
        final List<Long> expected = new ArrayList<>();
        final Iterator<Record> records = bucket.iterator();
        while (records.hasNext())
          expected.add(records.next().getIdentity().getPosition());

        final List<Long> positions = new ArrayList<>();
        ((LocalBucket) bucket).forEachRecordPosition(positions::add);
        assertThat(positions).isEqualTo(expected);
      }
    });
  }

  /**
   * The point of the issue, measured on the thread's allocation counter rather than on time: a scan of every list allocates
   * per list, not per entry, so the bytes per entry are a small fraction of the record walk's.
   */
  @Test
  void aFullScanAllocatesAFractionOfTheRecordWalkPerEntry() {
    final List<RID> vertices = createGraph();
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    // THE THREAD'S ALLOCATION COUNTER IS A HOTSPOT FEATURE: A JVM WITHOUT IT HAS NOTHING TO MEASURE
    assumeTrue(threads.isThreadAllocatedMemorySupported() && threads.isThreadAllocatedMemoryEnabled());
    final long thread = Thread.currentThread().getId();

    database.transaction(() -> {
      final List<VertexInternal> loaded = new ArrayList<>();
      for (final RID rid : vertices)
        loaded.add((VertexInternal) rid.asVertex());

      final long[] sink = new long[1];
      final EdgeListScanner scanner = new EdgeListScanner((DatabaseInternal) database);
      final EdgeListScanner.EntryVisitor visitor = (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> sink[0] +=
          vertexPosition + edgeBucketId;

      long recordWalkBytes = Long.MAX_VALUE;
      long scannerBytes = Long.MAX_VALUE;
      long entries = 0;
      // The smallest of a few rounds, so a JIT compilation in the middle of one does not count
      for (int round = 0; round < 5; round++) {
        long before = threads.getThreadAllocatedBytes(thread);
        long count = 0;
        for (final VertexInternal v : loaded) {
          final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk(v, Vertex.DIRECTION.OUT);
          if (list == null)
            continue;
          final Iterator<Pair<RID, RID>> it = list.entryIterator();
          while (it.hasNext()) {
            final Pair<RID, RID> entry = it.next();
            sink[0] += entry.getSecond().getPosition();
            ++count;
          }
        }
        recordWalkBytes = Math.min(recordWalkBytes, threads.getThreadAllocatedBytes(thread) - before);
        entries = count;

        before = threads.getThreadAllocatedBytes(thread);
        for (final VertexInternal v : loaded)
          scanner.scan(v, Vertex.DIRECTION.OUT, visitor);
        scannerBytes = Math.min(scannerBytes, threads.getThreadAllocatedBytes(thread) - before);
      }

      assertThat(entries).isGreaterThan(8_000L);
      assertThat(scannerBytes).as("scanner %d bytes, record walk %d bytes, over %d entries", scannerBytes, recordWalkBytes, entries)
          .isLessThan(recordWalkBytes / 10);
      assertThat(sink[0]).isNotZero();
    });
  }

  /** A graph whose out-lists span several segments, with regular and lightweight edges of two types and self loops. */
  private List<RID> createGraph() {
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.command("sql", "CREATE EDGE TYPE KNOWS").close();
    database.command("sql", "CREATE EDGE TYPE LIKES LIGHTWEIGHT").close();
    final List<RID> vertices = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        vertices.add(database.newVertex("Person").set("id", i).save().getIdentity());
    });
    final Random random = new Random(11012);
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++) {
        final Vertex from = vertices.get(i).asVertex();
        for (int k = 0; k < 40; k++)
          from.newEdge(k % 3 == 0 ? "LIKES" : "KNOWS", vertices.get(random.nextInt(VERTICES)).asVertex());
        from.newEdge("KNOWS", from);
      }
      // One hub with many more edges than the others
      final Vertex hub = vertices.get(0).asVertex();
      for (int k = 0; k < 300; k++)
        hub.newEdge("KNOWS", vertices.get(random.nextInt(VERTICES)).asVertex());
    });
    return vertices;
  }

  private void assertSameEntriesAsTheRecordWalk(final EdgeListScanner scanner, final List<RID> vertices) {
    final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
    for (final RID rid : vertices) {
      final VertexInternal vertex = (VertexInternal) rid.asVertex();
      for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
        final List<String> expected = new ArrayList<>();
        final EdgeLinkedList list = engine.getEdgeHeadChunk(vertex, direction);
        if (list != null) {
          final Iterator<Pair<RID, RID>> it = list.entryIterator();
          while (it.hasNext()) {
            final Pair<RID, RID> entry = it.next();
            expected.add(entry.getFirst().getBucketId() + ":" + entry.getFirst().getPosition() + ">" + entry.getSecond().getBucketId() + ":"
                + entry.getSecond().getPosition());
          }
        }

        final List<String> scanned = new ArrayList<>();
        scanner.scan(vertex, direction,
            (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> scanned.add(
                edgeBucketId + ":" + edgePosition + ">" + vertexBucketId + ":" + vertexPosition));

        assertThat(scanned).as("%s list of %s", direction, rid).isEqualTo(expected);

        // The same list, from the vertex's position: its head is read where the vertex record lies
        scanned.clear();
        scanner.scan(rid.getBucketId(), rid.getPosition(), direction,
            (edgeBucketId, edgePosition, vertexBucketId, vertexPosition) -> scanned.add(
                edgeBucketId + ":" + edgePosition + ">" + vertexBucketId + ":" + vertexPosition));
        assertThat(scanned).as("%s list of %s by position", direction, rid).isEqualTo(expected);
      }
    }
  }
}
