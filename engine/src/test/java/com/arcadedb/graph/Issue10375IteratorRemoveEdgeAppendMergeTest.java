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
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #10375: {@code Iterator.remove()} on an adjacency iterator took the entry out of the segment by
 * position and wrote the segment without excluding its page from the edge-append merge. When the same transaction had
 * also appended to that segment, a concurrent commit on the page made the merge rebase it - committed image plus this
 * transaction's tracked appends - and the removal vanished, while the edge record it named was deleted for good: the
 * list was left with an entry pointing at a freed slot.
 * <p>
 * Every case runs the reporter's window: the transaction appends to the vertex's OUT chunk and removes an entry of it
 * through an iterator, a second thread commits an append to the same chunk, then the transaction commits. The commit
 * must fail with the retryable conflict every other removal site gives, and the graph must be left as it was.
 */
class Issue10375IteratorRemoveEdgeAppendMergeTest extends TestHelper {
  private static final int COMMITTED_EDGES = 5;

  private RID   source;
  private RID[] targets;
  private RID[] edges;

  /** One way to take the entry of {@code edges[0]} out of the list, each through its own iterator. */
  enum Walk {
    EDGES {
      @Override
      Iterator<?> open(final EdgeLinkedList list) {
        return list.edgeIterator();
      }
    }, EDGES_FILTERED {
      @Override
      Iterator<?> open(final EdgeLinkedList list) {
        return list.edgeIterator("E");
      }
    }, VERTICES {
      @Override
      Iterator<?> open(final EdgeLinkedList list) {
        return list.vertexIterator();
      }
    }, VERTICES_FILTERED {
      @Override
      Iterator<?> open(final EdgeLinkedList list) {
        return list.vertexIterator("E");
      }
    }, ENTRIES {
      @Override
      Iterator<?> open(final EdgeLinkedList list) {
        return list.entryIterator();
      }
    }, ENTRIES_FILTERED {
      @Override
      Iterator<?> open(final EdgeLinkedList list) {
        return list.entryIterator("E");
      }
    };

    abstract Iterator<?> open(EdgeLinkedList list);
  }

  @ParameterizedTest
  @EnumSource(Walk.class)
  void anIteratorRemovalNextToAnAppendIsNotRebasedAway(final Walk walk) throws Exception {
    createGraph();

    runWindow(() -> {
      final Iterator<?> it = walk.open(outList());
      advanceTo(it, targets[0]);
      it.remove();
    });

    // THE EDGE THE REFUSED TRANSACTION DELETED IS STILL THERE, AND SO IS ITS ENTRY
    assertThat(edges[0].asEdge(true)).isNotNull();
    assertThat(outEntries()).contains(edges[0]);
    assertNoDanglingEntry();
  }

  /**
   * The {@code CHECK DATABASE FIX} prune, {@link EdgeVertexIterator#removeEntryKeepingEdge}: the same unpoisoned write,
   * so the repair pass could lose its own repair the same way.
   */
  @Test
  void thePruneOfTheRepairPassIsNotRebasedAway() throws Exception {
    createGraph();

    runWindow(() -> {
      final EdgeVertexIterator it = (EdgeVertexIterator) outList().entryIterator();
      advanceTo(it, targets[0]);
      it.removeEntryKeepingEdge();
    });

    assertThat(outEntries()).contains(edges[0]);
    assertNoDanglingEntry();
  }

  /**
   * A delete listener that appends to the walked list makes {@code remove()} find the entry by identity instead of by
   * position: the other unpoisoned write. Here the listener's append alone is what the merge tracks.
   */
  @Test
  void aRemovalByIdentityAfterAListenerAppendIsNotRebasedAway() throws Exception {
    createGraph();

    final boolean[] inListener = { false };
    final AfterRecordDeleteListener listener = record -> {
      if (inListener[0] || !(record instanceof Edge))
        return;
      inListener[0] = true;
      try {
        source.asVertex().newEdge("E", targets[1].asVertex());
      } finally {
        inListener[0] = false;
      }
    };

    database.getEvents().registerListener(listener);
    try {
      runWindow(() -> {
        final Iterator<?> it = outList().edgeIterator();
        advanceTo(it, targets[0]);
        it.remove();
      });
    } finally {
      database.getEvents().unregisterListener(listener);
    }

    assertThat(edges[0].asEdge(true)).isNotNull();
    assertThat(outEntries()).contains(edges[0]);
    assertNoDanglingEntry();
  }

  /** The conflict is retryable: the retry reads the list again and the removal goes through, leaving nothing behind. */
  @Test
  void theRetryOfTheRefusedRemovalSucceeds() throws Exception {
    createGraph();

    final ExecutorService other = Executors.newSingleThreadExecutor();
    final int[] attempts = { 0 };
    try {
      database.transaction(() -> {
        ++attempts[0];
        source.asVertex().newEdge("E", targets[COMMITTED_EDGES].asVertex());
        final Iterator<?> it = outList().edgeIterator();
        advanceTo(it, targets[0]);
        it.remove();
        if (attempts[0] == 1)
          appendInOwnTransaction(other);
      }, false, 3);
    } finally {
      other.shutdownNow();
    }

    assertThat(attempts[0]).isEqualTo(2);
    assertThatThrownBy(() -> edges[0].asEdge(true)).isInstanceOf(RecordNotFoundException.class);
    assertThat(outEntries()).doesNotContain(edges[0]).hasSize(COMMITTED_EDGES + 1);
    assertNoDanglingEntry();
  }

  /**
   * Every source of the graph package that handles edge segments, and what it hands {@code updateRecord} directly. A
   * segment write that is not an in-chunk append must go through {@link EdgeLinkedList#updateRewrittenSegment}, which
   * excludes the page from the edge-append merge: a new direct {@code updateRecord} of a segment fails this census, so
   * whoever adds one has to say which of the two it is.
   * <p>
   * A tripwire, not a proof: it reads the sources line by line, so a write that reaches {@code updateRecord} through
   * another method, or a call split over several lines, goes unseen. Every argument expression is compared, a cast or a
   * method call included, and the subpackages are scanned too.
   */
  private static final Map<String, Map<String, Integer>> DIRECT_UPDATES = Map.of(
      // add(): THE TRACKED IN-CHUNK APPEND; updateRewrittenSegment(): THE ONE DOOR FOR EVERY OTHER SEGMENT WRITE
      "EdgeLinkedList.java", Map.of("lastSegment", 1, "segment", 1),
      // append(): THE TRACKED IN-CHUNK APPEND; updateSlot(): THE STRIPE DIRECTORY, WHICH POISONS ITS OWN PAGE
      "StripedEdgeList.java", Map.of("chunk", 1, "dir", 1),
      // ITS OWN TRANSACTIONS ONLY, WHICH TRACK NO APPEND: SEE THE NOTE ON THE EDGE-APPEND MERGE IN THE CLASS
      "GraphBatch.java", Map.of("vertex", 5, "outChunk", 2, "inChunk", 2, "currentSegment", 6));

  private static final Pattern UPDATE_RECORD = Pattern.compile("\\.updateRecord\\((.+?)\\);");

  @Test
  void everySegmentRewriteGoesThroughTheDoorThatExcludesItsPage() throws IOException {
    // RESOLVED RELATIVE TO THE MODULE ROOT, THE WAY SUREFIRE RUNS
    final Path graphSources = Path.of("src/main/java/com/arcadedb/graph");
    assertThat(graphSources).as("the graph sources, read from the engine module's directory (run the test from engine/)")
        .isDirectory();

    final Map<String, Map<String, Integer>> found = new TreeMap<>();
    try (final Stream<Path> files = Files.walk(graphSources)) {
      for (final Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
        final List<String> lines = Files.readAllLines(file);
        if (lines.stream().noneMatch(line -> line.contains("EdgeSegment")))
          continue;
        for (final String line : lines) {
          final String code = line.strip();
          if (code.startsWith("*") || code.startsWith("//") || code.startsWith("/*"))
            continue;
          final Matcher matcher = UPDATE_RECORD.matcher(code);
          while (matcher.find())
            found.computeIfAbsent(file.getFileName().toString(), k -> new TreeMap<>())
                .merge(matcher.group(1).strip(), 1, Integer::sum);
        }
      }
    }

    assertThat(found).as("direct updateRecord calls in the graph sources that handle edge segments").isNotEmpty();
    // THE ITERATORS IN PARTICULAR: THEIR remove() IS THE WRITE #10375 FOUND UNEXCLUDED
    assertThat(found).doesNotContainKey("ResettableIteratorBase.java");
    // BY COUNT, SO A NEW CALL ON A NAME ALREADY LISTED FAILS TOO
    for (final Map.Entry<String, Map<String, Integer>> entry : found.entrySet())
      assertThat(entry.getValue())
          .as("records %s hands updateRecord directly, and how many times. A segment rewrite must use "
              + "EdgeLinkedList.updateRewrittenSegment; a call that is a tracked append, or writes no segment, is added to "
              + "DIRECT_UPDATES with a comment saying which", entry.getKey())
          .isEqualTo(DIRECT_UPDATES.getOrDefault(entry.getKey(), Map.of()));
  }

  /**
   * The reporter's window: this transaction appends to the OUT chunk of {@code source} and runs {@code removal} on it, a
   * second thread commits an append to the same chunk, then this transaction commits - which must be refused.
   */
  private void runWindow(final Runnable removal) throws Exception {
    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      // AN IN-CHUNK APPEND, TRACKED BY THE EDGE-APPEND MERGE
      source.asVertex().newEdge("E", targets[COMMITTED_EDGES].asVertex());
      removal.run();

      // BUMPS THE VERSION OF THE CHUNK'S PAGE
      appendInOwnTransaction(other);

      assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      other.shutdownNow();
    }
  }

  private void appendInOwnTransaction(final ExecutorService other) {
    try {
      // BOUNDED, SO A STALLED COMMIT FAILS THE TEST INSTEAD OF HANGING THE SUITE
      other.submit(() -> database.transaction(() -> source.asVertex().newEdge("E", targets[COMMITTED_EDGES + 1].asVertex())))
          .get(60, TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createGraph() {
    database.command("sql", "CREATE VERTEX TYPE V BUCKETS 1").close();
    database.command("sql", "CREATE EDGE TYPE E BUCKETS 1").close();
    targets = new RID[COMMITTED_EDGES + 2];
    edges = new RID[COMMITTED_EDGES];
    database.transaction(() -> {
      source = database.newVertex("V").save().getIdentity();
      for (int i = 0; i < targets.length; i++)
        targets[i] = database.newVertex("V").set("id", i).save().getIdentity();
      for (int i = 0; i < COMMITTED_EDGES; i++)
        edges[i] = source.asVertex().newEdge("E", targets[i].asVertex()).getIdentity();
    });
  }

  private EdgeLinkedList outList() {
    final Vertex vertex = source.asVertex();
    return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.OUT);
  }

  /** Moves {@code it} onto the entry whose far end is {@code target}. */
  private static void advanceTo(final Iterator<?> it, final RID target) {
    while (it.hasNext()) {
      final Object item = it.next();
      final RID farEnd = switch (item) {
        case Edge edge -> edge.getIn();
        case Vertex vertex -> vertex.getIdentity();
        case Pair<?, ?> pair -> (RID) pair.getSecond();
        default -> throw new IllegalStateException("Unexpected item " + item);
      };
      if (farEnd.equals(target))
        return;
    }
    throw new IllegalStateException("No entry to " + target);
  }

  /** The edge RIDs the OUT list of {@code source} names, read without loading the edges. */
  private List<RID> outEntries() {
    final List<RID> entries = new ArrayList<>();
    database.transaction(() -> {
      final Iterator<Pair<RID, RID>> it = outList().entryIterator();
      while (it.hasNext())
        entries.add(it.next().getFirst());
    });
    return entries;
  }

  private void assertNoDanglingEntry() {
    final List<RID> dangling = new ArrayList<>();
    database.transaction(() -> {
      for (final RID edge : outEntries())
        try {
          edge.asEdge(true);
        } catch (final RecordNotFoundException e) {
          dangling.add(edge);
        }
    });
    assertThat(dangling).as("entries of the OUT list naming a deleted edge record").isEmpty();
  }
}
