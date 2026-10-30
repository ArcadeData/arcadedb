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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.EdgeEntryRIDIterator;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The hierarchy node space built from the tag graph: one node per distinct key, CSR parents deduplicated and sorted by value, depth
 * from the roots, cycles counted (never looped), duplicate keys collapsed. Every view built here is also checked for the CSR
 * invariants {@code AncestorExpander} relies on.
 */
class HierarchyBuilderTest extends TestHelper {
  private static final long         SEED       = 4242L;
  private static final List<String> TAG_VALUES = List.of("a/b/c", "a/d", "zzz");
  private static final String       INDEX_NAME = ColumnarTestData.TYPE + "[tagsbyitem]_columnar";
  private static final String       CYCLE_LOG  = "on a cycle of parent links";

  private HierarchyDefinition def;

  @Override
  protected void beginTest() {
    final ColumnarTestData data = new ColumnarTestData(SEED);
    data.tagHierarchy = new JSONObject().put("vertexType", ColumnarTestData.TAG_TYPE).put("key", "path")
        .put("parentEdge", ColumnarTestData.PARENT_EDGE);
    data.tagValues = TAG_VALUES;
    data.createSchema(database);
    data.insertAssets(database, 30);

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX_NAME);
    def = ((ColumnarIndex) index.getSubIndexes().getFirst()).getColumnarMetadata().getHierarchyDefinition();
  }

  @Test
  void buildsParentsDepthAndOrdinalMapping() {
    standardTree(ColumnarTestData.PARENT_EDGE);

    final HierarchyView v = build(def);
    final int c = v.nodeOfValue("a/b/c");
    assertThat(v.value(parentsOf(v, c)[0])).isEqualTo("a/b");
    assertThat(v.depth(c)).isEqualTo(2);
    assertThat(v.depth(v.nodeOfValue("x"))).isEqualTo(0);
    assertThat(v.nodeOfValue("a/d")).isGreaterThanOrEqualTo(0);

    assertThat(v.nodeCount()).isEqualTo(5);
    assertThat(v.generation()).isEqualTo(1);
    assertThat(v.depth(v.nodeOfValue("a"))).isEqualTo(0);
    assertThat(v.depth(v.nodeOfValue("a/b"))).isEqualTo(1);
    assertThat(v.depth(v.nodeOfValue("a/d"))).isEqualTo(1);
    assertThat(parentValues(v, "a")).isEmpty();
    assertThat(parentValues(v, "a/d")).containsExactly("a");
    assertThat(v.cycleNodes()).isEqualTo(0);
    assertThat(v.duplicateKeys()).isEqualTo(0);
    assertThat(v.nullKeys()).isEqualTo(0);
    final List<String> warnings = new ArrayList<>();
    capturingWarnings(warnings, () -> build(def));
    assertThat(warnings).noneMatch(w -> w.contains(CYCLE_LOG));
    for (int n = 0; n < v.nodeCount(); n++) {
      assertThat(v.orphan(n)).isFalse();
      assertThat(v.nodeOfValue(v.value(n))).isEqualTo(n);
    }
  }

  @Test
  void documentValueWithoutTagIsOrphanRoot() {
    standardTree(ColumnarTestData.PARENT_EDGE);

    // the assets do carry "zzz" (knob tagValues): the view has no node for it, the collector makes it an orphan root
    assertThat(assetTags()).contains("zzz");
    final HierarchyView v = build(def);
    assertThat(v.nodeOfValue("zzz")).isEqualTo(-1);
    assertThat(v.nodeOfValue(null)).isEqualTo(-1);
  }

  @Test
  void cycleIsCountedNotLooped() {
    final Map<String, MutableVertex> tags = standardTree(ColumnarTestData.PARENT_EDGE);
    database.transaction(() -> edge(tags, ColumnarTestData.PARENT_EDGE, "a/b/c", "a"));

    final List<String> warnings = new ArrayList<>();
    final HierarchyView v = capturingWarnings(warnings, () -> build(def));
    assertThat(v.cycleNodes()).isEqualTo(3);
    // one WARNING for the build, naming the index and one key on the cycle
    final List<String> cycleWarnings = warnings.stream().filter(w -> w.contains(CYCLE_LOG)).toList();
    assertThat(cycleWarnings).hasSize(1);
    assertThat(cycleWarnings.getFirst()).contains("'" + INDEX_NAME + "'").containsAnyOf("'a'", "'a/b'", "'a/b/c'");
    assertThat(parentValues(v, "a")).containsExactly("a/b/c");
    // no root reaches the cycle, nor the a/d branch hanging off it; x stays a root
    assertThat(v.depth(v.nodeOfValue("a"))).isEqualTo(-1);
    assertThat(v.depth(v.nodeOfValue("a/b/c"))).isEqualTo(-1);
    assertThat(v.depth(v.nodeOfValue("a/d"))).isEqualTo(-1);
    assertThat(v.depth(v.nodeOfValue("x"))).isEqualTo(0);
  }

  @Test
  void unrootedCycleHasDepthMinusOne() {
    database.transaction(() -> {
      final Map<String, MutableVertex> tags = tags("p", "q", "r");
      edge(tags, ColumnarTestData.PARENT_EDGE, "p", "q");
      edge(tags, ColumnarTestData.PARENT_EDGE, "q", "p");
    });

    final HierarchyView v = build(def);
    assertThat(v.depth(v.nodeOfValue("p"))).isEqualTo(-1);
    assertThat(v.depth(v.nodeOfValue("q"))).isEqualTo(-1);
    assertThat(v.depth(v.nodeOfValue("r"))).isEqualTo(0);
    assertThat(v.cycleNodes()).isEqualTo(2);
  }

  @Test
  void selfParentIsACycleOfOne() {
    database.transaction(() -> {
      final Map<String, MutableVertex> tags = tags("s", "t");
      edge(tags, ColumnarTestData.PARENT_EDGE, "s", "s");
      edge(tags, ColumnarTestData.PARENT_EDGE, "s", "t");
    });

    final HierarchyView v = build(def);
    assertThat(v.cycleNodes()).isEqualTo(1);
    assertThat(v.depth(v.nodeOfValue("s"))).isEqualTo(-1);
    assertThat(v.depth(v.nodeOfValue("t"))).isEqualTo(-1);
  }

  /**
   * p4 lies on the cycle p1 -> p4 -> p2 -> p3 -> p1 (child to parent), but a depth-first walk from p1 meets p2 first (parents sorted by
   * value), closes p1 -> p2 -> p3 -> p1 and only then reaches p4, whose parent p2 is already finished: counting the grey nodes of each
   * back edge would miss p4.
   */
  @Test
  void cycleReachedThroughAFinishedNodeCountsEveryMember() {
    database.transaction(() -> {
      final Map<String, MutableVertex> tags = tags("p1", "p2", "p3", "p4");
      edge(tags, ColumnarTestData.PARENT_EDGE, "p2", "p1");
      edge(tags, ColumnarTestData.PARENT_EDGE, "p4", "p1");
      edge(tags, ColumnarTestData.PARENT_EDGE, "p3", "p2");
      edge(tags, ColumnarTestData.PARENT_EDGE, "p1", "p3");
      edge(tags, ColumnarTestData.PARENT_EDGE, "p2", "p4");
    });

    final HierarchyView v = build(def);
    assertThat(v.cycleNodes()).isEqualTo(4);
    assertThat(parentValues(v, "p1")).containsExactly("p2", "p4");
  }

  @Test
  void duplicateKeysCollapse() {
    final Map<String, MutableVertex> tags = standardTree(ColumnarTestData.PARENT_EDGE);
    database.transaction(() -> {
      final MutableVertex second = database.newVertex(ColumnarTestData.TAG_TYPE).set("path", "a/d").save();
      tags.get("x").newEdge(ColumnarTestData.PARENT_EDGE, second);
      // a second edge a -> a/d through the duplicate: the union keeps one
      tags.get("a").newEdge(ColumnarTestData.PARENT_EDGE, second);
    });

    final HierarchyView v = build(def);
    assertThat(v.nodeCount()).isEqualTo(5);
    assertThat(v.duplicateKeys()).isEqualTo(1);
    assertThat(parentValues(v, "a/d")).containsExactly("a", "x");
  }

  @Test
  void nullKeysAreSkippedAndCounted() {
    final Map<String, MutableVertex> tags = standardTree(ColumnarTestData.PARENT_EDGE);
    database.transaction(() -> {
      final MutableVertex unnamed = database.newVertex(ColumnarTestData.TAG_TYPE).save();
      tags.get("x").newEdge(ColumnarTestData.PARENT_EDGE, unnamed);
      unnamed.newEdge(ColumnarTestData.PARENT_EDGE, tags.get("a/d"));
    });

    final HierarchyView v = build(def);
    assertThat(v.nodeCount()).isEqualTo(5);
    assertThat(v.nullKeys()).isEqualTo(1);
    assertThat(parentValues(v, "a/d")).containsExactly("a");
  }

  @Test
  void parentsOutsideTheVertexTypeAreIgnoredAndSubtypesIncluded() {
    database.command("sql", "CREATE VERTEX TYPE Folder");
    database.command("sql", "CREATE VERTEX TYPE SubTag EXTENDS " + ColumnarTestData.TAG_TYPE);
    final Map<String, MutableVertex> tags = standardTree(ColumnarTestData.PARENT_EDGE);
    database.transaction(() -> {
      final MutableVertex folder = database.newVertex("Folder").set("path", "folder").save();
      folder.newEdge(ColumnarTestData.PARENT_EDGE, tags.get("a/d"));
      final MutableVertex sub = database.newVertex("SubTag").set("path", "a/d/e").save();
      tags.get("a/d").newEdge(ColumnarTestData.PARENT_EDGE, sub);
    });

    final HierarchyView v = build(def);
    assertThat(v.nodeOfValue("folder")).isEqualTo(-1);
    assertThat(parentValues(v, "a/d")).containsExactly("a");
    assertThat(parentValues(v, "a/d/e")).containsExactly("a/d");
    assertThat(v.depth(v.nodeOfValue("a/d/e"))).isEqualTo(2);
  }

  /** An entry whose edge record is gone links nothing, and does not use up the parent: a live edge from the same parent still counts. */
  @Test
  void ghostParentEdgeIsIgnored() {
    final Map<String, MutableVertex> tags = standardTree(ColumnarTestData.PARENT_EDGE);
    database.transaction(() -> edge(tags, ColumnarTestData.PARENT_EDGE, "x", "a/d"));
    final RID[] ghost = new RID[1];
    database.transaction(() -> {
      for (final var e : database.lookupByRID(tags.get("a/d").getIdentity(), true).asVertex()
          .getEdges(Vertex.DIRECTION.IN, ColumnarTestData.PARENT_EDGE))
        if (e.getOut().equals(tags.get("a").getIdentity()))
          ghost[0] = e.getIdentity();
      // the record only, bypassing the graph: the entries pointing at it are left dangling
      ((LocalBucket) database.getSchema().getBucketById(ghost[0].getBucketId())).deleteRecord(ghost[0]);
    });
    assertThat(ghost[0]).isNotNull();

    final long skippedBefore = GhostEdgeReporter.getTotalSkipped();
    final HierarchyView v = build(def);
    assertThat(parentValues(v, "a/d")).containsExactly("x");
    assertThat(GhostEdgeReporter.getTotalSkipped()).isGreaterThan(skippedBefore);
    assertThat(v.depth(v.nodeOfValue("a/d"))).isEqualTo(1);
    assertThat(parentValues(v, "a/b")).containsExactly("a");

    // the ghost is left on purpose; the integrity check after the test wants it repaired
    database.command("sql", "CHECK DATABASE FIX");
  }

  /** A chain of 100000 tags: the depth walk and the cycle walk are iterative, a recursive one would overflow the thread stack. */
  @Test
  void deepChainNeedsNoRecursion() {
    final int length = 100_000;
    final MutableVertex[] chain = new MutableVertex[length];
    database.transaction(() -> {
      // the leaf first, so node 0 is the leaf and a depth-first walk from it climbs the whole chain
      for (int i = length - 1; i >= 0; i--)
        chain[i] = database.newVertex(ColumnarTestData.TAG_TYPE).set("path", "c" + i).save();
      for (int i = 1; i < length; i++)
        chain[i - 1].newEdge(ColumnarTestData.PARENT_EDGE, chain[i]);
    });

    final HierarchyView v = build(def);
    assertThat(v.nodeOfValue("c" + (length - 1))).isEqualTo(0);
    assertThat(v.depth(v.nodeOfValue("c" + (length - 1)))).isEqualTo(length - 1);
    assertThat(v.cycleNodes()).isEqualTo(0);

    // closed into one cycle: every node of it counts, none is reached from a root
    database.transaction(() -> chain[length - 1].newEdge(ColumnarTestData.PARENT_EDGE, chain[0]));
    final HierarchyView closed = capturingWarnings(new ArrayList<>(), () -> build(def));
    assertThat(closed.cycleNodes()).isEqualTo(length);
    assertThat(closed.depth(closed.nodeOfValue("c" + (length - 1)))).isEqualTo(-1);
  }

  @Test
  void lightweightParentEdgesAreFollowed() {
    database.command("sql", "CREATE EDGE TYPE LightParentOf LIGHTWEIGHT");
    final Map<String, MutableVertex> tags = standardTree(ColumnarTestData.PARENT_EDGE);
    standardEdges(tags, "LightParentOf");

    // the edges really are lightweight: no record behind the entry
    final EdgeEntryRIDIterator entries = ((DatabaseInternal) database).getGraphEngine()
        .getConnectedVertexEntries((VertexInternal) tags.get("a/d"), Vertex.DIRECTION.IN, "LightParentOf");
    assertThat(entries.hasNext()).isTrue();
    entries.next();
    assertThat(entries.getEdgeRID().getPosition()).isNegative();

    final HierarchyView regular = build(def);
    final HierarchyView light = build(new HierarchyDefinition(def.vertexType(), def.key(), "LightParentOf", Vertex.DIRECTION.IN));
    assertSameParents(light, regular);
    assertThat(parentValues(light, "a/b/c")).containsExactly("a/b");
  }

  @Test
  void parentDirectionOut() {
    database.command("sql", "CREATE EDGE TYPE ChildOf");
    final Map<String, MutableVertex> tags = standardTree(ColumnarTestData.PARENT_EDGE);
    database.transaction(() -> {
      // child -> parent
      edge(tags, "ChildOf", "a/b", "a");
      edge(tags, "ChildOf", "a/b/c", "a/b");
      edge(tags, "ChildOf", "a/d", "a");
    });

    final HierarchyView regular = build(def);
    final HierarchyView out = build(new HierarchyDefinition(def.vertexType(), def.key(), "ChildOf", Vertex.DIRECTION.OUT));
    assertSameParents(out, regular);
    assertThat(out.depth(out.nodeOfValue("a/b/c"))).isEqualTo(2);
    // the same edges read with IN make every child a parent
    final HierarchyView reversed = build(new HierarchyDefinition(def.vertexType(), def.key(), "ChildOf", Vertex.DIRECTION.IN));
    assertThat(parentValues(reversed, "a")).containsExactly("a/b", "a/d");
  }

  @Test
  void parentsAreSortedByValue() {
    database.transaction(() -> {
      final Map<String, MutableVertex> tags = tags("a", "x", "a/d", "m");
      // a/d: x inserted first; m: a inserted first, so one of the two disagrees with the newest-first edge order either way
      edge(tags, ColumnarTestData.PARENT_EDGE, "x", "a/d");
      edge(tags, ColumnarTestData.PARENT_EDGE, "a", "a/d");
      edge(tags, ColumnarTestData.PARENT_EDGE, "a", "m");
      edge(tags, ColumnarTestData.PARENT_EDGE, "x", "m");
    });

    for (int rebuild = 0; rebuild < 3; rebuild++) {
      final HierarchyView v = build(def);
      assertThat(parentValues(v, "a/d")).containsExactly("a", "x");
      assertThat(parentValues(v, "m")).containsExactly("a", "x");
    }
  }

  @Test
  void tagTreeFixtureBuildsTheWholeTreeWithParentsInRange() {
    final List<String> paths = ColumnarTestData.createTagTree(database, SEED, 3, 3);
    assertThat(paths).hasSize(3 + 9 + 27);
    assertThat(new HashSet<>(paths)).hasSize(paths.size());
    // the seed shuffles the creation order only: the set of paths is the same for every seed
    assertThat(paths).isNotEqualTo(paths.stream().sorted().toList());
    assertThat(database.getSchema().getType(ColumnarTestData.TAG_TYPE).getIndexesByProperties("path")).isNotEmpty();

    final HierarchyView v = HierarchyBuilder.build((DatabaseInternal) database, def, 7, INDEX_NAME);
    assertWellFormed(v);
    assertThat(v.generation()).isEqualTo(7);
    assertThat(v.nodeCount()).isEqualTo(paths.size());
    assertThat(v.cycleNodes()).isEqualTo(0);
    for (final String path : paths) {
      final int node = v.nodeOfValue(path);
      assertThat(node).isGreaterThanOrEqualTo(0);
      final int slash = path.lastIndexOf('/');
      assertThat(v.depth(node)).isEqualTo((int) path.chars().filter(ch -> ch == '/').count());
      if (slash < 0)
        assertThat(parentValues(v, path)).isEmpty();
      else
        assertThat(parentValues(v, path)).containsExactly(path.substring(0, slash));
    }
  }

  @Test
  void assetsDrawTheirTagsFromTagValues() {
    assertThat(def).isEqualTo(new HierarchyDefinition(ColumnarTestData.TAG_TYPE, "path", ColumnarTestData.PARENT_EDGE, Vertex.DIRECTION.IN));
    final Set<String> carried = assetTags();
    assertThat(carried).isNotEmpty();
    assertThat(TAG_VALUES).containsAll(carried);
  }

  private HierarchyView build(final HierarchyDefinition definition) {
    final HierarchyView view = HierarchyBuilder.build((DatabaseInternal) database, definition, 1, INDEX_NAME);
    assertWellFormed(view);
    return view;
  }

  /** The CSR invariants AncestorExpander asserts: every parent index is -1 or in range, offsets monotonic, lists without duplicates. */
  private static void assertWellFormed(final HierarchyView v) {
    final int[] offsets = v.parentOffsets();
    final int[] parents = v.parents();
    assertThat(offsets).hasSize(v.nodeCount() + 1);
    assertThat(offsets[0]).isEqualTo(0);
    assertThat(offsets[v.nodeCount()]).isEqualTo(parents.length);
    for (int n = 0; n < v.nodeCount(); n++) {
      assertThat(offsets[n + 1]).isGreaterThanOrEqualTo(offsets[n]);
      final Set<Integer> seen = new HashSet<>();
      for (int p = offsets[n]; p < offsets[n + 1]; p++) {
        assertThat(parents[p]).isBetween(-1, v.nodeCount() - 1);
        assertThat(seen.add(parents[p])).isTrue();
      }
      assertThat(v.depth(n)).isGreaterThanOrEqualTo(-1);
    }
  }

  private static void assertSameParents(final HierarchyView actual, final HierarchyView expected) {
    assertThat(actual.nodeCount()).isEqualTo(expected.nodeCount());
    for (int n = 0; n < expected.nodeCount(); n++) {
      final String value = (String) expected.value(n);
      assertThat(parentValues(actual, value)).as(value).isEqualTo(parentValues(expected, value));
      assertThat(actual.depth(actual.nodeOfValue(value))).as(value).isEqualTo(expected.depth(n));
    }
  }

  private static int[] parentsOf(final HierarchyView view, final int node) {
    final int from = view.parentOffsets()[node];
    final int to = view.parentOffsets()[node + 1];
    final int[] result = new int[to - from];
    System.arraycopy(view.parents(), from, result, 0, result.length);
    return result;
  }

  private static List<Object> parentValues(final HierarchyView view, final String value) {
    final int node = view.nodeOfValue(value);
    assertThat(node).as(value).isGreaterThanOrEqualTo(0);
    final List<Object> values = new ArrayList<>();
    for (final int parent : parentsOf(view, node))
      values.add(view.value(parent));
    return values;
  }

  /** Tags a, a/b, a/b/c, a/d, x with the edges a -> a/b, a/b -> a/b/c, a -> a/d of {@code edgeType}, in one transaction. */
  private Map<String, MutableVertex> standardTree(final String edgeType) {
    final Map<String, MutableVertex> tags = new HashMap<>();
    database.transaction(() -> tags.putAll(tags("a", "a/b", "a/b/c", "a/d", "x")));
    standardEdges(tags, edgeType);
    return tags;
  }

  private void standardEdges(final Map<String, MutableVertex> tags, final String edgeType) {
    database.transaction(() -> {
      edge(tags, edgeType, "a", "a/b");
      edge(tags, edgeType, "a/b", "a/b/c");
      edge(tags, edgeType, "a", "a/d");
    });
  }

  private Map<String, MutableVertex> tags(final String... paths) {
    final Map<String, MutableVertex> tags = new HashMap<>();
    for (final String path : paths)
      tags.put(path, database.newVertex(ColumnarTestData.TAG_TYPE).set("path", path).save());
    return tags;
  }

  private static void edge(final Map<String, MutableVertex> tags, final String edgeType, final String from, final String to) {
    tags.get(from).newEdge(edgeType, tags.get(to));
  }

  /** Runs {@code action} with the WARNINGs of every logger call collected into {@code warnings}, formatted. */
  private static <T> T capturingWarnings(final List<String> warnings, final Supplier<T> action) {
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(warnings));
    try {
      return action.get();
    } finally {
      LogManager.instance().setLogger(previous);
    }
  }

  private static final class CapturingLogger implements Logger {
    private final List<String> warnings;

    private CapturingLogger(final List<String> warnings) {
      this.warnings = warnings;
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
        final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
        final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      log(requester, level, message, exception, context,
          new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17 });
    }

    @Override
    public synchronized void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      if (level == Level.WARNING && message != null)
        warnings.add(args == null || args.length == 0 ? message : String.format(message, args));
    }

    @Override
    public void flush() {
    }
  }

  private Set<String> assetTags() {
    final Set<String> carried = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + ColumnarTestData.TYPE)) {
      while (rs.hasNext()) {
        final Document asset = rs.next().toElement();
        final List<String> tags = asset.getList("tags");
        if (tags != null)
          carried.addAll(tags);
      }
    }
    return carried;
  }
}
