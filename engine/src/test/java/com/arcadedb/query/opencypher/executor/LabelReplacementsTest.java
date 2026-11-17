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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct tests for the machinery behind a Cypher label write, so a regression in it is localised here rather than
 * inferred from a failing query: the replacement chain a twice-relabelled node builds, the edges and edge
 * properties carried over to the new record, and the row values - plain, nested, and path-shaped - redirected onto
 * it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LabelReplacementsTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/test-label-replacements").create();
    database.getSchema().getOrCreateVertexType("A");
    database.getSchema().getOrCreateVertexType("B");
    database.getSchema().getOrCreateVertexType("C");
    database.getSchema().getOrCreateEdgeType("E");
    database.getSchema().buildEdgeType().withName("Light").withLightweight(true).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void resolveFollowsAChainOfReplacements() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex original = database.newVertex("A").set("name", "n").save();

      final Vertex once = replacements.replace(original, "B");
      final Vertex twice = replacements.replace(once, "C");

      assertThat(replacements.resolve(original)).isSameAs(twice);
      assertThat(replacements.resolve(once)).isSameAs(twice);
      assertThat(replacements.resolve(twice)).isSameAs(twice);
      assertThat(twice.getTypeName()).isEqualTo("C");
      assertThat(twice.<String>get("name")).isEqualTo("n");
    });
  }

  @Test
  void resolveLeavesAnUntouchedVertexAlone() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex untouched = database.newVertex("A").save();

      assertThat(replacements.isEmpty()).isTrue();
      assertThat(replacements.resolve(untouched)).isSameAs(untouched);
      assertThat(replacements.resolve(null)).isNull();
    });
  }

  @Test
  void replaceCarriesEdgesAndTheirPropertiesOver() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex target = database.newVertex("A").set("name", "target").save();
      final MutableVertex other = database.newVertex("A").set("name", "other").save();
      target.newEdge("E", other, "w", 7);
      other.newEdge("E", target, "w", 9);
      target.newEdge("E", target, "w", 11);
      target.newEdge("Light", other);

      final Vertex replacement = replacements.replace(target, "B");

      assertThat(replacement.getTypeName()).isEqualTo("B");
      assertThat(edgeWeights(replacement, Vertex.DIRECTION.OUT)).containsExactlyInAnyOrder(7, 11, null);
      assertThat(edgeWeights(replacement, Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(9, 11);
      // The self-loop is one edge, seen once from each side, not two.
      assertThat(replacement.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(2);
    });
  }

  @Test
  void redirectRewritesPlainNestedAndPathShapedRowValues() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex start = database.newVertex("A").set("name", "start").save();
      final MutableVertex end = database.newVertex("A").set("name", "end").save();
      final Edge edge = start.newEdge("E", end, "w", 1);

      final TraversalPath path = new TraversalPath(start);
      path.addStep(edge, end);

      final ResultInternal row = new ResultInternal();
      row.setProperty("n", start);
      row.setProperty("untouched", end);
      row.setProperty("r", edge);
      row.setProperty("p", path);
      row.setProperty("collected", List.of(start, end));
      row.setProperty("mapped", Map.of("k", start));
      row.setProperty("scalar", 42);

      final Vertex replacement = replacements.replace(start, "B");
      replacements.redirect(row);

      assertThat(row.<Vertex>getProperty("n")).isSameAs(replacement);
      assertThat(row.<Vertex>getProperty("untouched")).isSameAs(end);
      assertThat(row.<Edge>getProperty("r").getIdentity()).isNotEqualTo(edge.getIdentity());
      assertThat(row.<Edge>getProperty("r").<Integer>get("w")).isEqualTo(1);
      assertThat(row.<Integer>getProperty("scalar")).isEqualTo(42);

      final TraversalPath redirected = row.getProperty("p");
      assertThat(redirected.getVertices().get(0)).isSameAs(replacement);
      assertThat(redirected.getVertices().get(1)).isSameAs(end);
      assertThat(redirected.getEdges().get(0).getIdentity()).isEqualTo(row.<Edge>getProperty("r").getIdentity());

      assertThat(row.<List<Vertex>>getProperty("collected").get(0)).isSameAs(replacement);
      assertThat(row.<List<Vertex>>getProperty("collected").get(1)).isSameAs(end);
      assertThat(row.<Map<String, Vertex>>getProperty("mapped").get("k")).isSameAs(replacement);
    });
  }

  @Test
  void redirectRewritesALightweightEdgeOntoTheReplacement() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex start = database.newVertex("A").set("name", "start").save();
      final MutableVertex end = database.newVertex("A").set("name", "end").save();
      final Edge light = start.newEdge("Light", end);

      final ResultInternal row = new ResultInternal();
      row.setProperty("r", light);

      final Vertex replacement = replacements.replace(start, "B");
      replacements.redirect(row);

      // A lightweight edge has no record to address, but its identity is the (type, out, in) triple - so the one
      // hanging off the replacement is a different edge, and the row has to be pointed at it.
      final Edge redirected = row.getProperty("r");
      assertThat(redirected).isNotSameAs(light);
      assertThat(redirected.getOut()).isEqualTo(replacement.getIdentity());
      assertThat(redirected.getIn()).isEqualTo(end.getIdentity());
      assertThat(light.getOut()).isNotEqualTo(replacement.getIdentity());
    });
  }

  @Test
  void redirectLeavesARowWithNothingToRewriteUntouched() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex vertex = database.newVertex("A").save();
      final List<Vertex> collected = List.of(vertex);

      final ResultInternal row = new ResultInternal();
      row.setProperty("n", vertex);
      row.setProperty("collected", collected);

      replacements.redirect(row);

      assertThat(row.<Vertex>getProperty("n")).isSameAs(vertex);
      // Nothing inside moved, so the list is the same instance rather than a rebuilt copy.
      assertThat(row.<List<Vertex>>getProperty("collected")).isSameAs(collected);
    });
  }

  @Test
  void restoreDiscardsReplacementsRecordedAfterTheSnapshot() {
    database.transaction(() -> {
      final LabelReplacements replacements = new LabelReplacements();
      final MutableVertex first = database.newVertex("A").set("name", "first").save();
      final MutableVertex second = database.newVertex("A").set("name", "second").save();

      final Vertex firstReplacement = replacements.replace(first, "B");
      final LabelReplacements.Snapshot snapshot = replacements.copy();

      // Simulates issue #6367's finding: a failed, rolled-back MergeStep attempt records a replacement
      // (here, of `second`) that the retried attempt must not see - the vertex it points at was rolled
      // back along with everything else that attempt wrote.
      replacements.replace(second, "C");
      assertThat(replacements.resolve(second)).isNotSameAs(second);

      replacements.restore(snapshot);

      // The entry recorded before the snapshot survives the restore...
      assertThat(replacements.resolve(first)).isSameAs(firstReplacement);
      // ...but the one recorded after it is gone, exactly as if that attempt had never run.
      assertThat(replacements.resolve(second)).isSameAs(second);
    });
  }

  private boolean isHeld(final RID rid) {
    return ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).isPositionHeld(rid.getPosition());
  }

  private static List<Object> edgeWeights(final Vertex vertex, final Vertex.DIRECTION direction) {
    final List<Object> weights = new ArrayList<>();
    for (final Edge edge : vertex.getEdges(direction))
      weights.add(edge.get("w"));
    return weights;
  }

  /**
   * Batches of {@code CALL { ... } IN TRANSACTIONS} commit on their own, and the engine hands the slot a committed delete
   * freed to a later insert. A replacement created on the slot of a record replaced earlier in the statement would make
   * that RID name two records: an old binding of the replaced one and the new record could not be told apart, and either
   * resolve answered the wrong one or the two chains formed a cycle. A replacement never takes such a slot.
   */
  @Test
  // A hang detector: a RID naming two records let resolve() follow a cycle forever
  @Timeout(60)
  void aReplacementNeverTakesTheSlotOfARecordReplacedEarlierInTheStatement() {
    final LabelReplacements replacements = new LabelReplacements();
    final Vertex[] original = new Vertex[1];
    database.transaction(() -> original[0] = database.newVertex("A").set("name", "old").save());
    final RID freed = original[0].getIdentity();

    // Batch 1 relabels it A -> B and commits: the original's slot in A's bucket is free
    final Vertex[] firstReplacement = new Vertex[1];
    database.transaction(() -> firstReplacement[0] = replacements.replace(original[0], "B"));

    // The freed slot is held while the map can answer for it: no new A vertex takes it
    database.begin();
    final RID probe = database.newVertex("A").save().getIdentity();
    database.rollback();
    assertThat(probe).as("a new A vertex does not take the held slot").isNotEqualTo(freed);

    // Batch 2 relabels another vertex B -> A: its replacement is created in A's bucket, where the freed slot is
    final Vertex[] other = new Vertex[1];
    database.transaction(() -> other[0] = database.newVertex("B").set("name", "other").save());
    final Vertex[] secondReplacement = new Vertex[1];
    database.transaction(() -> secondReplacement[0] = replacements.replace(other[0], "A"));

    assertThat(secondReplacement[0].getIdentity()).isNotEqualTo(freed);
    assertThat(replacements.resolve(original[0])).isSameAs(firstReplacement[0]);
    assertThat(replacements.resolve(other[0])).isSameAs(secondReplacement[0]);
    assertThat(replacements.resolve(secondReplacement[0])).isSameAs(secondReplacement[0]);
    assertThat(database.countType("A", false)).isEqualTo(1);
    assertThat(database.countType("B", false)).isEqualTo(1);

    // Once the statement is over the slot is the engine's again: the hold was all that kept it
    replacements.releaseHeldPositions();
    database.begin();
    final RID afterRelease = database.newVertex("A").save().getIdentity();
    database.rollback();
    assertThat(afterRelease).as("a new A vertex takes the released slot").isEqualTo(freed);
  }

  /**
   * The edge side of {@link #aReplacementNeverTakesTheSlotOfARecordReplacedEarlierInTheStatement}: an edge re-attached
   * once per relabelled endpoint, across committed batches, never lands on the slot a replaced edge freed, so an edge
   * bound before the first batch still resolves - and terminates - on the live copy incident to both live endpoints.
   */
  @Test
  // A hang detector: a RID naming two edges let resolveEdge() follow a cycle forever
  @Timeout(60)
  void aReattachedEdgeNeverTakesTheSlotOfAnEdgeReplacedEarlierInTheStatement() {
    final LabelReplacements replacements = new LabelReplacements();
    final Vertex[] source = new Vertex[1];
    final Vertex[] target = new Vertex[1];
    final Edge[] bound = new Edge[1];
    database.transaction(() -> {
      source[0] = database.newVertex("A").set("name", "s").save();
      target[0] = database.newVertex("B").set("name", "t").save();
      bound[0] = ((MutableVertex) source[0]).newEdge("E", target[0], "w", 7);
    });
    final RID freed = bound[0].getIdentity();

    // Batch 1 relabels the source and commits: the bound edge is re-created and its slot in E's bucket is free
    final Vertex[] newSource = new Vertex[1];
    database.transaction(() -> newSource[0] = replacements.replace(source[0], "C"));
    final Edge firstCopy = replacements.resolveEdge(bound[0]);
    assertThat(firstCopy.getIdentity()).isNotEqualTo(freed);

    // The freed edge slot is held while the map can answer for it: no new E edge takes it
    database.begin();
    final RID probe = database.newVertex("A").save().newEdge("E", database.newVertex("B").save()).getIdentity();
    database.rollback();
    assertThat(probe).as("a new E edge does not take the held slot").isNotEqualTo(freed);

    // Batch 2 relabels the target: the copy is re-attached again, in E's bucket, where the freed slot is
    final Vertex[] newTarget = new Vertex[1];
    database.transaction(() -> newTarget[0] = replacements.replace(target[0], "C"));

    final Edge live = replacements.resolveEdge(bound[0]);
    assertThat(live.getIdentity()).isNotEqualTo(freed);
    assertThat(replacements.resolveEdge(firstCopy)).isSameAs(live);
    assertThat(replacements.resolveEdge(live)).isSameAs(live);
    assertThat(live.getOut()).isEqualTo(newSource[0].getIdentity());
    assertThat(live.getIn()).isEqualTo(newTarget[0].getIdentity());
    assertThat(live.<Integer>get("w")).isEqualTo(7);
    assertThat(database.countType("E", false)).isEqualTo(1);

    replacements.releaseHeldPositions();
    database.begin();
    final RID afterRelease = database.newVertex("A").save().newEdge("E", database.newVertex("B").save()).getIdentity();
    database.rollback();
    assertThat(afterRelease).as("a new E edge takes the released slot").isEqualTo(freed);
  }

  /**
   * Issue #12336: an ordinary insert after a committed batch - a CREATE of the same statement, or a concurrent transaction -
   * used to take the slot of a record the statement replaced. The new record then had the replaced record's RID, and a binding
   * of it was redirected to the replacement. The slot is held instead, so the new record has a RID of its own and resolves to
   * itself, while the old binding still resolves to the replacement.
   */
  @Test
  void aPlainInsertNeverTakesTheSlotOfAReplacedRecordWhileTheStatementRuns() {
    final LabelReplacements replacements = new LabelReplacements();
    final Vertex[] original = new Vertex[1];
    database.transaction(() -> original[0] = database.newVertex("A").set("name", "old").save());
    final RID freed = original[0].getIdentity();

    final Vertex[] replacement = new Vertex[1];
    database.transaction(() -> replacement[0] = replacements.replace(original[0], "B"));

    final Vertex[] fresh = new Vertex[1];
    database.transaction(() -> fresh[0] = database.newVertex("A").set("fresh", true).save());

    assertThat(fresh[0].getIdentity()).isNotEqualTo(freed);
    final Vertex readBack = database.lookupByRID(fresh[0].getIdentity(), true).asVertex();
    assertThat(replacements.resolve(readBack)).isSameAs(readBack);
    assertThat(replacements.resolve(original[0])).isSameAs(replacement[0]);
    assertThat(isHeld(freed)).isTrue();

    replacements.releaseHeldPositions();
    assertThat(isHeld(freed)).isFalse();
  }

  /**
   * Two running statements can both hold one position (both relabel the same vertex before either commits): the one that
   * ends first must not free the slot the other still answers for.
   */
  @Test
  void aPositionStaysHeldUntilEveryHolderReleasedIt() {
    final Vertex[] vertex = new Vertex[1];
    database.transaction(() -> vertex[0] = database.newVertex("A").save());
    final RID rid = vertex[0].getIdentity();
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());

    bucket.holdPosition(rid.getPosition());
    bucket.holdPosition(rid.getPosition());
    bucket.releaseHeldPosition(rid.getPosition());
    assertThat(isHeld(rid)).isTrue();
    bucket.releaseHeldPosition(rid.getPosition());
    assertThat(isHeld(rid)).isFalse();
    // a release with no hold left is a no-op, never a negative count
    bucket.releaseHeldPosition(rid.getPosition());
    bucket.holdPosition(rid.getPosition());
    assertThat(isHeld(rid)).isTrue();
    bucket.releaseHeldPosition(rid.getPosition());
    assertThat(isHeld(rid)).isFalse();
  }

  /** Ending a map twice gives each hold back once: the second call must not release a hold another statement took. */
  @Test
  void releasingTwiceDoesNotReleaseAnotherHoldersPosition() {
    final LabelReplacements replacements = new LabelReplacements();
    final Vertex[] original = new Vertex[1];
    database.transaction(() -> original[0] = database.newVertex("A").save());
    final RID rid = original[0].getIdentity();
    database.transaction(() -> replacements.replace(original[0], "B"));

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    bucket.holdPosition(rid.getPosition()); // another statement's hold
    replacements.releaseHeldPositions();
    replacements.releaseHeldPositions();
    assertThat(isHeld(rid)).isTrue();
    bucket.releaseHeldPosition(rid.getPosition());
    assertThat(isHeld(rid)).isFalse();
  }

  /**
   * A page whose every record was replaced, its whole record table included, takes records again once the statement released
   * its holds: holding a slot never leaves the page in a state the allocator stops offering.
   */
  @Test
  void aPageEmptiedWhileItsSlotsAreHeldIsReusedOnceTheHoldsAreReleased() {
    database.getSchema().buildDocumentType().withName("Small").withTotalBuckets(1).create();
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Small").getBuckets(false).getFirst();
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < maxRecordsInPage; i++)
        rids.add(database.newDocument("Small").save().getIdentity());
    });
    final RID first = rids.getFirst();
    assertThat(rids.getLast().getPosition()).as("the record table of page 0 is full").isEqualTo(first.getPosition() + maxRecordsInPage - 1);

    // What a statement relabelling every record of the page does to it: every slot held, every record deleted and committed
    for (final RID rid : rids)
      bucket.holdPosition(rid.getPosition());
    database.transaction(() -> {
      for (final RID rid : rids)
        database.deleteRecord(rid.asDocument());
    });

    final RID[] whileHeld = new RID[1];
    database.begin();
    whileHeld[0] = database.newDocument("Small").save().getIdentity();
    database.rollback();
    assertThat(whileHeld[0].getPosition()).as("no held slot of page 0 is handed out").isGreaterThanOrEqualTo(maxRecordsInPage);

    for (final RID rid : rids)
      bucket.releaseHeldPosition(rid.getPosition());
    final RID[] afterRelease = new RID[1];
    database.begin();
    afterRelease[0] = database.newDocument("Small").save().getIdentity();
    database.rollback();
    assertThat(afterRelease[0]).as("the emptied page takes records again").isEqualTo(first);
  }

  /** A rolled-back attempt deleted nothing in the end, so the positions it held go back with the entries it recorded. */
  @Test
  void restoringASnapshotReleasesThePositionsTheDiscardedEntriesHeld() {
    final LabelReplacements replacements = new LabelReplacements();
    final Vertex[] original = new Vertex[1];
    database.transaction(() -> original[0] = database.newVertex("A").save());
    final RID rid = original[0].getIdentity();

    final LabelReplacements.Snapshot before = replacements.copy();
    database.begin();
    replacements.replace(original[0], "B");
    database.rollback();
    assertThat(isHeld(rid)).isTrue();

    replacements.restore(before);
    assertThat(isHeld(rid)).isFalse();
    assertThat(replacements.isEmpty()).isTrue();
  }
}
