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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12507: a Graph Analytical View keeps one CSR slice per concrete edge type, keyed by the type's NAME, and the check
 * of #12032 and #12039 asked only whether a type of that name is still an edge type the view covers. Two DDL statements with no read
 * of the view between them can leave a type of the same name behind that is not the one the slice was scanned from: a type dropped
 * and created again, a rename swap. And a type detached from the listed type, written to and attached again was taken as joined by
 * its name alone, though nobody of the view heard the edge written while it was detached. The view stayed READY and answered from
 * the old slice. A slice is now identified by the buckets of its type, which survive a rename and do not survive a drop.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12507GavEdgeSliceIdentityTest extends TestHelper {
  private static final String E0_COUNT = "MATCH (a:V)-[e:E0]->(b:V) RETURN count(*) AS c";
  private static final String VIEW_HOP = "GAVExpandAll";

  /** DROP TYPE ... UNSAFE leaves the vertices' edge lists pointing at the dropped edges, by design. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
  void aTypeDroppedAndCreatedAgainUnderTheSameNameIsNotServedFromTheOldSlice(final String mode) {
    final GraphAnalyticalView view = viewOverTheFamily(mode, false);
    final CountDownLatch gate = holdTheRebuild(view);
    try {
      // BACK TO BACK: NO READ OF THE VIEW BETWEEN THE TWO
      database.command("sql", "DROP TYPE F UNSAFE").close();
      database.command("sql", "CREATE EDGE TYPE F EXTENDS E0").close();

      assertThat(view.isReady()).as("the CSR holds the edges of the F that was dropped").isFalse();
      assertThat(count(E0_COUNT)).as("the edges of the old F are gone").isEqualTo(3L);
      assertThat(sqlCount("SELECT count(@rid) AS c FROM E0")).isEqualTo(3L);
    } finally {
      release(view, gate);
    }

    rebuildIfOff(view, mode);
    assertServedWith(view, 3);
    assertThat(walkedOut("F")).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
  void aRenameSwapIsNotServedFromTheOldSlices(final String mode) {
    final GraphAnalyticalView view = viewOverTheFamily(mode, true);
    assertThat(walkedOut("F")).isEqualTo(3L);

    final CountDownLatch gate = holdTheRebuild(view);
    try {
      // BACK TO BACK: F's edges move to F_old, and the empty G is the F of the schema from now on
      database.command("sql", "ALTER TYPE F NAME F_old").close();
      database.command("sql", "ALTER TYPE G NAME F").close();

      assertThat(view.isReady()).as("the CSR holds the edges of the old F under the name F").isFalse();
      assertThat(walkedOut("F_old")).isEqualTo(3L);
      assertThat(walkedOut("F")).isZero();
      assertThat(count("MATCH ()-[e:F_old]->() RETURN count(*) AS c")).isEqualTo(3L);
      assertThat(count("MATCH ()-[e:F]->() RETURN count(*) AS c")).isZero();
      assertThat(count(E0_COUNT)).isEqualTo(6L);
    } finally {
      release(view, gate);
    }

    rebuildIfOff(view, mode);
    assertServedWith(view, 6);
    assertThat(walkedOut("F_old")).isEqualTo(3L);
    assertThat(walkedOut("F")).isZero();
    assertThat(count("MATCH ()-[e:F_old]->() RETURN count(*) AS c")).isEqualTo(3L);
    assertThat(count("MATCH ()-[e:F]->() RETURN count(*) AS c")).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
  void aTypeDetachedWrittenToAndAttachedAgainIsNotServedWithoutThatEdge(final String mode) {
    final GraphAnalyticalView view = viewOverTheFamily(mode, false);
    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "ALTER TYPE F SUPERTYPE -E0").close();
      write("CREATE EDGE F FROM (SELECT FROM V WHERE i = 0) TO (SELECT FROM V WHERE i = 3)");
      database.command("sql", "ALTER TYPE F SUPERTYPE +E0").close();

      assertThat(sqlCount("SELECT count(@rid) AS c FROM E0")).as("the reference: the record scan").isEqualTo(7L);
      assertThat(view.isReady()).as("an edge of F was written while nobody of the view was listening").isFalse();
      assertThat(count(E0_COUNT)).isEqualTo(7L);
    } finally {
      release(view, gate);
    }

    rebuildIfOff(view, mode);
    assertServedWith(view, 7);
    assertThat(walkedOut("F")).isEqualTo(4L);
  }

  /** The same with a lightweight edge, which has no record and reaches the view through the edge-write listener. */
  @Test
  void aLightweightEdgeWrittenWhileTheTypeWasDetachedIsNotMissed() {
    final GraphAnalyticalView view = viewOverTheFamily("SYNCHRONOUS", false);
    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "ALTER TYPE F SUPERTYPE -E0").close();
      database.transaction(() -> {
        final Vertex from = database.query("sql", "SELECT FROM V WHERE i = 0").next().getVertex().get();
        final Vertex to = database.query("sql", "SELECT FROM V WHERE i = 2").next().getVertex().get();
        from.newLightEdge("F", to);
      });
      database.command("sql", "ALTER TYPE F SUPERTYPE +E0").close();

      assertThat(view.isReady()).as("an edge of F was written while nobody of the view was listening").isFalse();
      assertThat(count(E0_COUNT)).isEqualTo(7L);
    } finally {
      release(view, gate);
    }
    // NO RECORD STANDS FOR A LIGHTWEIGHT EDGE: THE RECORD SCAN CANNOT COUNT IT
    Awaitility.await("the rebuild").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
    assertThat(view.getEdgeCount()).isEqualTo(7);
    assertThat(count(E0_COUNT)).isEqualTo(7L);
    assertThat(cypherPlan(E0_COUNT)).contains(VIEW_HOP);
  }

  /** What the rule must not cost: a type detached and attached again, or given a bucket, holds what the view holds. */
  @Test
  void aTypeDetachedAndAttachedAgainWithNoWriteDoesNotRebuildTheView() {
    final GraphAnalyticalView view = viewOverTheFamily("SYNCHRONOUS", false);
    final Object base = view.pinned().snapshot().csrPerType;

    database.command("sql", "ALTER TYPE F SUPERTYPE -E0").close();
    database.command("sql", "ALTER TYPE F SUPERTYPE +E0").close();

    assertThat(view.isReady()).as("nothing was written while F was detached").isTrue();
    assertThat(count(E0_COUNT)).isEqualTo(6L);
    assertThat(cypherPlan(E0_COUNT)).contains(VIEW_HOP);
    assertThat(view.pinned().snapshot().csrPerType).as("no rebuild").isSameAs(base);
  }

  @Test
  void aTypeGivenAnotherBucketKeepsItsSliceAndDoesNotRebuildTheView() {
    final GraphAnalyticalView view = viewOverTheFamily("SYNCHRONOUS", false);
    final Object base = view.pinned().snapshot().csrPerType;

    database.command("sql", "CREATE BUCKET F_extra").close();
    database.command("sql", "ALTER TYPE F BUCKET +F_extra").close();

    assertThat(view.isReady()).as("F owns every bucket its slice was scanned from").isTrue();
    assertThat(count(E0_COUNT)).isEqualTo(6L);
    assertThat(view.pinned().snapshot().csrPerType).as("no rebuild").isSameAs(base);

    // AN EDGE OF THE NEW BUCKET IS HEARD LIKE ANY OTHER OF A COVERED TYPE
    write("CREATE EDGE F BUCKET F_extra FROM (SELECT FROM V WHERE i = 0) TO (SELECT FROM V WHERE i = 2)");
    assertThat(view.isReady()).isTrue();
    assertThat(count(E0_COUNT)).isEqualTo(7L);
    assertThat(sqlCount("SELECT count(@rid) AS c FROM E0")).isEqualTo(7L);
  }

  /** The slice of a type created after the build lives only in the overlay, and is identified by the buckets of its edges. */
  @Test
  void anOverlayOnlySliceOfATypeDroppedAndCreatedAgainIsNotServed() {
    final GraphAnalyticalView view = viewOverTheFamily("SYNCHRONOUS", false);
    database.command("sql", "CREATE EDGE TYPE H EXTENDS E0").close();
    write("CREATE EDGE H FROM (SELECT FROM V WHERE i = 0) TO (SELECT FROM V WHERE i = 3)");
    write("CREATE EDGE H FROM (SELECT FROM V WHERE i = 1) TO (SELECT FROM V WHERE i = 3)");
    assertThat(count(E0_COUNT)).as("the overlay holds the edges of H").isEqualTo(8L);
    assertThat(view.isReady()).isTrue();

    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "DROP TYPE H UNSAFE").close();
      database.command("sql", "CREATE EDGE TYPE H EXTENDS E0").close();

      assertThat(view.isReady()).as("the overlay holds the edges of the H that was dropped").isFalse();
      assertThat(count(E0_COUNT)).isEqualTo(6L);
    } finally {
      release(view, gate);
    }

    Awaitility.await("the rebuild").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
    assertThat(count(E0_COUNT)).isEqualTo(6L);
    assertThat(view.getEdgeCount()).isEqualTo(6);
  }

  /** What a slice is made of: the buckets its type owned when the scan read it, and still the same after a restore from disk. */
  @Test
  void theSnapshotRecordsTheBucketsEachSliceWasScannedFrom() {
    final GraphAnalyticalView view = viewOverTheFamily("SYNCHRONOUS", true);
    final GraphAnalyticalView.Snapshot built = view.pinned().snapshot();
    assertThat(built.edgeTypeBuckets).as("every concrete edge type of the family, empty ones too").containsOnlyKeys("E0", "F", "G");
    for (final String type : new String[] { "E0", "F", "G" })
      assertThat(boxed(built.edgeTypeBuckets.get(type))).as("the buckets of " + type).containsExactlyInAnyOrderElementsOf(bucketsOf(type));
    for (final int bucket : built.edgeTypeBuckets.get("F"))
      assertThat(bucketsOf("E0")).as("a sub-type owns buckets of its own").doesNotContain(bucket);

    reopenDatabase();
    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, "GV");
    assertThat(restored.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(restored.isRestoredFromPersistedCsr()).as("restored, not rebuilt").isTrue();
    final GraphAnalyticalView.Snapshot snapshot = restored.pinned().snapshot();
    assertThat(snapshot.edgeTypeBuckets).as("the file carries them").containsOnlyKeys("E0", "F", "G");
    for (final String type : new String[] { "E0", "F", "G" })
      assertThat(boxed(snapshot.edgeTypeBuckets.get(type))).as("the buckets of " + type)
          .containsExactlyInAnyOrderElementsOf(bucketsOf(type));
  }

  /** A view restored from its persisted CSR identifies its slices the same way. */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void aViewRestoredFromDiskIsNotServedFromTheSliceOfADroppedType(final boolean queriedBeforeTheChange) {
    final GraphAnalyticalView built = viewOverTheFamily("SYNCHRONOUS", false);
    assertThat(built.isReady()).isTrue();

    reopenDatabase();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "GV");
    assertThat(view).isNotNull();
    if (queriedBeforeTheChange) {
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(view.isRestoredFromPersistedCsr()).as("restored, not rebuilt").isTrue();
    }

    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "DROP TYPE F UNSAFE").close();
      database.command("sql", "CREATE EDGE TYPE F EXTENDS E0").close();

      assertThat(count(E0_COUNT)).isEqualTo(3L);
      assertThat(sqlCount("SELECT count(@rid) AS c FROM E0")).isEqualTo(3L);
    } finally {
      release(view, gate);
    }

    assertServedWith(view, 3);
  }

  @Test
  void aRestoredViewWhoseTypeWasDetachedWrittenToAndAttachedAgainIsNotServedWithoutThatEdge() {
    final GraphAnalyticalView built = viewOverTheFamily("SYNCHRONOUS", false);
    assertThat(built.isReady()).isTrue();

    reopenDatabase();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "GV");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(view.isRestoredFromPersistedCsr()).as("restored, not rebuilt").isTrue();

    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "ALTER TYPE F SUPERTYPE -E0").close();
      write("CREATE EDGE F FROM (SELECT FROM V WHERE i = 0) TO (SELECT FROM V WHERE i = 3)");
      database.command("sql", "ALTER TYPE F SUPERTYPE +E0").close();

      assertThat(count(E0_COUNT)).isEqualTo(7L);
    } finally {
      release(view, gate);
    }

    assertServedWith(view, 7);
  }

  /**
   * V (4 vertices, {@code i = 0..3}), E0 (3 edges {@code i -> i+1}) and F EXTENDS E0 (3 edges {@code i+1 -> i}), under a view over V and
   * E0, which is READY. {@code withEmptySibling} adds G EXTENDS E0, created before the view and empty.
   */
  private GraphAnalyticalView viewOverTheFamily(final String mode, final boolean withEmptySibling) {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE E0").close();
    database.command("sql", "CREATE EDGE TYPE F EXTENDS E0").close();
    if (withEmptySibling)
      database.command("sql", "CREATE EDGE TYPE G EXTENDS E0").close();
    for (int i = 0; i < 4; i++)
      write("INSERT INTO V SET i = " + i);
    for (int i = 0; i < 3; i++) {
      write("CREATE EDGE E0 FROM (SELECT FROM V WHERE i = " + i + ") TO (SELECT FROM V WHERE i = " + (i + 1) + ")");
      write("CREATE EDGE F FROM (SELECT FROM V WHERE i = " + (i + 1) + ") TO (SELECT FROM V WHERE i = " + i + ")");
    }
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW GV VERTEX TYPES (V) EDGE TYPES (E0) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "GV");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(view.isReady()).isTrue();
    assertThat(view.getEdgeCount()).isEqualTo(6);
    return view;
  }

  /** The rebuild the change starts is held at its scan: what the view answers until then is what the old CSR says, or the record path. */
  private static CountDownLatch holdTheRebuild(final GraphAnalyticalView view) {
    final CountDownLatch gate = new CountDownLatch(1);
    view.setBeforeBuildScanForTest(() -> {
      try {
        gate.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    return gate;
  }

  private static void release(final GraphAnalyticalView view, final CountDownLatch gate) {
    gate.countDown();
    view.setBeforeBuildScanForTest(null);
  }

  private void rebuildIfOff(final GraphAnalyticalView view, final String mode) {
    if ("OFF".equals(mode)) {
      // THE CHECK HANDS THE CHANGE TO THE BUILD EXECUTOR, WHICH MARKS THE VIEW STALE WHEN IT RUNS
      Awaitility.await("the view turns STALE").atMost(30, TimeUnit.SECONDS)
          .until(() -> view.getStatus() == GraphAnalyticalView.Status.STALE);
      database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW GV").close();
    }
  }

  /** READY again, with the edges of the types as they are, and the Cypher hop answered by the view. */
  private void assertServedWith(final GraphAnalyticalView view, final long edges) {
    Awaitility.await("the rebuild").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
    assertThat(view.getEdgeCount()).isEqualTo((int) edges);
    assertThat(count(E0_COUNT)).isEqualTo(edges);
    assertThat(sqlCount("SELECT count(@rid) AS c FROM E0")).isEqualTo(edges);
    assertThat(cypherPlan(E0_COUNT)).contains(VIEW_HOP);
  }

  private static List<Integer> boxed(final int[] ids) {
    return Arrays.stream(ids).boxed().toList();
  }

  private List<Integer> bucketsOf(final String type) {
    return database.getSchema().getType(type).getBucketIds(false);
  }

  /** One statement in its own transaction, committed before the next: what the view hears one commit at a time. */
  private void write(final String statement) {
    database.transaction(() -> database.command("sql", statement).close());
  }

  private long walkedOut(final String edgeType) {
    long walked = 0;
    try (final ResultSet rs = database.query("sql", "SELECT out('" + edgeType + "') AS o FROM V")) {
      while (rs.hasNext()) {
        final Iterable<?> out = rs.next().getProperty("o");
        if (out != null)
          for (final Object ignored : out)
            ++walked;
      }
    }
    return walked;
  }

  /** A {@code count(*)}: a number, zero included, never null. */
  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      final Number n = rs.next().getProperty("c");
      assertThat(n).as("count(*) answers a number: " + cypher).isNotNull();
      return n.longValue();
    }
  }

  private long sqlCount(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private String cypherPlan(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
