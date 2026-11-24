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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12549: the vertex-side sibling of #12032. A Graph Analytical View over a vertex type holds the vertices of its
 * sub-types in its node mapping, and nothing asked whether the types those buckets were scanned from still exist: the coverage checks
 * range over the CURRENT schema, where a type removed with {@code DROP TYPE ... UNSAFE} no longer appears. The view stayed READY and
 * every whole-graph procedure it serves ({@code algo.wcc} here) counted the vertices of the dropped type, until a restart. The node
 * buckets of the snapshot are now checked against the schema the way its edge slices are.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12549GavVertexSubTypeLeavesTest extends TestHelper {
  private static final String WCC_ROWS = "CALL algo.wcc() YIELD componentId RETURN count(*) AS c";

  /** DROP TYPE ... UNSAFE leaves the edges pointing at the dropped vertices, by design. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
  void aDroppedVertexSubTypeIsNotServedFromTheView(final String mode) {
    final GraphAnalyticalView view = viewOverTheFamily(mode);
    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "DROP TYPE W UNSAFE").close();

      assertThat(sqlCount("SELECT count(@rid) AS c FROM V0")).as("the reference: the record scan").isEqualTo(3L);
      assertThat(view.isReady()).as("the CSR holds the vertices of the dropped W").isFalse();
      assertThat(count(WCC_ROWS)).as("one row per vertex that is left").isEqualTo(3L);
    } finally {
      release(view, gate);
    }

    rebuildIfOff(view, mode);
    assertServedWith(view, 3);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
  void aVertexSubTypeDroppedAndCreatedAgainUnderTheSameNameIsNotServedFromTheOldBucket(final String mode) {
    final GraphAnalyticalView view = viewOverTheFamily(mode);
    final CountDownLatch gate = holdTheRebuild(view);
    try {
      // BACK TO BACK: NO READ OF THE VIEW BETWEEN THE TWO
      database.command("sql", "DROP TYPE W UNSAFE").close();
      database.command("sql", "CREATE VERTEX TYPE W EXTENDS V0").close();

      assertThat(view.isReady()).as("the CSR holds the vertices of the W that was dropped").isFalse();
      assertThat(count(WCC_ROWS)).isEqualTo(3L);
    } finally {
      release(view, gate);
    }

    rebuildIfOff(view, mode);
    assertServedWith(view, 3);
  }

  /** A sub-type renamed keeps its vertices, but not under the name the view holds its bucket by. */
  @Test
  void aRenamedVertexSubTypeIsServedUnderItsNewName() {
    final GraphAnalyticalView view = viewOverTheFamily("SYNCHRONOUS");
    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "ALTER TYPE W NAME WR").close();

      assertThat(view.isReady()).as("the CSR holds the vertices of WR under the name W").isFalse();
      assertThat(count(WCC_ROWS)).isEqualTo(5L);
    } finally {
      release(view, gate);
    }

    assertServedWith(view, 5);
    assertThat(view.pinned().snapshot().nodeMapping.getTypeName(view.pinned().snapshot().nodeMapping.getGlobalId(firstOf("WR"))))
        .isEqualTo("WR");
  }

  /** A sub-type detached from the listed type: its vertices leave the view, whose type no longer covers them. */
  @Test
  void aDetachedVertexSubTypeIsNotServedFromTheView() {
    final GraphAnalyticalView view = viewOverTheFamily("SYNCHRONOUS");
    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "ALTER TYPE W SUPERTYPE -V0").close();

      assertThat(view.isReady()).as("the CSR holds the vertices of W, which V0 no longer covers").isFalse();
    } finally {
      release(view, gate);
    }

    Awaitility.await("the rebuild").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
    assertThat(view.getNodeCount()).as("the view holds V0 alone").isEqualTo(3);
    assertThat(sqlCount("SELECT count(@rid) AS c FROM V0")).isEqualTo(3L);
    // W HOLDS VERTICES THE VIEW DOES NOT: THE WHOLE-GRAPH PROCEDURE TAKES THE RECORD PATH AND COUNTS THEM
    assertThat(view.coversVertexType(null)).isFalse();
    assertThat(count(WCC_ROWS)).isEqualTo(5L);
  }

  /** The vertices of a sub-type created after the build live only in the overlay, and leave with it as well. */
  @Test
  void anOverlayOnlyVertexSubTypeThatIsDroppedIsNotServed() {
    final GraphAnalyticalView view = viewOverTheFamily("SYNCHRONOUS");
    database.command("sql", "CREATE VERTEX TYPE X EXTENDS V0").close();
    write("INSERT INTO X SET name = 'x0'");
    write("INSERT INTO X SET name = 'x1'");
    assertThat(view.isReady()).isTrue();
    assertThat(view.getNodeCount()).as("the overlay holds the vertices of X").isEqualTo(7);
    assertThat(count(WCC_ROWS)).isEqualTo(7L);

    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "DROP TYPE X UNSAFE").close();

      assertThat(view.isReady()).as("the overlay holds the vertices of the dropped X").isFalse();
      assertThat(count(WCC_ROWS)).isEqualTo(5L);
    } finally {
      release(view, gate);
    }

    assertServedWith(view, 5);
  }

  /** A view built over every vertex type holds the vertices of a dropped type just the same. */
  @Test
  void aViewOverEveryVertexTypeIsNotServedFromTheBucketOfADroppedType() {
    database.command("sql", "CREATE VERTEX TYPE V0").close();
    database.command("sql", "CREATE VERTEX TYPE W").close();
    database.command("sql", "CREATE EDGE TYPE E0").close();
    for (int i = 0; i < 3; i++)
      write("INSERT INTO V0 SET name = 'v" + i + "'");
    for (int i = 0; i < 2; i++)
      write("INSERT INTO W SET name = 'w" + i + "'");
    write("CREATE EDGE E0 FROM (SELECT FROM V0 WHERE name = 'v0') TO (SELECT FROM W WHERE name = 'w0')");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW GV EDGE TYPES (E0) UPDATE MODE SYNCHRONOUS").close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "GV");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getNodeCount()).isEqualTo(5);
    assertThat(count(WCC_ROWS)).isEqualTo(5L);

    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "DROP TYPE W UNSAFE").close();

      assertThat(view.isReady()).as("the CSR holds the vertices of the dropped W").isFalse();
      assertThat(count(WCC_ROWS)).isEqualTo(3L);
    } finally {
      release(view, gate);
    }

    assertServedWith(view, 3);
  }

  /** A view restored from its persisted CSR checks its node buckets the same way. */
  @Test
  void aViewRestoredFromDiskIsNotServedFromTheBucketOfADroppedVertexType() {
    final GraphAnalyticalView built = viewOverTheFamily("SYNCHRONOUS");
    assertThat(built.isReady()).isTrue();

    reopenDatabase();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "GV");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(view.isRestoredFromPersistedCsr()).as("restored, not rebuilt").isTrue();

    final CountDownLatch gate = holdTheRebuild(view);
    try {
      database.command("sql", "DROP TYPE W UNSAFE").close();

      assertThat(view.isReady()).isFalse();
      assertThat(count(WCC_ROWS)).isEqualTo(3L);
    } finally {
      release(view, gate);
    }

    assertServedWith(view, 3);
  }

  /** What the rule must not cost: a schema change that leaves every node bucket with its type does not rebuild the view. */
  @Test
  void aSchemaChangeThatKeepsEveryNodeBucketDoesNotRebuildTheView() {
    final GraphAnalyticalView view = viewOverTheFamily("SYNCHRONOUS");
    final Object base = view.pinned().snapshot().nodeMapping;

    database.command("sql", "CREATE VERTEX TYPE Unrelated").close();
    database.command("sql", "CREATE BUCKET W_extra").close();
    database.command("sql", "ALTER TYPE W BUCKET +W_extra").close();
    database.command("sql", "CREATE PROPERTY V0.name STRING").close();

    assertThat(view.isReady()).as("every node bucket is still of the type it was scanned from").isTrue();
    assertThat(count(WCC_ROWS)).isEqualTo(5L);
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.pinned().snapshot().nodeMapping).as("no rebuild").isSameAs(base);
  }

  /**
   * V0 (3 vertices) and W EXTENDS V0 (2 vertices), E0 edges {@code v0 -> v1} and {@code v1 -> w0}, under a view over V0 and E0 that is
   * READY and holds the 5 vertices.
   */
  private GraphAnalyticalView viewOverTheFamily(final String mode) {
    database.command("sql", "CREATE VERTEX TYPE V0").close();
    database.command("sql", "CREATE VERTEX TYPE W EXTENDS V0").close();
    database.command("sql", "CREATE EDGE TYPE E0").close();
    for (int i = 0; i < 3; i++)
      write("INSERT INTO V0 SET name = 'v" + i + "'");
    for (int i = 0; i < 2; i++)
      write("INSERT INTO W SET name = 'w" + i + "'");
    write("CREATE EDGE E0 FROM (SELECT FROM V0 WHERE name = 'v0') TO (SELECT FROM V0 WHERE name = 'v1')");
    write("CREATE EDGE E0 FROM (SELECT FROM V0 WHERE name = 'v1') TO (SELECT FROM W WHERE name = 'w0')");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW GV VERTEX TYPES (V0) EDGE TYPES (E0) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "GV");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(view.isReady()).isTrue();
    assertThat(view.getNodeCount()).isEqualTo(5);
    assertThat(count(WCC_ROWS)).isEqualTo(5L);
    return view;
  }

  /** The rebuild the change starts is held at its scan: what the view answers until then is the record path. */
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

  /** READY again, with the vertices of the types as they are, and offered to the whole-graph procedures. */
  private void assertServedWith(final GraphAnalyticalView view, final int vertices) {
    Awaitility.await("the rebuild").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
    assertThat(view.getNodeCount()).isEqualTo(vertices);
    assertThat(view.coversVertexType(null)).as("the view covers every vertex").isTrue();
    assertThat(count(WCC_ROWS)).isEqualTo(vertices);
    assertThat(sqlCount("SELECT count(@rid) AS c FROM V0")).isEqualTo(vertices);
  }

  private RID firstOf(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " LIMIT 1")) {
      return rs.next().getIdentity().get();
    }
  }

  /** One statement in its own transaction, committed before the next: what the view hears one commit at a time. */
  private void write(final String statement) {
    database.transaction(() -> database.command("sql", statement).close());
  }

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
}
