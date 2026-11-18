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
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12039: with a Graph Analytical View over {@code E0}, renaming an edge sub-type {@code F} of {@code E0}
 * ({@code ALTER TYPE F NAME FR}) left the view's slice of F's edges under the old name, so SQL {@code out('FR')} and Cypher
 * {@code ()-[:FR]->()} served by the view found none of them until a restart. The view definition names only {@code E0}, so the
 * rename never reached it; the view now finds the slice of a type that is gone and rebuilds before it serves again.
 */
class Issue12039GavRenamedEdgeSubTypeTest extends TestHelper {

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void aRenamedEdgeSubTypeIsWalkedByItsNewName(final boolean joinedAfterTheBuild) {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE E0").close();
    database.command("sql", "CREATE EDGE TYPE F" + (joinedAfterTheBuild ? "" : " EXTENDS E0")).close();
    for (int i = 0; i < 4; i++)
      write("INSERT INTO V SET i = " + i);
    for (int i = 0; i < 3; i++) {
      write("CREATE EDGE E0 FROM (SELECT FROM V WHERE i = " + i + ") TO (SELECT FROM V WHERE i = " + (i + 1) + ")");
      write("CREATE EDGE F FROM (SELECT FROM V WHERE i = " + (i + 1) + ") TO (SELECT FROM V WHERE i = " + i + ")");
    }
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW G VERTEX TYPES (V) EDGE TYPES (E0) UPDATE MODE SYNCHRONOUS").close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "G");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    if (joinedAfterTheBuild) {
      // THE READ FINDS F JOINED WITH EDGES THE CSR DOES NOT HOLD, AND THE REBUILD SCANS F INTO ITS OWN SLICE
      database.command("sql", "ALTER TYPE F SUPERTYPE +E0").close();
      database.query("opencypher", "MATCH ()-[e:E0]->() RETURN count(*) AS c").close();
      awaitServed(view);
    }
    assertThat(walkedOut("F")).isEqualTo(3L);

    // THE REBUILD THE RENAME STARTS IS HELD AT ITS SCAN: THE VIEW STILL HOLDS THE SLICE UNDER THE OLD NAME, AND IS ASKED WITH IT
    final CountDownLatch gate = new CountDownLatch(1);
    view.setBeforeBuildScanForTest(() -> {
      try {
        gate.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    try {
      database.command("sql", "ALTER TYPE F NAME FR").close();

      assertThat(view.isReady()).as("the CSR holds F's edges under a name that is no longer a type").isFalse();
      assertThat(walkedOut("FR")).isEqualTo(3L);
      assertThat(count("MATCH ()-[e:FR]->() RETURN count(*) AS c")).isEqualTo(3L);
      assertThat(count("MATCH ()-[e:FR]->() RETURN sum(1) AS c")).isEqualTo(3L);
    } finally {
      gate.countDown();
      view.setBeforeBuildScanForTest(null);
    }

    awaitServed(view);
    assertThat(walkedOut("FR")).isEqualTo(3L);
    try (final ResultSet rs = database.query("sql", "SELECT out('FR') AS o FROM V")) {
      while (rs.hasNext())
        rs.next();
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("CSR-accelerated via Graph Analytical View");
    }
    assertThat(count("MATCH (a:V)-[e:FR]->(b:V) RETURN count(*) AS c")).isEqualTo(3L);
    assertThat(count("MATCH (a:V)-[e:E0]->(b:V) RETURN count(*) AS c")).isEqualTo(6L);
  }

  private static void awaitServed(final GraphAnalyticalView view) {
    Awaitility.await("the rebuild").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
  }

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

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      final Number n = rs.next().getProperty("c");
      return n == null ? 0L : n.longValue();
    }
  }
}
