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
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10411, the measurement of the report: a 2-hop count over 50,000 vertices and 400,000 edges, timed on the freshly built
 * view and again after one commit of a single edge left an overlay on it. Reports the two medians; the overlay one used to be
 * about 27% slower.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue10411OverlayReadCostBenchmark extends TestHelper {
  private static final String QUERY = "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n";

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void twoHopCountWithAnOverlay(final String mode) {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    final Random random = new Random(7);
    final int vertices = 50_000;
    final List<RID> rids = new ArrayList<>(vertices);
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++)
        rids.add(database.newVertex("P").set("pid", i).save().getIdentity());
      for (int i = 0; i < vertices; i++)
        for (int j = 0; j < 8; j++)
          rids.get(i).asVertex().modify().newEdge("K", rids.get((i + 1 + random.nextInt(vertices - 1)) % vertices));
    });
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.awaitReady(120, TimeUnit.SECONDS)).isTrue();

    final long freshCount = count();
    final double fresh = medianMillis();

    database.transaction(
        () -> rids.get(random.nextInt(vertices)).asVertex().modify().newEdge("K", rids.get(random.nextInt(vertices))));
    assertThat(view.awaitReady(120, TimeUnit.SECONDS)).isTrue();
    assertThat(view.hasActiveOverlay()).isTrue();

    final long overlayCount = count();
    final double overlay = medianMillis();

    LogManager.instance().log(this, Level.INFO, "#10411 %s: fresh view %.1f ms, with an overlay %.1f ms (ratio %.3f)", mode, fresh,
        overlay, overlay / fresh);
    assertThat(overlayCount).isGreaterThanOrEqualTo(freshCount);
  }

  private double medianMillis() {
    for (int i = 0; i < 30; i++)
      count();
    final long[] times = new long[20];
    for (int i = 0; i < times.length; i++) {
      final long start = System.nanoTime();
      count();
      times[i] = System.nanoTime() - start;
    }
    Arrays.sort(times);
    return (times[9] + times[10]) / 2e6;
  }

  private long count() {
    try (final ResultSet rs = database.query("opencypher", QUERY)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }
}
