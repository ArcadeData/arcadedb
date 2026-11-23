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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13230 on data large enough for the parallel top-K ({@code PartialTopK}, issue #11873) and for a sequential pull of
 * several batches: {@code LIMIT 2147483647}, {@code 2147483646} and an ordinary LIMIT answer what a sort of the whole type does,
 * on the parallel path and on the sequential one,.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherOrderByLimitIntMaxLargeIssue13230Test extends TestHelper {
  private static final int ROWS = 20_000;

  @Override
  protected void beginTest() {
    // Small units, so the fixture's few hundred pages are cut in many of them and the scan runs in parallel
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    final VertexType type = database.getSchema().createVertexType("T", 4);
    type.createProperty("k", Type.INTEGER);
    type.createProperty("g", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("T").set("k", i, "g", (i * 7) % 1000).save();
    });
  }

  /** The reference: every k, sorted by g DESC then k, as the ORDER BY below. */
  private static List<Long> reference(final int limit) {
    return IntStream.range(0, ROWS).boxed()
        .sorted(Comparator.<Integer>comparingInt(i -> -((i * 7) % 1000)).thenComparingInt(i -> i))
        .limit(limit).map(Integer::longValue).toList();
  }

  private List<Long> run(final String query, final Object limit) {
    final List<Long> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, Map.of("l", limit))) {
      rs.forEachRemaining(r -> out.add(r.<Number>getProperty("k").longValue()));
    }
    return out;
  }

  private String profile(final String query, final Object limit) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, Map.of("l", limit))) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static final String QUERY = "MATCH (n:T) RETURN n.k AS k ORDER BY n.g DESC, n.k LIMIT $l";

  @Test
  void largeLimitsAnswerTheSortedReferenceOnTheParallelPath() {
    assertThat(profile(QUERY, 10)).as("the fixture reaches the parallel top-K").contains("parallel: ").contains(" workers)");
    for (final int limit : new int[] { 10, 5_000, ROWS, ROWS + 1, Integer.MAX_VALUE - 1, Integer.MAX_VALUE })
      assertThat(run(QUERY, limit)).as("LIMIT " + limit).isEqualTo(reference(limit));
  }

  @Test
  void largeLimitsAnswerTheSortedReferenceOnTheSequentialPath() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      assertThat(profile(QUERY, 10)).as("sequential").doesNotContain("parallel: ");
      for (final int limit : new int[] { 10, 5_000, ROWS, ROWS + 1, Integer.MAX_VALUE - 1, Integer.MAX_VALUE })
        assertThat(run(QUERY, limit)).as("LIMIT " + limit).isEqualTo(reference(limit));
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }
}
