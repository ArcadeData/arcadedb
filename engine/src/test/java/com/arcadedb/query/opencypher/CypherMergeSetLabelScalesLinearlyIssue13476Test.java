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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #13476: {@code UNWIND $rows AS item MERGE (i:L {id: item.id}) SET i:Extra} took O(n^2) because
 * {@code MergeStep} copied the statement's whole label-replacement map before every row. The statement is timed at n and at 4n
 * rows: a linear statement takes about 4 times as long, the quadratic one about 16 times. The ratio is a claim about the shape of
 * the cost, so it holds on a slow machine as on a fast one, and each window is stall-discounted so a GC pause in a full-suite JVM
 * does not count against it.
 */
class CypherMergeSetLabelScalesLinearlyIssue13476Test extends TestHelper {
  private static final int SMALL = 3_000;
  private static final int LARGE = 4 * SMALL;

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES) // a hang detector, not a latency bound
  void mergeThenSetLabelScalesLinearlyWithTheRows() {
    // Warms the plan cache and the JIT, which would otherwise be charged to the small run and flatten the ratio
    mergeAndRelabel("Warmup", SMALL);

    final long smallMs = Math.max(1L, mergeAndRelabel("Small", SMALL));
    final long largeMs = mergeAndRelabel("Large", LARGE);
    final double ratio = (double) largeMs / smallMs;

    assertThat(ratio)
        .as("UNWIND ... MERGE ... SET n:Label over %,d rows took %,d ms and over %,d rows %,d ms (stall-discounted): a ratio of %.2f, "
            + "where a linear cost gives about 4 and the per-row copy of the label-replacement map about 16", SMALL, smallMs, LARGE,
            largeMs, ratio)
        .isLessThan(8.0);
  }

  /** Runs the statement of the issue over {@code rows} fresh rows, checks every node was relabelled once, and returns its time. */
  private long mergeAndRelabel(final String suffix, final int rows) {
    final String type = "Item13476" + suffix;
    final String label = "Container13476" + suffix;
    database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".id STRING");
    database.command("sql", "CREATE INDEX ON " + type + " (id) NOTUNIQUE");

    final List<Map<String, Object>> parameters = new ArrayList<>(rows);
    for (int i = 0; i < rows; i++)
      parameters.add(Map.of("id", "item-" + i, "v", (long) i));

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    database.transaction(() -> database.command("opencypher",
        "UNWIND $rows AS item MERGE (i:" + type + " {id: item.id}) SET i.v = item.v, i:" + label, Map.of("rows", parameters)).close());
    final long effectiveMs = stopwatch.effectiveMs();

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:" + label + ":" + type + ") RETURN count(n) AS c, sum(n.v) AS s")) {
      final Result row = rs.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo((long) rows);
      assertThat(row.<Long>getProperty("s")).isEqualTo((long) rows * (rows - 1) / 2);
    }
    return effectiveMs;
  }
}
