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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8802: an ORDER BY bounded by LIMIT keeps its best rows in the workers of a parallel scan and merges them,
 * instead of projecting and sorting every row on the consumer thread. The answer must be the sequential one, ties included: among
 * equal keys the row the sequential scan meets first wins.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8802ParallelTopKTest extends TestHelper {
  private static final int ROWS = 20_000;

  @Override
  protected void beginTest() {
    // SMALL UNITS, SO THE FIXTURE'S FEW HUNDRED PAGES ARE CUT IN MANY OF THEM
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    for (final String[] t : new String[][] { { "OneBucket", "1" }, { "FourBuckets", "4" } }) {
      database.getSchema().createDocumentType(t[0], Integer.parseInt(t[1]));
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          // MANY TIES ON x (100 DISTINCT VALUES): THE TIE-BREAK IS WHAT THE SEQUENTIAL ORDER DECIDES
          database.newDocument(t[0]).set("id", i, "x", (i * 37) % 100, "grp", i % 7, "name", "n" + (i % 13)).save();
      });
    }
  }

  private List<String> run(final String query, final boolean parallel, final String[] planOut) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
    try (final ResultSet rs = database.query("sql", query)) {
      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getPropertyNames().stream().sorted().map(n -> n + "=" + r.getProperty(n)).reduce("", (a, b) -> a + b + ";"));
      }
      if (planOut != null)
        planOut[0] = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      return rows;
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private void assertSameAsSequential(final String query) {
    final String[] plan = new String[1];
    final List<String> parallel = run(query, true, plan);
    assertThat(plan[0]).as(query).contains("parallel: ");
    assertThat(parallel).as(query).isNotEmpty().isEqualTo(run(query, false, null));
  }

  @Test
  void topKRunsInTheWorkersAndKeepsTheSequentialAnswer() {
    for (final String type : new String[] { "FourBuckets", "OneBucket" }) {
      assertSameAsSequential("SELECT id, x FROM " + type + " ORDER BY x DESC LIMIT 10");
      assertSameAsSequential("SELECT id, x FROM " + type + " ORDER BY x ASC LIMIT 25");
      assertSameAsSequential("SELECT id, x FROM " + type + " ORDER BY x DESC SKIP 1000 LIMIT 10");
      assertSameAsSequential("SELECT id, x, grp FROM " + type + " ORDER BY grp ASC, x DESC LIMIT 40");
      assertSameAsSequential("SELECT id, x FROM " + type + " WHERE grp < 4 ORDER BY x DESC LIMIT 10");
      assertSameAsSequential("SELECT x AS y, id FROM " + type + " ORDER BY y DESC LIMIT 10");
      assertSameAsSequential("SELECT id FROM " + type + " ORDER BY x ASC LIMIT 10");
      assertSameAsSequential("SELECT id, x FROM " + type + " ORDER BY x DESC LIMIT " + (ROWS + 5));
    }
  }

  @Test
  void limitZeroAndEmptyFilterAnswerNothing() {
    assertThat(run("SELECT id FROM FourBuckets ORDER BY x LIMIT 0", true, null)).isEmpty();
    assertThat(run("SELECT id FROM FourBuckets WHERE x > 1000 ORDER BY x LIMIT 10", true, null)).isEmpty();
  }

  /** Workers do not see a transaction's changes, so an ORDER BY inside one stays sequential - and sees them. */
  @Test
  void insideATransactionTheSortStaysSequentialAndSeesItsChanges() {
    database.transaction(() -> {
      database.newDocument("OneBucket").set("id", -1, "x", 1_000, "grp", 0, "name", "tx").save();
      final String[] plan = new String[1];
      final List<String> rows = run("SELECT id, x FROM OneBucket ORDER BY x DESC LIMIT 1", true, plan);
      assertThat(plan[0]).doesNotContain("parallel: ");
      assertThat(rows.getFirst()).contains("x=1000");
      database.rollback();
    });
  }

  /** A function the engine does not ship promises nothing about concurrent use: it keeps the sort sequential. */
  @Test
  void aUserFunctionKeepsTheSortSequential() {
    database.command("sql", "DEFINE FUNCTION t8802.keep 'return q' PARAMETERS [q] LANGUAGE js");
    final String[] plan = new String[1];
    final List<String> rows = run("SELECT id, `t8802.keep`(x) AS k FROM FourBuckets ORDER BY k DESC LIMIT 5", true, plan);
    assertThat(plan[0]).doesNotContain("parallel: ");
    assertThat(rows).hasSize(5);
  }
}
