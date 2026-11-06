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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final review A-I2 / C-I1: the bitmaps a query derives from its match set per facet (a FILTER facet's own matches, the matches minus
 * the patched positions) scale with matches x facets, so each one is charged to {@link FacetMemoryBudget} before it is allocated. A
 * budget that holds the match set and the counters but not those bitmaps makes the FILTER facets unavailable, never an unbounded heap.
 */
class FacetBitmapChargesTest extends TestHelper {
  private static final String TYPE    = "Product";
  private static final int    FILTERS = 10;
  private static final int    RECORDS = 6_000;

  /** Records of about 1.5 KB, so a bitmap of the matches spans many pages (one charged stripe each). */
  private void load() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".brand STRING");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    final String body = "x".repeat(1_500);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("brand", "brand-" + (i % FILTERS)).set("body", body).save();
    });
    FacetOracle.awaitCaughtUp(database);
  }

  /** Half the records pass every filter, the others fail exactly one, spread over every page. */
  private MatchSet matches() {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, FILTERS, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE)) {
      int i = 0;
      while (rs.hasNext()) {
        final int failMask = i % 2 == 0 ? 0 : 1 << ((i / 2) % FILTERS);
        assertThat(builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), failMask)).isTrue();
        i++;
      }
      final MatchSet matches = builder.build();
      final int[] filterIndexByFacet = new int[FILTERS + 1];
      for (int f = 0; f < FILTERS; f++)
        filterIndexByFacet[f] = f;
      filterIndexByFacet[FILTERS] = -1;
      matches.setFacetFilterIndex(filterIndexByFacet);
      return matches;
    }
  }

  /** One terms facet per filter, then one without: the spec FILTER itself is only what the match set's masks stand for. */
  private static List<FacetSpec> facets() {
    final List<FacetSpec> facets = new ArrayList<>();
    for (int f = 0; f <= FILTERS; f++)
      facets.add(new TermsFacetSpec("f" + f, "brand", null, null, 100, 1, FacetOrder.COUNT_DESC, null, false, null));
    return facets;
  }

  @Test
  @Timeout(120)
  void filterFacetBitmapsBeyondTheBudgetMakeTheFacetsUnavailable() {
    load();
    final long saved = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    final long savedWait = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
    try (final MatchSet matches = matches()) {
      // with the whole budget, what the counters alone reserve
      final FacetResults counted = FacetExecutor.execute((DatabaseInternal) database, matches, facets(), new BasicCommandContext());
      long counters = 0;
      for (int f = 0; f <= FILTERS; f++) {
        assertThat(counted.get("f" + f).source()).isEqualTo("index");
        counters += counted.executionStats().get("f" + f).budgetBytes();
      }

      // room for the match set and the counters, not for one FILTER facet's bitmap (one stripe of every page of the bucket)
      final long held = saved - FacetMemoryBudget.instance().available();
      // counters are reserved in KiB steps, one or more per collector; the budget itself counts whole KiB
      final long headroom = ((counters + 1023) / 1024 + FILTERS + 1 + 4) * 1024;
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(held + headroom);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(0L);
      final FacetResults refused = FacetExecutor.execute((DatabaseInternal) database, matches, facets(), new BasicCommandContext());
      for (int f = 0; f < FILTERS; f++) {
        final FacetResult result = refused.get("f" + f);
        assertThat(result.source()).as("FILTER facet " + f).isEqualTo(FacetResult.UNAVAILABLE);
        assertThat(result.reason()).isEqualTo(TermsCollector.MEMORY_BUDGET);
      }
      // the facet without FILTER counts the match set itself: nothing to derive, nothing refused
      assertThat(refused.get("f" + FILTERS).source()).isEqualTo("index");
      assertThat(refused.get("f" + FILTERS).buckets()).isNotEmpty();
      assertThat(FacetMemoryBudget.instance().available()).as("everything the query reserved is released").isEqualTo(headroom);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(savedWait);
    }
    assertThat(FacetMemoryBudget.instance().available()).isEqualTo(saved);
  }
}
