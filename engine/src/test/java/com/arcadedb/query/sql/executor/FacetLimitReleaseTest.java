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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.facet.FacetMemoryBudget;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A LIMIT reached in a FACETS plan computes the facets before it releases the upstream; when that computation fails, the upstream is
 * released all the same (the failure reaches the caller, nothing stays open).
 */
class FacetLimitReleaseTest extends TestHelper {
  private static final String TYPE = ColumnarTestData.TYPE;

  @Test
  @Timeout(300)
  void facetFailureAtTheLimitStillReleasesTheUpstream() {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".title STRING");
    final List<RID> all = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE)) {
      while (rs.hasNext())
        all.add(rs.next().getProperty("rid"));
    }
    database.transaction(() -> {
      for (int i = 0; i < all.size(); i++)
        database.lookupByRID(all.get(i), true).asDocument().modify().set("title", i % 10 == 0 ? "red shoe" : "blue boot").save();
    });
    database.command("sql", "CREATE INDEX ON " + TYPE + " (title) FULL_TEXT");
    FacetOracle.awaitCaughtUp(database);

    final long capacity = FacetMemoryBudget.instance().available();
    // 1-byte buckets over sizes up to 4 MiB: far more than the 100,000 buckets a histogram may have
    final ResultSet rs = database.query("sql",
        "SELECT FROM " + TYPE + " WHERE search_fields(['title'], 'shoe') FACETS h: fileSize HISTOGRAM FIXED 1 LIMIT 3");
    final FetchFromIndexedFunctionStep fetch = fetchStep(rs);
    assertThatThrownBy(() -> {
      while (rs.hasNext())
        rs.next();
    }).isInstanceOf(CommandExecutionException.class);

    // not closed by the caller: the LIMIT released the fetch (its answer) and the match set all the same
    assertThat(fetch.fullResult).isNull();
    assertThat(FacetMemoryBudget.instance().available()).isEqualTo(capacity);
    rs.close();
  }

  private static FetchFromIndexedFunctionStep fetchStep(final ResultSet rs) {
    for (final ExecutionStep step : rs.getExecutionPlan().orElseThrow().getSteps())
      if (step instanceof FetchFromIndexedFunctionStep fetch)
        return fetch;
    throw new AssertionError("The plan has no indexed function fetch: " + rs.getExecutionPlan().get().prettyPrint(0, 2));
  }
}
