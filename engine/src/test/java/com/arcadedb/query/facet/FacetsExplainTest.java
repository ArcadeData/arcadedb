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

import com.arcadedb.TestHelper;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FacetsExplainTest extends TestHelper {
  private static final String TYPE  = ColumnarTestData.TYPE;
  private static final String WHERE = "fileSize < 1048576";
  private static final String QUERY = "SELECT FROM " + TYPE + " WHERE " + WHERE + " FACETS brand TERMS TOP 10, s: fileSize RANGES ([0, 1024))";

  private void load() {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);
  }

  private Result plan(final String prefix, final String sql) {
    try (final ResultSet rs = database.query("sql", prefix + " " + sql)) {
      return rs.next();
    }
  }

  /** The {@code facets} property of the collect step in the plan's {@code toResult()}, null when there is none. */
  @SuppressWarnings("unchecked")
  private static Object facetsOfStep(final Result plan) {
    final Result executionPlan = plan.getProperty("executionPlan");
    for (final Result step : (List<Result>) executionPlan.getProperty("steps"))
      if (step.hasProperty("facets"))
        return step.getProperty("facets");
    return null;
  }

  private long count(final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + TYPE + " WHERE " + where)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  @Timeout(120)
  void explainShowsFacetSteps() {
    load();
    final String text = plan("EXPLAIN", QUERY).getProperty("executionPlanAsString");
    assertThat(text).contains("+ COLLECT FACET MATCHES");
    assertThat(text).contains("brand TERMS TOP 10");
    assertThat(text).contains("s: fileSize RANGES ([0, 1024))");
    // only planned: nothing is computed
    assertThat(text).doesNotContain("segments=");
  }

  @Test
  @Timeout(120)
  void profileShowsExecution() {
    load();
    final Result plan = plan("PROFILE", QUERY);
    final String text = plan.getProperty("executionPlanAsString");
    assertThat(text).contains("+ COLLECT FACET MATCHES", "segments=", "skipped=", "patched=", "budget=", "source=index", "matchSource=rows");
    assertThat(text).containsPattern("counting=(DENSE|HASH)");
    assertThat(facetsOfStep(plan)).isNotNull();
  }

  @Test
  @Timeout(120)
  void profileWithLimitStillComputesFacets() {
    load();
    final Result plan = plan("PROFILE", QUERY + " LIMIT 2");
    assertThat((String) plan.getProperty("executionPlanAsString")).contains("source=index");
    final Object facets = facetsOfStep(plan);
    assertThat(facets).isNotNull();
    final JSONObject json = facets instanceof Map<?, ?> map ? new JSONObject((Map<String, Object>) map) : new JSONObject(facets.toString());
    final FacetResult brand = FacetResults.fromJSON(json).get("brand");
    long sum = brand.otherCount() + brand.missing();
    for (final FacetBucket bucket : brand.buckets())
      sum += bucket.count();
    assertThat(sum).isEqualTo(count(WHERE));
  }
}
