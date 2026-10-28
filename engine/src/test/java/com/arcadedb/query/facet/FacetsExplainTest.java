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
import com.arcadedb.exception.CommandSQLParsingException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
  void profileShowsHierarchyExecution() {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.tagHierarchy = new JSONObject().put("vertexType", ColumnarTestData.TAG_TYPE).put("key", "path")
        .put("parentEdge", ColumnarTestData.PARENT_EDGE);
    data.createSchema(database);
    data.tagValues = ColumnarTestData.createTagTree(database, 42, 3, 3);
    data.insertAssets(database, 500);
    FacetOracle.awaitCaughtUp(database);

    final String text = plan("PROFILE", "SELECT FROM " + TYPE + " WHERE " + WHERE + " FACETS t: tags HIERARCHY TOP 5")
        .getProperty("executionPlanAsString");
    assertThat(text).contains("+ COLLECT FACET MATCHES", "HIERARCHY", "source=index", "budget=", "nodeVisits=");
    assertThat(text).containsPattern("counting=(DENSE|HASH)");
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

  /**
   * Final review B-N1: the server's query profiler keeps {@code toResult()} of every plan it records, so the collect step reports the
   * figures of its facets there (meta, sources, counts) but no value of the data: no term, no bound, no cursor.
   */
  @Test
  @Timeout(120)
  void thePlanResultKeepsTheFacetCountsButNoValue() {
    load();
    final Object facets = facetsOfStep(plan("PROFILE", QUERY + ", a: author TERMS TOP 3"));
    assertThat(facets).isNotNull();
    final String text = facets.toString();
    assertThat(text).contains("brand", "exact", "count", "otherCount");
    assertThat(text).doesNotContain("brand-", "author-", "value=", "from=", "to=");
  }

  /**
   * Final review B-I1: an INCLUDE value is evaluated when the facets are planned, so a nested statement there would run under EXPLAIN
   * (a read-only statement) and once more per execution. INCLUDE refuses any nested statement or subquery when it is parsed.
   */
  @Test
  @Timeout(120)
  void includeRefusesNestedStatementsAndWritesNothing() {
    load();
    database.command("sql", "CREATE DOCUMENT TYPE Audit");
    final String insert = "SELECT FROM " + TYPE + " FACETS brand TERMS INCLUDE [(INSERT INTO Audit SET x = 1)]";
    assertThatThrownBy(() -> plan("EXPLAIN", insert)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("INCLUDE");
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("sql", insert)) {
        rs.getFacets();
      }
    }).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("INCLUDE");
    final String subquery = "SELECT FROM " + TYPE + " FACETS brand TERMS INCLUDE [(SELECT brand FROM " + TYPE + " LIMIT 1)]";
    assertThatThrownBy(() -> plan("EXPLAIN", subquery)).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("INCLUDE");
    assertThat(database.countType("Audit", false)).isZero();

    // an expression without a nested statement is still a value
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " FACETS brand TERMS INCLUDE [format('brand-%d', 3)]")) {
      assertThat(rs.getFacets().get("brand").buckets()).extracting(FacetBucket::value).contains("brand-3");
    }
  }
}
