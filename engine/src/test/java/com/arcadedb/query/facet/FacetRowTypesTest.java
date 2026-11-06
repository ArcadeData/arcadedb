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
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final fix batch 2 round 2 (ruling on review I1): a terms facet's values are the values a row of its property holds, so the embedded
 * facets equal the embedded GROUP BY by {@code equals}: an Integer for an INTEGER property, a Short for a SHORT one, a Float, a
 * Double, a BigDecimal, dates as the database reads them back; whether the value is counted from the projection or scanned.
 */
class FacetRowTypesTest extends TestHelper {
  private static final String       TYPE       = "Typed";
  private static final List<String> PROPERTIES = List.of("i", "s", "f", "d", "dec", "day", "at");

  private void load() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".i INTEGER");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".s SHORT");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".f FLOAT");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".d DOUBLE");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".dec DECIMAL");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".day DATE");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".at DATETIME");
    // DECIMAL has no COLUMNAR column: it is scanned
    for (final String property : List.of("i", "s", "f", "d", "day", "at"))
      database.command("sql", "CREATE INDEX ON " + TYPE + " (" + property + ") COLUMNAR");
    database.transaction(() -> {
      for (int n = 0; n < 60; n++)
        database.newDocument(TYPE).set("i", n % 4).set("s", (short) (n % 3)).set("f", 0.05f * (n % 5)).set("d", 1.25 * (n % 6))
            // decimals without trailing zeros: a facet groups 0.0 and 0 as one term and returns it stripped (0), GROUP BY returns the
            // first row's scale; both are one group, equal by compareTo
            .set("dec", n % 2 == 0 ? BigDecimal.ZERO : new BigDecimal("12.5")).set("day", LocalDate.of(2024, 2, 1 + n % 7))
            .set("at", LocalDateTime.of(2024, 2, 1, 10, n % 8, 15)).save();
    });
    FacetOracle.awaitCaughtUp(database);
  }

  private Map<Object, Long> groupBy(final String property) {
    final Map<Object, Long> counts = new HashMap<>();
    try (final FacetOracle.NormalPlan normal = FacetOracle.normalPlan(database);
        final ResultSet rs = database.query("sql", "SELECT " + property + " AS v, count(*) AS c FROM " + TYPE + " GROUP BY " + property)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        counts.put(row.getProperty("v"), row.<Number>getProperty("c").longValue());
      }
    }
    return counts;
  }

  private static Map<Object, Long> counts(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        counts.put(bucket.value(), bucket.count());
    return counts;
  }

  @Test
  @Timeout(120)
  void facetValuesAreTheRowValuesOfTheirProperty() {
    load();
    final StringBuilder sql = new StringBuilder("SELECT FROM " + TYPE + " FACETS ");
    for (int i = 0; i < PROPERTIES.size(); i++)
      sql.append(i > 0 ? ", " : "").append(PROPERTIES.get(i)).append(" TERMS TOP 100");
    final FacetResults facets;
    try (final ResultSet rs = database.query("sql", sql + " LIMIT 0")) {
      facets = rs.getFacets();
    }
    for (final String property : PROPERTIES) {
      final Map<Object, Long> expected = groupBy(property);
      assertThat(counts(facets.get(property))).as(property).isEqualTo(expected);
      final Object rowValue = expected.keySet().iterator().next();
      for (final Object facetValue : counts(facets.get(property)).keySet())
        assertThat(facetValue.getClass()).as(property + " " + facetValue).isEqualTo(rowValue.getClass());
    }
    assertThat(facets.get("dec").source()).isEqualTo(FacetExecutor.SCAN);
  }
}
