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
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetStats;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests issue #11572: MIN and MAX of a DOUBLE or FLOAT column answered from the COLUMNAR projection, and the STATS facet, keep the sign of
 * a negative zero (-0.0) instead of answering 0.0.
 */
class Issue11572ColumnarNegativeZeroMinMaxTest extends TestHelper {

  @Test
  void doubleColumnarMinMaxPreservesNegativeZero() {
    loadZeros("DoubleItem", "DOUBLE", -0.0d, 0.0d);
    assertMinMax("DoubleItem");
  }

  @Test
  void floatColumnarMinMaxPreservesNegativeZero() {
    loadZeros("FloatItem", "FLOAT", -0.0f, 0.0f);
    assertMinMax("FloatItem");
  }

  /**
   * Groups A {-0.0}, B {-0.0, 0.0}, C {0.0} and D {0.0, -0.0} (the zeros of B and D inserted in opposite orders).
   */
  private void loadZeros(final String type, final String propertyType, final Number negativeZero, final Number positiveZero) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".grp STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".val " + propertyType);
    database.command("sql", "CREATE INDEX ON " + type + " (grp) COLUMNAR");
    database.command("sql", "CREATE INDEX ON " + type + " (val) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);

    database.transaction(() -> {
      database.newDocument(type).set("grp", "A").set("val", negativeZero).save();
      database.newDocument(type).set("grp", "B").set("val", negativeZero).save();
      database.newDocument(type).set("grp", "B").set("val", positiveZero).save();
      database.newDocument(type).set("grp", "C").set("val", positiveZero).save();
      database.newDocument(type).set("grp", "D").set("val", positiveZero).save();
      database.newDocument(type).set("grp", "D").set("val", negativeZero).save();
    });
    FacetOracle.awaitCaughtUp(database);
  }

  /**
   * The projection orders the zeros as {@code Double.compare} (-0.0 below 0.0) whatever the row order. The normal plan compares with
   * {@code BinaryComparator}, where the zeros are equal, so on a group holding both it keeps the zero it met first: the two plans agree on
   * the value of every extreme and differ only on the sign of a zero extreme of a group holding both zeros.
   */
  private void assertMinMax(final String type) {
    final String sql = "SELECT grp, min(val) AS mn, max(val) AS mx FROM " + type + " GROUP BY grp";

    final Map<String, double[]> accelerated = minMax(sql);
    assertThat(columnarPath(sql)).as("the accelerated run of " + sql + " is answered from the projection").isEqualTo("columnar");
    assertThat(accelerated).containsOnlyKeys("A", "B", "C", "D");
    assertSigned(accelerated, "A", -0.0d, -0.0d);
    assertSigned(accelerated, "B", -0.0d, 0.0d);
    assertSigned(accelerated, "C", 0.0d, 0.0d);
    assertSigned(accelerated, "D", -0.0d, 0.0d);

    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, false);
    try {
      final Map<String, double[]> normal = minMax(sql);
      assertThat(columnarPath(sql)).as("the normal run of " + sql + " is not answered from the projection").startsWith("normal");
      assertThat(normal).containsOnlyKeys("A", "B", "C", "D");
      // one sign of zero in the group: both plans answer it with its sign
      assertSigned(normal, "A", -0.0d, -0.0d);
      assertSigned(normal, "C", 0.0d, 0.0d);
      // both zeros in the group: the normal plan keeps the zero it meets first, so only the value is fixed
      for (final String group : new String[] { "B", "D" })
        for (final double extreme : normal.get(group))
          assertThat(Math.abs(extreme)).as("normal plan, group " + group).isEqualTo(0.0d);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, true);
    }
  }

  private static void assertSigned(final Map<String, double[]> rows, final String group, final double min, final double max) {
    assertThat(Double.doubleToRawLongBits(rows.get(group)[0])).as("min of group " + group).isEqualTo(Double.doubleToRawLongBits(min));
    assertThat(Double.doubleToRawLongBits(rows.get(group)[1])).as("max of group " + group).isEqualTo(Double.doubleToRawLongBits(max));
  }

  /** min and max of each group, widened to double as a Float keeps its sign. */
  private Map<String, double[]> minMax(final String sql) {
    final Map<String, double[]> rows = new HashMap<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.put(row.getProperty("grp"),
            new double[] { ((Number) row.getProperty("mn")).doubleValue(), ((Number) row.getProperty("mx")).doubleValue() });
      }
    }
    return rows;
  }

  /** The path the {@link ColumnarGroupByStep} of a PROFILE of the statement took, null when its plan has no such step. */
  private String columnarPath(final String sql) {
    try (final ResultSet rs = database.query("sql", "PROFILE " + sql)) {
      while (rs.hasNext())
        rs.next();
      for (final ExecutionStep step : rs.getExecutionPlan().get().getSteps())
        if (step instanceof ColumnarGroupByStep columnar)
          return columnar.lastPath();
    }
    return null;
  }

  @Test
  void doubleStatsFacetPreservesNegativeZero() {
    database.command("sql", "CREATE DOCUMENT TYPE StatItem");
    database.command("sql", "CREATE PROPERTY StatItem.val DOUBLE");
    database.command("sql", "CREATE INDEX ON StatItem (val) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);

    database.transaction(() -> {
      database.newDocument("StatItem").set("val", -0.0d).save();
      database.newDocument("StatItem").set("val", 0.0d).save();
    });
    FacetOracle.awaitCaughtUp(database);

    try (final ResultSet rs = database.query("sql", "SELECT FROM StatItem FACETS s: val STATS")) {
      final FacetStats stats = rs.getFacets().get("s").stats();
      final double min = ((Number) stats.min()).doubleValue();
      final double max = ((Number) stats.max()).doubleValue();

      assertThat(Double.doubleToRawLongBits(min)).isEqualTo(Double.doubleToRawLongBits(-0.0d));
      assertThat(Double.doubleToRawLongBits(max)).isEqualTo(Double.doubleToRawLongBits(0.0d));
    }

    // Also verify stats over only -0.0d has max = -0.0d
    database.command("sql", "CREATE DOCUMENT TYPE StatItemNegOnly");
    database.command("sql", "CREATE PROPERTY StatItemNegOnly.val DOUBLE");
    database.command("sql", "CREATE INDEX ON StatItemNegOnly (val) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);

    database.transaction(() -> database.newDocument("StatItemNegOnly").set("val", -0.0d).save());
    FacetOracle.awaitCaughtUp(database);

    try (final ResultSet rs = database.query("sql", "SELECT FROM StatItemNegOnly FACETS s: val STATS")) {
      final FacetStats stats = rs.getFacets().get("s").stats();
      final double min = ((Number) stats.min()).doubleValue();
      final double max = ((Number) stats.max()).doubleValue();

      assertThat(Double.doubleToRawLongBits(min)).isEqualTo(Double.doubleToRawLongBits(-0.0d));
      assertThat(Double.doubleToRawLongBits(max)).isEqualTo(Double.doubleToRawLongBits(-0.0d));
    }
  }

  @Test
  void negativeBoundsAndZeroPreservationInColumnar() {
    database.command("sql", "CREATE DOCUMENT TYPE BoundItem");
    database.command("sql", "CREATE PROPERTY BoundItem.val DOUBLE");
    database.command("sql", "CREATE INDEX ON BoundItem (val) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);

    database.transaction(() -> {
      database.newDocument("BoundItem").set("val", -10.0d).save();
      database.newDocument("BoundItem").set("val", -1.0d).save();
      database.newDocument("BoundItem").set("val", 5.0d).save();
    });
    FacetOracle.awaitCaughtUp(database);

    for (final boolean accelerated : new boolean[] { false, true }) {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, accelerated);

      try (final ResultSet rs = database.query("sql", "SELECT min(val) AS mn, max(val) AS mx FROM BoundItem")) {
        assertThat(rs.hasNext()).isTrue();
        final Result res = rs.next();
        final double min = res.getProperty("mn");
        final double max = res.getProperty("mx");
        assertThat(min).isEqualTo(-10.0d);
        assertThat(max).isEqualTo(5.0d);
      }
    }
  }

  @Test
  void groupByAndFacetsCollapseNegativeAndPositiveZeroIntoSingleKey() {
    assertZerosAreOneKey("ZeroDoubleItem", "DOUBLE", -0.0d, 0.0d);
    assertZerosAreOneKey("ZeroFloatItem", "FLOAT", -0.0f, 0.0f);
  }

  /** The column now holds -0.0 as it is: GROUP BY, TERMS and DISTINCT must still see one value for both zeros. */
  private void assertZerosAreOneKey(final String type, final String propertyType, final Number negativeZero, final Number positiveZero) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".val " + propertyType);
    database.command("sql", "CREATE INDEX ON " + type + " (val) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);

    database.transaction(() -> {
      database.newDocument(type).set("val", negativeZero).save();
      database.newDocument(type).set("val", positiveZero).save();
      database.newDocument(type).set("val", negativeZero).save();
    });
    FacetOracle.awaitCaughtUp(database);

    for (final boolean accelerated : new boolean[] { false, true }) {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, accelerated);
      try (final ResultSet rs = database.query("sql", "SELECT val, count(*) AS cnt FROM " + type + " GROUP BY val")) {
        assertThat(rs.hasNext()).isTrue();
        final Result res = rs.next();
        assertThat(((Number) res.getProperty("cnt")).longValue()).as(type + " GROUP BY accelerated=" + accelerated).isEqualTo(3L);
        assertThat(rs.hasNext()).isFalse();
      }
    }
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, true);

    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " FACETS t: val TERMS")) {
      final FacetResult facet = rs.getFacets().get("t");
      assertThat(facet.buckets()).as(type + " TERMS").hasSize(1);
      assertThat(facet.buckets().getFirst().count()).isEqualTo(3L);
    }

    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " FACETS d: val DISTINCT")) {
      final FacetResult facet = rs.getFacets().get("d");
      assertThat(facet.distinct().value()).as(type + " DISTINCT").isEqualTo(1L);
    }
  }
}
