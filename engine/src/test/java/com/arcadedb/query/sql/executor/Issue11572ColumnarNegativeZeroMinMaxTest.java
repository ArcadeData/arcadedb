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
import com.arcadedb.query.facet.FacetStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests issue #11572: MIN and MAX of DOUBLE and FLOAT columnar projections preserve negative zero (-0.0)
 * rather than normalizing to 0.0, matching scan behavior.
 */
class Issue11572ColumnarNegativeZeroMinMaxTest extends TestHelper {

  @Test
  void doubleColumnarMinMaxPreservesNegativeZero() {
    database.command("sql", "CREATE DOCUMENT TYPE DoubleItem");
    database.command("sql", "CREATE PROPERTY DoubleItem.grp STRING");
    database.command("sql", "CREATE PROPERTY DoubleItem.val DOUBLE");
    database.command("sql", "CREATE INDEX ON DoubleItem (grp) COLUMNAR");
    database.command("sql", "CREATE INDEX ON DoubleItem (val) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);

    database.transaction(() -> {
      // Group A: only negative zero
      database.newDocument("DoubleItem").set("grp", "A").set("val", -0.0d).save();
      // Group B: negative zero and positive zero
      database.newDocument("DoubleItem").set("grp", "B").set("val", -0.0d).save();
      database.newDocument("DoubleItem").set("grp", "B").set("val", 0.0d).save();
      // Group C: only positive zero
      database.newDocument("DoubleItem").set("grp", "C").set("val", 0.0d).save();
    });
    FacetOracle.awaitCaughtUp(database);

    for (final boolean accelerated : new boolean[] { false, true }) {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, accelerated);

      // Group A: min and max over {-0.0} should both be -0.0
      try (final ResultSet rs = database.query("sql", "SELECT min(val) AS minVal, max(val) AS maxVal FROM DoubleItem WHERE grp = 'A' GROUP BY grp")) {
        assertThat(rs.hasNext()).isTrue();
        final Result res = rs.next();
        final double minA = res.getProperty("minVal");
        final double maxA = res.getProperty("maxVal");
        assertThat(Double.doubleToRawLongBits(minA)).as("Group A min accelerated=" + accelerated)
            .isEqualTo(Double.doubleToRawLongBits(-0.0d));
        assertThat(Double.doubleToRawLongBits(maxA)).as("Group A max accelerated=" + accelerated)
            .isEqualTo(Double.doubleToRawLongBits(-0.0d));
      }

      // Group B: min over {0.0, -0.0} should be -0.0
      try (final ResultSet rs = database.query("sql", "SELECT min(val) AS minVal FROM DoubleItem WHERE grp = 'B' GROUP BY grp")) {
        assertThat(rs.hasNext()).isTrue();
        final Result res = rs.next();
        final double minB = res.getProperty("minVal");
        assertThat(Double.doubleToRawLongBits(minB)).as("Group B min accelerated=" + accelerated)
            .isEqualTo(Double.doubleToRawLongBits(-0.0d));
      }

      // Group C: min and max over {0.0} should both be 0.0
      try (final ResultSet rs = database.query("sql", "SELECT min(val) AS minVal, max(val) AS maxVal FROM DoubleItem WHERE grp = 'C' GROUP BY grp")) {
        assertThat(rs.hasNext()).isTrue();
        final Result res = rs.next();
        final double minC = res.getProperty("minVal");
        final double maxC = res.getProperty("maxVal");
        assertThat(Double.doubleToRawLongBits(minC)).as("Group C min accelerated=" + accelerated)
            .isEqualTo(Double.doubleToRawLongBits(0.0d));
        assertThat(Double.doubleToRawLongBits(maxC)).as("Group C max accelerated=" + accelerated)
            .isEqualTo(Double.doubleToRawLongBits(0.0d));
      }
    }
  }

  @Test
  void floatColumnarMinMaxPreservesNegativeZero() {
    database.command("sql", "CREATE DOCUMENT TYPE FloatItem");
    database.command("sql", "CREATE PROPERTY FloatItem.grp STRING");
    database.command("sql", "CREATE PROPERTY FloatItem.val FLOAT");
    database.command("sql", "CREATE INDEX ON FloatItem (grp) COLUMNAR");
    database.command("sql", "CREATE INDEX ON FloatItem (val) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);

    database.transaction(() -> {
      // Group A: only negative zero
      database.newDocument("FloatItem").set("grp", "A").set("val", -0.0f).save();
      // Group B: negative zero and positive zero
      database.newDocument("FloatItem").set("grp", "B").set("val", -0.0f).save();
      database.newDocument("FloatItem").set("grp", "B").set("val", 0.0f).save();
      // Group C: only positive zero
      database.newDocument("FloatItem").set("grp", "C").set("val", 0.0f).save();
    });
    FacetOracle.awaitCaughtUp(database);

    for (final boolean accelerated : new boolean[] { false, true }) {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, accelerated);

      try (final ResultSet rs = database.query("sql", "SELECT min(val) AS minVal, max(val) AS maxVal FROM FloatItem WHERE grp = 'A' GROUP BY grp")) {
        assertThat(rs.hasNext()).isTrue();
        final Result res = rs.next();
        final float minA = res.getProperty("minVal");
        final float maxA = res.getProperty("maxVal");
        assertThat(Float.floatToRawIntBits(minA)).as("Float Group A min accelerated=" + accelerated)
            .isEqualTo(Float.floatToRawIntBits(-0.0f));
        assertThat(Float.floatToRawIntBits(maxA)).as("Float Group A max accelerated=" + accelerated)
            .isEqualTo(Float.floatToRawIntBits(-0.0f));
      }

      try (final ResultSet rs = database.query("sql", "SELECT min(val) AS minVal FROM FloatItem WHERE grp = 'B' GROUP BY grp")) {
        assertThat(rs.hasNext()).isTrue();
        final Result res = rs.next();
        final float minB = res.getProperty("minVal");
        assertThat(Float.floatToRawIntBits(minB)).as("Float Group B min accelerated=" + accelerated)
            .isEqualTo(Float.floatToRawIntBits(-0.0f));
      }

      try (final ResultSet rs = database.query("sql", "SELECT min(val) AS minVal, max(val) AS maxVal FROM FloatItem WHERE grp = 'C' GROUP BY grp")) {
        assertThat(rs.hasNext()).isTrue();
        final Result res = rs.next();
        final float minC = res.getProperty("minVal");
        final float maxC = res.getProperty("maxVal");
        assertThat(Float.floatToRawIntBits(minC)).as("Float Group C min accelerated=" + accelerated)
            .isEqualTo(Float.floatToRawIntBits(0.0f));
        assertThat(Float.floatToRawIntBits(maxC)).as("Float Group C max accelerated=" + accelerated)
            .isEqualTo(Float.floatToRawIntBits(0.0f));
      }
    }
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
}
