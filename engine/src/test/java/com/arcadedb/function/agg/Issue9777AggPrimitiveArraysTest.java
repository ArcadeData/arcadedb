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
package com.arcadedb.function.agg;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9777: every {@code agg.*} function read its list argument through a switch over four array shapes, so a {@code float[]}
 * or a {@code short[]} - how an {@code ARRAY_OF_FLOATS}/{@code ARRAY_OF_SHORTS} property and every {@code vector.*} function
 * hand their values over - read as an empty list: {@code agg.median} answered null and {@code agg.statistics} a count of 0,
 * while {@code coll.sum} answered the same column right.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9777AggPrimitiveArraysTest extends TestHelper {
  private static final Object[] SHAPES = { List.of(1.0, 2.0, 3.0, 4.0), new double[] { 1, 2, 3, 4 }, new int[] { 1, 2, 3, 4 },
      new long[] { 1, 2, 3, 4 }, new float[] { 1, 2, 3, 4 }, new short[] { 1, 2, 3, 4 }, new byte[] { 1, 2, 3, 4 },
      new Integer[] { 1, 2, 3, 4 } };

  @Test
  void storedFloatAndShortArrayColumnsAreReadByTheAggFunctions() {
    database.command("sql", "CREATE DOCUMENT TYPE M");
    database.getSchema().getType("M").createProperty("fv", Type.ARRAY_OF_FLOATS);
    database.getSchema().getType("M").createProperty("dv", Type.ARRAY_OF_DOUBLES);
    database.getSchema().getType("M").createProperty("sv", Type.ARRAY_OF_SHORTS);
    database.transaction(() -> database.newDocument("M").set("fv", new float[] { 1, 2, 3, 4 }).set("dv", new double[] { 1, 2, 3, 4 })
        .set("sv", new short[] { 1, 2, 3, 4 }).save());

    try (final ResultSet rs = database.query("sql",
        "SELECT `agg.median`(fv) AS m, `agg.median`(dv) AS md, `agg.median`(sv) AS ms, `agg.statistics`(fv) AS st, "
            + "`agg.first`(fv) AS f, `agg.last`(sv) AS l, `coll.sum`(fv) AS s FROM M")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("m")).isEqualTo(2.5);
      assertThat(row.<Object>getProperty("md")).isEqualTo(2.5);
      assertThat(row.<Object>getProperty("ms")).isEqualTo(2.5);
      assertThat(((Number) row.<Map<String, Object>>getProperty("st").get("count")).intValue()).isEqualTo(4);
      assertThat(row.<Map<String, Object>>getProperty("st").get("sum")).isEqualTo(10.0);
      assertThat(((Number) row.getProperty("f")).doubleValue()).isEqualTo(1.0);
      assertThat(((Number) row.getProperty("l")).intValue()).isEqualTo(4);
      assertThat(((Number) row.getProperty("s")).doubleValue()).isEqualTo(10.0);
    }
  }

  @Test
  void theOutputOfAVectorFunctionIsAList() {
    try (final ResultSet rs = database.query("sql", "SELECT `agg.statistics`(`vector.scale`([1.0, 2.0, 3.0], 2)) AS st")) {
      assertThat(((Number) rs.next().<Map<String, Object>>getProperty("st").get("count")).intValue()).isEqualTo(3);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void everyArrayShapeIsReadWholeByEveryAggFunction() {
    for (final Object shape : SHAPES) {
      final String as = shape.getClass().getSimpleName();
      assertThat(((Number) ((Map<String, Object>) new AggStatistics().execute(new Object[] { shape }, null)).get("count")).intValue())
          .as(as).isEqualTo(4);
      assertThat(new AggMedian().execute(new Object[] { shape }, null)).as(as).isEqualTo(2.5);
      assertThat(new AggProduct().execute(new Object[] { shape }, null)).as(as).isEqualTo(24.0);
      assertThat(((Number) new AggFirst().execute(new Object[] { shape }, null)).doubleValue()).as(as).isEqualTo(1.0);
      assertThat(((Number) new AggLast().execute(new Object[] { shape }, null)).doubleValue()).as(as).isEqualTo(4.0);
      assertThat(((Number) new AggNth().execute(new Object[] { shape, 1 }, null)).doubleValue()).as(as).isEqualTo(2.0);
      assertThat((List<Object>) new AggSlice().execute(new Object[] { shape, 0, 2 }, null)).as(as).hasSize(2);
      assertThat((List<Object>) new AggPercentiles().execute(new Object[] { shape, List.of(0.5) }, null)).as(as).hasSize(1);
      assertThat(((Map<String, Object>) new AggMinItems().execute(new Object[] { shape, List.of("a", "b", "c", "d") }, null)).get(
          "items")).as(as).isEqualTo(List.of("a"));
      assertThat(((Map<String, Object>) new AggMaxItems().execute(new Object[] { shape, List.of("a", "b", "c", "d") }, null)).get(
          "items")).as(as).isEqualTo(List.of("d"));
    }
  }

  @Test
  void theCallersListIsNeverHandedBackOrSorted() {
    final List<Object> input = new ArrayList<>(List.of(4.0, 1.0, 3.0, 2.0));
    final Object slice = new AggSlice().execute(new Object[] { input, 0, 2 }, null);
    new AggMedian().execute(new Object[] { input }, null);
    ((List<Object>) slice).clear();
    assertThat(input).containsExactly(4.0, 1.0, 3.0, 2.0);
  }

  @Test
  void aScalarIsStillAOneElementList() {
    assertThat(new AggMedian().execute(new Object[] { 7 }, null)).isEqualTo(7.0);
    assertThat(new AggFirst().execute(new Object[] { "x" }, null)).isEqualTo("x");
  }
}
