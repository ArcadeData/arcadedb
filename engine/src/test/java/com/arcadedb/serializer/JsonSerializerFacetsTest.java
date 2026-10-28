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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.query.facet.DistinctValue;
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetStats;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.serializer.json.NonFiniteNumbers;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code facets} member a transport writes ({@link JsonSerializer#serializeFacets}) for the plan 5 kinds: every value of the data,
 * the sum and the average of a STATS facet included, is written as a row's value of its type (a non-finite number by its name), and
 * with type hints each member whose type JSON does not round-trip carries its own entry in the facet's {@code @props}, in the form a
 * row's hint takes ({@code name:<type id>,...}), so a client restores the Java type the embedded result holds.
 */
class JsonSerializerFacetsTest extends TestHelper {

  /** The facets member as a client reads it: written, then parsed back from the text sent on the wire. */
  private JSONObject serialize(final FacetResults facets, final boolean hints) {
    return new JSONObject(JsonSerializer.createJsonSerializer().setIncludeTypeHints(hints).serializeFacets(database, facets).toString());
  }

  private static FacetResults one(final String alias, final FacetResult result) {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put(alias, result);
    return new FacetResults(true, 0L, 0L, map);
  }

  /** What RemoteDatabase does with a hint: the value converted to the Java type of the hinted type. */
  private static Object asRowDecoder(final Object value, final Type type) {
    return type == null ? value : Type.convertOrKeep(null, value, type.getDefaultJavaType());
  }

  @Test
  void statsSumAndAvgCarryTheirOwnHint() {
    final FacetResult stats = FacetResult.ofStats("index", new FacetStats(2L, 0L, 0.25f, 1.5f, 1.75f, 0.875d)).withValueType(Type.FLOAT);
    final JSONObject json = serialize(one("r", stats), true).getJSONObject("r");
    // a FLOAT's sum is a Float (as SELECT sum() gives it), its average a Double, which JSON round-trips without a hint
    assertThat(json.getString("@props")).isEqualTo("value:" + Type.FLOAT.getId() + ",sum:" + Type.FLOAT.getId());

    final FacetStats back = FacetResults.fromJSON(serialize(one("r", stats), true), JsonSerializerFacetsTest::asRowDecoder).get("r").stats();
    assertThat(back.min()).isEqualTo(0.25f);
    assertThat(back.sum()).isEqualTo(1.75f);
    assertThat(back.avg()).isEqualTo(0.875d);
  }

  @Test
  void statsSumOfEveryWidthKeepsItsJavaType() {
    for (final Number sum : List.<Number>of((short) 7, 7, 7L, 7.5f, 7.5d, new BigDecimal("123456789012345678901234567890"))) {
      final FacetResult stats = FacetResult.ofStats("index", new FacetStats(1L, 0L, 1, 1, sum, sum)).withValueType(Type.INTEGER);
      final FacetStats back = FacetResults.fromJSON(serialize(one("s", stats), true), JsonSerializerFacetsTest::asRowDecoder).get("s")
          .stats();
      assertThat(back.sum()).as(sum.getClass().getSimpleName()).isEqualTo(sum);
      assertThat(back.avg()).as(sum.getClass().getSimpleName()).isEqualTo(sum);
    }
  }

  /** Task 9 M1: JSON has no NaN; a non-finite sum or average is written by its name, as a row's is, never as null. */
  @Test
  void nonFiniteSumAndAvgAreWrittenByName() {
    final FacetResult nan = FacetResult.ofStats("index", new FacetStats(2L, 0L, 1.0d, Double.NaN, Double.NaN, Double.NaN))
        .withValueType(Type.DOUBLE);
    final FacetResult inf = FacetResult.ofStats("index",
        new FacetStats(2L, 0L, 1.0f, 2.0f, Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)).withValueType(Type.FLOAT);
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("nan", nan);
    map.put("inf", inf);
    final FacetResults facets = new FacetResults(true, 0L, 0L, map);
    final JSONObject json = serialize(facets, false);
    assertThat(json.getJSONObject("nan").getString("sum")).isEqualTo("NaN");
    assertThat(json.getJSONObject("nan").getString("avg")).isEqualTo("NaN");
    assertThat(json.getJSONObject("nan").getString("max")).isEqualTo("NaN");
    assertThat(json.getJSONObject("inf").getString("sum")).isEqualTo("PosInfinity");
    assertThat(json.getJSONObject("inf").getString("avg")).isEqualTo("NegInfinity");

    // read back, the sum and the average are numbers again (FacetStats holds a Number), of the hinted type when there is one
    final FacetResults back = FacetResults.fromJSON(serialize(facets, true), JsonSerializerFacetsTest::asRowDecoder);
    assertThat(back.get("nan").stats().sum()).isEqualTo(Double.NaN);
    assertThat(back.get("nan").stats().avg()).isEqualTo(Double.NaN);
    assertThat(back.get("inf").stats().sum()).isEqualTo(Float.POSITIVE_INFINITY);
    assertThat(back.get("inf").stats().avg()).isEqualTo(Double.NEGATIVE_INFINITY);
    // and without a decoder (in-process readers of the JSON)
    final FacetResults raw = FacetResults.fromJSON(serialize(facets, false));
    assertThat(raw.get("nan").stats().sum()).isEqualTo(Double.NaN);
    assertThat(raw.get("inf").stats().sum()).isEqualTo(Double.POSITIVE_INFINITY);
  }

  /**
   * The writer (the HTTP serializer) and the reader ({@link FacetResults#fromJSON}) agree on every marker of {@link NonFiniteNumbers}:
   * each non-finite sum and average is written as the marker the shared codec gives it and read back as the same number, as a FLOAT
   * when hinted so. Fails when either side spells a token of its own.
   */
  @Test
  void everyNonFiniteMarkerRoundTripsThroughTheSharedCodec() {
    // boxed: Double.equals tells NaN equal to itself
    for (final Double value : List.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
      final String marker = (String) NonFiniteNumbers.encode(value);
      assertThat(NonFiniteNumbers.decode(marker)).isEqualTo(value);
      final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
      map.put("d", FacetResult.ofStats("index", new FacetStats(1L, 0L, 1.0d, 1.0d, value, value)).withValueType(Type.DOUBLE));
      map.put("f", FacetResult.ofStats("index", new FacetStats(1L, 0L, 1.0f, 1.0f, value.floatValue(), value)).withValueType(Type.FLOAT));
      final FacetResults facets = new FacetResults(true, 0L, 0L, map);
      for (final boolean hints : new boolean[] { false, true }) {
        final JSONObject json = serialize(facets, hints);
        assertThat(json.getJSONObject("d").getString("sum")).as(marker).isEqualTo(marker);
        assertThat(json.getJSONObject("f").getString("avg")).as(marker).isEqualTo(marker);
        final FacetResults back = FacetResults.fromJSON(json, hints ? JsonSerializerFacetsTest::asRowDecoder : null);
        assertThat(back.get("d").stats().sum()).as(marker).isEqualTo(value);
        assertThat(back.get("d").stats().avg()).as(marker).isEqualTo(value);
        assertThat(back.get("f").stats().avg()).as(marker).isEqualTo(value);
        // the FLOAT sum's own hint narrows it back to a Float
        assertThat(back.get("f").stats().sum()).as(marker).isEqualTo(hints ? (Number) value.floatValue() : (Number) value);
      }
    }
  }

  @Test
  void dateStatsHintTheExtremesOnly() {
    final FacetResult stats = FacetResult.ofStats("index", new FacetStats(2L, 1L, LocalDate.of(2020, 1, 1), LocalDate.of(2024, 2, 29), null,
        null)).withValueType(Type.DATE);
    final JSONObject json = serialize(one("d", stats), true).getJSONObject("d");
    assertThat(json.getString("@props")).isEqualTo("value:" + Type.DATE.getId());
    assertThat(json.getString("min")).isEqualTo("2020-01-01");
    assertThat(json.isNull("sum")).isTrue();
    assertThat(json.isNull("avg")).isTrue();
    final FacetStats back = FacetResults.fromJSON(serialize(one("d", stats), true), JsonSerializerFacetsTest::asRowDecoder).get("d").stats();
    assertThat(back.sum()).isNull();
    assertThat(back.avg()).isNull();
  }

  @Test
  void hierarchyAndDistinctNeedNoHint() {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("t", FacetResult.of("hierarchy", "index", List.of(FacetBucket.term("a/b", 3L)
        .withAttributes(new JSONObject().put("parent", "a").put("depth", 1)), FacetBucket.term("x", 1L)
        .withAttributes(new JSONObject().put("parent", (Object) null).put("depth", 0).put("orphan", true))), 0L, 0L).withValueType(Type.STRING));
    map.put("d", FacetResult.ofDistinct("index", new DistinctValue(42L, true), 3L));
    final JSONObject json = serialize(new FacetResults(true, 0L, 0L, map), true);
    assertThat(json.getJSONObject("t").has("@props")).isFalse();
    assertThat(json.getJSONObject("d").has("@props")).isFalse();
    final JSONObject orphan = json.getJSONObject("t").getJSONArray("buckets").getJSONObject(1);
    assertThat(orphan.has("parent")).isTrue();
    assertThat(orphan.isNull("parent")).isTrue();
    assertThat(orphan.getBoolean("orphan")).isTrue();
    assertThat(json.getJSONObject("d").getLong("value")).isEqualTo(42L);
  }
}
