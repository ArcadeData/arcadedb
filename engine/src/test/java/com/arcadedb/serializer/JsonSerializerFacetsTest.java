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
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code facets} member a transport writes ({@link JsonSerializer#serializeFacets}): with type hints a facet carries the
 * {@code @props} hint a row of its property would, so a client restores the Java type the embedded result holds.
 */
class JsonSerializerFacetsTest extends TestHelper {

  /** The facets member as a client reads it: written, then parsed back from the text sent on the wire. */
  private JSONObject serialize(final FacetResults facets, final boolean hints) {
    return new JSONObject(JsonSerializer.createJsonSerializer().setIncludeTypeHints(hints).serializeFacets(database, facets).toString());
  }

  /** What RemoteDatabase does with a hint: the value converted to the Java type of the hinted type. */
  private static Object asRowDecoder(final Object value, final Type type) {
    return type == null ? value : Type.convertOrKeep(null, value, type.getDefaultJavaType());
  }

  /**
   * Issue #10899 for facets: a DOUBLE term that is non-finite goes out as its marker string, so it carries the DOUBLE hint a row's
   * non-finite DOUBLE carries, and the client reads the number back instead of the marker. A finite DOUBLE facet keeps no hint, as a
   * row's finite DOUBLE does.
   */
  @Test
  void nonFiniteDoubleTermsCarryTheDoubleHint() {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("t", FacetResult.of("terms", "index", List.of(FacetBucket.term(1.25d, 2L), FacetBucket.term(Double.NaN, 1L)), 0L, 0L)
        .withValueType(Type.DOUBLE));
    map.put("finite", FacetResult.of("terms", "index", List.of(FacetBucket.term(1.25d, 2L)), 0L, 0L).withValueType(Type.DOUBLE));
    final FacetResults facets = new FacetResults(true, 0L, 0L, map);

    final JSONObject json = serialize(facets, true);
    assertThat(json.getJSONObject("t").getString("@props")).isEqualTo("value:" + Type.DOUBLE.getId());
    assertThat(json.getJSONObject("finite").has("@props")).isFalse();
    assertThat(serialize(facets, false).getJSONObject("t").has("@props")).isFalse();

    final FacetResults back = FacetResults.fromJSON(json, JsonSerializerFacetsTest::asRowDecoder);
    assertThat(back.get("t").buckets().get(1).value()).isEqualTo(Double.NaN);
  }
}
