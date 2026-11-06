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

import com.arcadedb.serializer.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.List;

/** Shared samples of facet results, public so the transport tests of the other modules can reuse them. */
public final class FacetResultsSamples {
  private FacetResultsSamples() {
  }

  /** Terms with the missing bucket, a range with an unbounded end and a default, a histogram and an unavailable facet. */
  public static FacetResults allKinds() {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("brand", FacetResult.of("terms", "index",
        List.of(FacetBucket.term("Nike", 1240L), FacetBucket.term(7L, 30L), FacetBucket.term(1.5, 2L), FacetBucket.missing(4L)), 55L, 9L));
    map.put("size", FacetResult.of("range", "scan",
        List.of(FacetBucket.range("small", null, 1024L, 5L), FacetBucket.range("large", 10485760L, null, 7L),
            FacetBucket.rangeDefault("other", 3L)), 0L, 2L));
    map.put("pub", FacetResult.of("histogram", "index",
        List.of(FacetBucket.histogram("2026-01", 1767225600000L, 1769904000000L, 12L), FacetBucket.histogram("2026-02", 1769904000000L, 1772323200000L, 0L)),
        0L, 1L));
    map.put("gone", FacetResult.unavailable("terms", "memory budget"));
    return new FacetResults(true, 12L, 340L, map);
  }

  /** Bucket attributes, DISTINCT, STATS and an AFTER cursor, with and without a following value. */
  public static FacetResults plan5Members() {
    final LinkedHashMap<String, FacetResult> map = new LinkedHashMap<>();
    map.put("tree", FacetResult.of("hierarchy", "index",
        List.of(FacetBucket.term("a/b", 3L).withAttributes(new JSONObject().put("parent", "a").put("depth", 1))), 0L, 0L));
    map.put("uniq", FacetResult.ofDistinct("index", new DistinctValue(7L, false), 2L));
    map.put("st", FacetResult.ofStats("index", new FacetStats(10L, 2L, 1L, 99L, 500L, 50.0)));
    map.put("more", FacetResult.of("terms", "index", List.of(FacetBucket.term("Nike", 5L)), 3L, 0L).withAfter("Nike"));
    map.put("last", FacetResult.of("terms", "index", List.of(FacetBucket.term("Zed", 1L)), 0L, 0L).withAfter(null));
    return new FacetResults(true, 0L, 0L, map);
  }
}
