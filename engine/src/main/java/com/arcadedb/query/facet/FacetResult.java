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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * The answer of one facet: its buckets, the number of counts the returned buckets leave out and the number of records without a
 * value. A DISTINCT or STATS facet carries {@link #distinct()} or {@link #stats()} instead of buckets.
 */
public final class FacetResult {
  /** The {@link #source()} of a facet that could not be computed. */
  public static final String UNAVAILABLE = "unavailable";

  private static final String TYPE        = "type";
  private static final String SOURCE      = "source";
  private static final String REASON      = "reason";
  private static final String BUCKETS     = "buckets";
  private static final String OTHER_COUNT = "otherCount";
  private static final String MISSING     = "missing";
  private static final String VALUE       = "value";
  private static final String EXACT       = "exact";
  private static final String COUNT       = "count";
  private static final String MIN         = "min";
  private static final String MAX         = "max";
  private static final String SUM         = "sum";
  private static final String AVG         = "avg";
  private static final String AFTER       = "after";

  private final String             kind;
  private final String             source;
  private final String             reason;
  private final List<FacetBucket>  buckets;
  private final long               otherCount;
  private final long               missing;
  private final DistinctValue      distinct;
  private final FacetStats         stats;
  private final Object             after;
  private final boolean            hasAfter;

  private FacetResult(final String kind, final String source, final String reason, final List<FacetBucket> buckets, final long otherCount,
      final long missing, final DistinctValue distinct, final FacetStats stats, final Object after, final boolean hasAfter) {
    this.kind = kind;
    this.source = source;
    this.reason = reason;
    this.buckets = buckets;
    this.otherCount = otherCount;
    this.missing = missing;
    this.distinct = distinct;
    this.stats = stats;
    this.after = after;
    this.hasAfter = hasAfter;
  }

  /**
   * @param kind       {@code terms}, {@code range}, {@code histogram} or {@code hierarchy}
   * @param source     {@code index}, {@code scan} or {@code mixed}
   * @param otherCount the counts of the values not returned; written for terms and hierarchy facets only
   * @param missing    the number of records without a value
   */
  public static FacetResult of(final String kind, final String source, final List<FacetBucket> buckets, final long otherCount, final long missing) {
    return new FacetResult(kind, source, null, List.copyOf(buckets), otherCount, missing, null, null, null, false);
  }

  /** A facet that could not be computed: no buckets, only the reason. */
  public static FacetResult unavailable(final String kind, final String reason) {
    return new FacetResult(kind, UNAVAILABLE, reason, List.of(), 0L, 0L, null, null, null, false);
  }

  public static FacetResult ofDistinct(final String source, final DistinctValue distinct, final long missing) {
    return new FacetResult("distinct", source, null, List.of(), 0L, missing, distinct, null, null, false);
  }

  public static FacetResult ofStats(final String source, final FacetStats stats) {
    return new FacetResult("stats", source, null, List.of(), 0L, stats.missing(), null, stats, null, false);
  }

  /** The same result with the cursor of the next page; a null {@code after} means no value follows. */
  public FacetResult withAfter(final Object after) {
    return new FacetResult(kind, source, reason, buckets, otherCount, missing, distinct, stats, after, true);
  }

  public String kind() {
    return kind;
  }

  public String source() {
    return source;
  }

  /** Why the facet is unavailable; null for any other source. */
  public String reason() {
    return reason;
  }

  public List<FacetBucket> buckets() {
    return buckets;
  }

  public long otherCount() {
    return otherCount;
  }

  public long missing() {
    return missing;
  }

  public DistinctValue distinct() {
    return distinct;
  }

  public FacetStats stats() {
    return stats;
  }

  public Object after() {
    return after;
  }

  /** True when {@link #withAfter} was called, even with a null cursor. */
  public boolean hasAfter() {
    return hasAfter;
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put(TYPE, kind);
    json.put(SOURCE, source);
    if (reason != null)
      json.put(REASON, reason);
    if (!UNAVAILABLE.equals(source)) {
      if (stats != null) {
        json.put(COUNT, stats.count());
        json.put(MISSING, stats.missing());
        json.put(MIN, FacetBucket.integral(stats.min()));
        json.put(MAX, FacetBucket.integral(stats.max()));
        json.put(SUM, FacetBucket.integral(stats.sum()));
        json.put(AVG, FacetBucket.integral(stats.avg()));
      } else {
        if (distinct != null) {
          json.put(VALUE, distinct.value());
          json.put(EXACT, distinct.exact());
        } else {
          final JSONArray array = new JSONArray();
          for (final FacetBucket bucket : buckets)
            array.put(bucket.toJSON());
          json.put(BUCKETS, array);
          if (hasOtherCount())
            json.put(OTHER_COUNT, otherCount);
        }
        json.put(MISSING, missing);
      }
    }
    if (hasAfter)
      json.put(AFTER, FacetBucket.integral(after));
    return json;
  }

  public static FacetResult fromJSON(final JSONObject json) {
    final String kind = json.getString(TYPE);
    final String source = json.getString(SOURCE);
    FacetResult result;
    if (UNAVAILABLE.equals(source))
      result = unavailable(kind, json.getString(REASON, null));
    else if ("stats".equals(kind))
      result = ofStats(source,
          new FacetStats(json.getLong(COUNT, 0L), json.getLong(MISSING, 0L), FacetBucket.integral(json.opt(MIN)), FacetBucket.integral(json.opt(MAX)),
              (Number) FacetBucket.integral(json.opt(SUM)), FacetBucket.integral(json.opt(AVG))));
    else if ("distinct".equals(kind))
      result = ofDistinct(source, new DistinctValue(json.getLong(VALUE, 0L), json.getBoolean(EXACT, true)), json.getLong(MISSING, 0L));
    else {
      final JSONArray array = json.getJSONArray(BUCKETS, new JSONArray());
      final List<FacetBucket> list = new ArrayList<>(array.length());
      for (int i = 0; i < array.length(); i++)
        list.add(FacetBucket.fromJSON(array.getJSONObject(i)));
      result = of(kind, source, list, json.getLong(OTHER_COUNT, 0L), json.getLong(MISSING, 0L));
    }
    if (json.has(AFTER))
      result = result.withAfter(FacetBucket.integral(json.opt(AFTER)));
    return result;
  }

  private boolean hasOtherCount() {
    return !"range".equals(kind) && !"histogram".equals(kind);
  }
}
