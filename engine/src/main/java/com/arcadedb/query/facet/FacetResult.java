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

import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * The answer of one facet: its buckets, the number of counts the returned buckets leave out and the number of records without a
 * value. A DISTINCT or STATS facet carries {@link #distinct()} or {@link #stats()} instead of buckets.
 * <p>
 * {@link #valueType()} is the schema type of the property a terms, hierarchy or stats facet counts, when it has one: a transport
 * encodes the values of the buckets (and the min and max of a stats facet) as it encodes a row's value of that property, and a client
 * decodes them as it decodes such a row (final review C-I8).
 */
public final class FacetResult {
  /** The {@link #source()} of a facet that could not be computed. */
  public static final String UNAVAILABLE = "unavailable";

  /**
   * The member of a result that names the type of its bucket values ({@code value:<type id>}), written by a transport that sends the
   * type hints of its rows, under the name and in the form a row's hint takes.
   */
  public static final String VALUE_TYPE_HINT = Property.PROPERTY_TYPES_PROPERTY;

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
  private final Type               valueType;

  private FacetResult(final String kind, final String source, final String reason, final List<FacetBucket> buckets, final long otherCount,
      final long missing, final DistinctValue distinct, final FacetStats stats, final Object after, final boolean hasAfter,
      final Type valueType) {
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
    this.valueType = valueType;
  }

  /**
   * @param kind       {@code terms}, {@code range}, {@code histogram} or {@code hierarchy}
   * @param source     {@code index} or {@code scan}; {@link #unavailable} builds the {@code unavailable} one
   * @param otherCount the counts of the values not returned; written for terms and hierarchy facets only
   * @param missing    the number of records without a value
   */
  public static FacetResult of(final String kind, final String source, final List<FacetBucket> buckets, final long otherCount, final long missing) {
    return new FacetResult(kind, source, null, List.copyOf(buckets), otherCount, missing, null, null, null, false, null);
  }

  /** A facet that could not be computed: no buckets, only the reason. */
  public static FacetResult unavailable(final String kind, final String reason) {
    return new FacetResult(kind, UNAVAILABLE, reason, List.of(), 0L, 0L, null, null, null, false, null);
  }

  public static FacetResult ofDistinct(final String source, final DistinctValue distinct, final long missing) {
    return new FacetResult("distinct", source, null, List.of(), 0L, missing, distinct, null, null, false, null);
  }

  public static FacetResult ofStats(final String source, final FacetStats stats) {
    return new FacetResult("stats", source, null, List.of(), 0L, stats.missing(), null, stats, null, false, null);
  }

  /** The same result with the cursor of the next page; a null {@code after} means no value follows. */
  public FacetResult withAfter(final Object after) {
    return new FacetResult(kind, source, reason, buckets, otherCount, missing, distinct, stats, after, true, valueType);
  }

  /** The same result with another {@link #source()}: the executor marks a facet {@code scan} once a bucket of it was read from records. */
  FacetResult withSource(final String source) {
    return new FacetResult(kind, source, reason, buckets, otherCount, missing, distinct, stats, after, hasAfter, valueType);
  }

  /** The same result with the schema type of its bucket values (null when unknown). */
  public FacetResult withValueType(final Type valueType) {
    return new FacetResult(kind, source, reason, buckets, otherCount, missing, distinct, stats, after, hasAfter, valueType);
  }

  /** The same result with other buckets: a transport that decodes the values itself rebuilds them. */
  public FacetResult withBuckets(final List<FacetBucket> buckets) {
    return new FacetResult(kind, source, reason, List.copyOf(buckets), otherCount, missing, distinct, stats, after, hasAfter, valueType);
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

  /** The schema type of the property whose values the buckets hold (a terms facet over a declared property), null when unknown. */
  public Type valueType() {
    return valueType;
  }

  public JSONObject toJSON() {
    return toJSON(FacetResults.AS_IS);
  }

  /**
   * @param encoder writes each value of the data (bucket values, a STATS facet's min and max and the cursor with {@link #valueType()},
   *                bounds without a type)
   */
  public JSONObject toJSON(final FacetResults.ValueEncoder encoder) {
    final JSONObject json = new JSONObject();
    json.put(TYPE, kind);
    json.put(SOURCE, source);
    if (reason != null)
      json.put(REASON, reason);
    if (!UNAVAILABLE.equals(source)) {
      if (stats != null) {
        json.put(COUNT, stats.count());
        json.put(MISSING, stats.missing());
        // the extremes are values of the property, encoded as a row's value of it (a date in the row's date format)
        json.put(MIN, stats.min() == null ? null : encoder.encode(FacetBucket.integral(stats.min()), valueType));
        json.put(MAX, stats.max() == null ? null : encoder.encode(FacetBucket.integral(stats.max()), valueType));
        json.put(SUM, FacetBucket.integral(stats.sum()));
        json.put(AVG, FacetBucket.integral(stats.avg()));
      } else {
        if (distinct != null) {
          json.put(VALUE, distinct.value());
          json.put(EXACT, distinct.exact());
        } else {
          final JSONArray array = new JSONArray();
          for (final FacetBucket bucket : buckets)
            array.put(bucket.toJSON(encoder, valueType));
          json.put(BUCKETS, array);
          if (hasOtherCount())
            json.put(OTHER_COUNT, otherCount);
        }
        json.put(MISSING, missing);
      }
    }
    if (hasAfter)
      json.put(AFTER, encoder.encode(FacetBucket.integral(after), valueType));
    return json;
  }

  public static FacetResult fromJSON(final JSONObject json) {
    return fromJSON(json, null);
  }

  /**
   * @param decoder reads each value of the data back (bucket values, a STATS facet's min and max and the cursor, with the type of the {@link #VALUE_TYPE_HINT}
   *                member, null without one); null to keep the values as JSON holds them, integral numbers as {@code Long}
   */
  public static FacetResult fromJSON(final JSONObject json, final FacetResults.ValueDecoder decoder) {
    final String kind = json.getString(TYPE);
    final String source = json.getString(SOURCE);
    final Type valueType = valueTypeOf(json.getString(VALUE_TYPE_HINT, null));
    FacetResult result;
    if (UNAVAILABLE.equals(source))
      result = unavailable(kind, json.getString(REASON, null));
    else if ("stats".equals(kind))
      result = ofStats(source,
          new FacetStats(json.getLong(COUNT, 0L), json.getLong(MISSING, 0L), FacetBucket.decode(json.opt(MIN), decoder, valueType),
              FacetBucket.decode(json.opt(MAX), decoder, valueType), (Number) FacetBucket.integral(json.opt(SUM)),
              FacetBucket.integral(json.opt(AVG))));
    else if ("distinct".equals(kind))
      result = ofDistinct(source, new DistinctValue(json.getLong(VALUE, 0L), json.getBoolean(EXACT, true)), json.getLong(MISSING, 0L));
    else {
      final JSONArray array = json.getJSONArray(BUCKETS, new JSONArray());
      final List<FacetBucket> list = new ArrayList<>(array.length());
      for (int i = 0; i < array.length(); i++)
        list.add(FacetBucket.fromJSON(array.getJSONObject(i), decoder, valueType));
      result = of(kind, source, list, json.getLong(OTHER_COUNT, 0L), json.getLong(MISSING, 0L));
    }
    if (json.has(AFTER))
      result = result.withAfter(FacetBucket.decode(json.opt(AFTER), decoder, valueType));
    return valueType != null ? result.withValueType(valueType) : result;
  }

  /** The type a {@link #VALUE_TYPE_HINT} member names ({@code value:<type id>}), null for none or one this version does not know. */
  private static Type valueTypeOf(final String hint) {
    if (hint == null || !hint.startsWith(VALUE + ":"))
      return null;
    try {
      return Type.getById(Byte.parseByte(hint.substring(VALUE.length() + 1)));
    } catch (final RuntimeException e) {
      return null;
    }
  }

  private boolean hasOtherCount() {
    return !"range".equals(kind) && !"histogram".equals(kind);
  }
}
