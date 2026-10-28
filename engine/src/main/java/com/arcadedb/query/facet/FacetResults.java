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

import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The facets of one query: one {@link FacetResult} per alias in declaration order, and how far the counts can be trusted. The JSON of
 * {@link #toJSON()} is the {@code facets} object of every transport, and {@link #fromJSON} parses it back.
 */
public final class FacetResults {
  private static final String META    = "meta";
  private static final String EXACT   = "exact";
  private static final String PATCHED = "patched";
  private static final String LAG_MS  = "lagMs";

  private final boolean                         exact;
  private final long                            patched;
  private final long                            lagMs;
  private final LinkedHashMap<String, FacetResult> results;
  /** How each facet was computed (EXPLAIN, PROFILE); never serialized, empty for results parsed from JSON. */
  private       Map<String, FacetExecutionStats>   executionStats = Map.of();

  /**
   * @param exact   false when the counts may lag the committed data (patch budget exceeded); an approximate DISTINCT also makes the
   *                answer inexact, see {@link #exact()}
   * @param patched the number of records read from the record itself to correct the projection
   * @param lagMs   how far the projection was behind the committed data
   */
  public FacetResults(final boolean exact, final long patched, final long lagMs, final LinkedHashMap<String, FacetResult> results) {
    this.exact = exact;
    this.patched = patched;
    this.lagMs = lagMs;
    this.results = new LinkedHashMap<>(results);
  }

  /** False when the constructor said so or when any facet is an approximate DISTINCT. */
  public boolean exact() {
    if (!exact)
      return false;
    for (final FacetResult result : results.values())
      if (result.distinct() != null && !result.distinct().exact())
        return false;
    return true;
  }

  public long patched() {
    return patched;
  }

  public long lagMs() {
    return lagMs;
  }

  /** The execution statistics by alias (PROFILE); empty unless the executor computed these results. */
  public Map<String, FacetExecutionStats> executionStats() {
    return executionStats;
  }

  void setExecutionStats(final Map<String, FacetExecutionStats> executionStats) {
    this.executionStats = Collections.unmodifiableMap(executionStats);
  }

  public FacetResult get(final String alias) {
    return results.get(alias);
  }

  /** The aliases in declaration order. */
  public Set<String> aliases() {
    return Collections.unmodifiableSet(results.keySet());
  }

  /**
   * How a transport writes a value of the data (a term, a bound, a cursor) into its JSON: the JSON form of {@code value}, a value of a
   * property of schema type {@code type} (null when unknown). The HTTP serializer writes it as it writes that property in a row.
   */
  @FunctionalInterface
  public interface ValueEncoder {
    Object encode(Object value, Type type);
  }

  /** How a client reads a value of the data back from the JSON of its transport, as it reads a row's value of type {@code type}. */
  @FunctionalInterface
  public interface ValueDecoder {
    Object decode(Object value, Type type);
  }

  /** The values as the engine holds them (integral numbers as {@code Long}): the JSON every in-process consumer reads. */
  public static final ValueEncoder AS_IS = (value, type) -> value;

  /**
   * A facet value as the value a row of the property holds: a number converted to the Java type of {@code type} (a DOUBLE term the
   * engine keys as an exact decimal is a {@code Double} again, an INTEGER one an {@code Integer}), anything else unchanged. Applied
   * once, by the executor, to every term and cursor of a facet whose property type it knows: every caller, embedded or remote, gets
   * what GROUP BY gets.
   */
  public static Object asRowValue(final Object value, final Type type) {
    if (!(value instanceof Number number) || !isNumeric(type))
      return value;
    try {
      return Type.convert(null, number, type.getDefaultJavaType());
    } catch (final RuntimeException e) {
      return value;
    }
  }

  private static boolean isNumeric(final Type type) {
    return type != null && switch (type) {
      case BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL -> true;
      default -> false;
    };
  }

  public JSONObject toJSON() {
    return toJSON(AS_IS);
  }

  /** @param encoder writes each value of the data, see {@link ValueEncoder} */
  public JSONObject toJSON(final ValueEncoder encoder) {
    final JSONObject json = new JSONObject();
    json.put(META, new JSONObject().put(EXACT, exact()).put(PATCHED, patched).put(LAG_MS, lagMs));
    for (final var entry : results.entrySet())
      json.put(entry.getKey(), entry.getValue().toJSON(encoder));
    return json;
  }

  /**
   * {@link #toJSON()} without any value of the data, for a plan's description that outlives the query (the server's query profiler
   * keeps it): the meta, and per facet its kind, source, reason and counts, each bucket reduced to its count and its missing or default
   * flag. No term, no bound, no label derived from a value, no cursor, and of a STATS facet its counts only (its min, max, sum and
   * average are values of the data too).
   */
  public JSONObject toProfileJSON() {
    final JSONObject json = toJSON();
    for (final String alias : results.keySet()) {
      final JSONObject result = json.getJSONObject(alias);
      for (final String value : new String[] { "after", "min", "max", "sum", "avg" })
        result.remove(value);
      final JSONArray buckets = result.getJSONArray("buckets", null);
      if (buckets == null)
        continue;
      final JSONArray counts = new JSONArray();
      for (int i = 0; i < buckets.length(); i++) {
        final JSONObject bucket = buckets.getJSONObject(i);
        final JSONObject count = new JSONObject().put("count", bucket.getLong("count", 0L));
        if (bucket.getBoolean("missing", false))
          count.put("missing", true);
        if (bucket.getBoolean("default", false))
          count.put("default", true);
        counts.put(count);
      }
      result.put("buckets", counts);
    }
    return json;
  }

  public static FacetResults fromJSON(final JSONObject json) {
    return fromJSON(json, null);
  }

  /** @param decoder reads each value of the data back, see {@link FacetResult#fromJSON(JSONObject, ValueDecoder)} */
  public static FacetResults fromJSON(final JSONObject json, final ValueDecoder decoder) {
    final JSONObject meta = json.getJSONObject(META, new JSONObject());
    final LinkedHashMap<String, FacetResult> results = new LinkedHashMap<>();
    for (final String alias : json.keySet())
      if (!META.equals(alias))
        results.put(alias, FacetResult.fromJSON(json.getJSONObject(alias), decoder));
    return new FacetResults(meta.getBoolean(EXACT, true), meta.getLong(PATCHED, 0L), meta.getLong(LAG_MS, 0L), results);
  }
}
