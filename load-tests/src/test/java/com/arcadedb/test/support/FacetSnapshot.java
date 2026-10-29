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
package com.arcadedb.test.support;

import com.arcadedb.database.Database;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A parsed facet response: the {@code meta} member plus every count normalized into one sorted map, so that the facet side and the
 * {@code GROUP BY} side compare with {@code equals}. Key format: terms bucket {@code alias=value}, range bucket {@code alias=key},
 * {@code alias.missing} and {@code alias.other}. Entries with a zero count are dropped. Only terms and range facets are covered:
 * a DISTINCT or STATS facet contributes just its {@code alias.missing}, and a HIERARCHY bucket is keyed {@code alias=value} with no
 * depth or parent, so one value at two depths would overwrite itself.
 *
 * @param exact   the response's {@code meta.exact}
 * @param patched the response's {@code meta.patched}
 * @param lagMs   the response's {@code meta.lagMs}
 * @param sources the {@code source} of every facet, by alias
 * @param counts  the normalized counts
 */
public record FacetSnapshot(boolean exact, long patched, long lagMs, Map<String, String> sources, SortedMap<String, Long> counts) {
  private static final String SOURCE_INDEX       = "index";
  private static final String SOURCE_UNAVAILABLE = "unavailable";

  /**
   * @param response the whole HTTP body: {@code {"result":[..],"facets":{..}}}
   */
  public static FacetSnapshot parse(final JSONObject response) {
    final JSONObject facets = response.getJSONObject("facets", new JSONObject());
    final JSONObject meta = facets.getJSONObject("meta", new JSONObject());
    final Map<String, String> sources = new TreeMap<>();
    final SortedMap<String, Long> counts = new TreeMap<>();

    for (final String alias : facets.keySet()) {
      if ("meta".equals(alias))
        continue;
      final JSONObject facet = facets.getJSONObject(alias);
      sources.put(alias, facet.getString("source", "unknown"));
      final JSONArray buckets = facet.getJSONArray("buckets", new JSONArray());
      for (int i = 0; i < buckets.length(); i++) {
        final JSONObject bucket = buckets.getJSONObject(i);
        // the records without a value are counted by the result's "missing" member: skip the bucket that repeats them
        if (bucket.getBoolean("missing", false))
          continue;
        final String name = bucket.has("key") ? bucket.get("key").toString() : String.valueOf(bucket.get("value"));
        put(counts, alias + "=" + name, bucket.getLong("count", 0L));
      }
      put(counts, alias + ".other", facet.getLong("otherCount", 0L));
      put(counts, alias + ".missing", facet.getLong("missing", 0L));
    }
    return new FacetSnapshot(meta.getBoolean("exact", false), meta.getLong("patched", 0L), meta.getLong("lagMs", 0L), sources, counts);
  }

  /**
   * Embedded results with the values as the engine holds them: the same key text as the HTTP side for strings and numbers, but a
   * temporal term is written as the engine's value, not as HTTP formats it. Use {@link #of(Database, FacetResults)} for those.
   */
  public static FacetSnapshot of(final FacetResults results) {
    return parse(new JSONObject().put("facets", results.toJSON()));
  }

  /** Embedded results encoded exactly as the HTTP handlers encode them (temporals formatted by the schema's formats). */
  public static FacetSnapshot of(final Database database, final FacetResults results) {
    return parse(new JSONObject().put("facets", JsonSerializer.createJsonSerializer().serializeFacets(database, results)));
  }

  /** @return true when no lag is reported and every facet is answered by the index */
  public boolean settled() {
    return lagMs == 0L && sources.values().stream().allMatch(SOURCE_INDEX::equals);
  }

  /** @return true when the counts are exact and no facet is unavailable, so they can be compared with a {@code GROUP BY} */
  public boolean comparable() {
    return exact && !sources.containsValue(SOURCE_UNAVAILABLE);
  }

  /** @return false for an inexact answer that reports no lag */
  public boolean metaConsistent() {
    return exact || lagMs > 0L;
  }

  /**
   * @return one line per key whose count differs, over the sorted union of keys (absent = 0), at most {@code limit} lines
   */
  public static List<String> diff(final Map<String, Long> expected, final Map<String, Long> actual, final int limit) {
    final List<String> lines = new ArrayList<>();
    final TreeSet<String> keys = new TreeSet<>(expected.keySet());
    keys.addAll(actual.keySet());
    for (final String key : keys) {
      if (lines.size() >= limit)
        break;
      final long e = expected.getOrDefault(key, 0L);
      final long a = actual.getOrDefault(key, 0L);
      if (e != a)
        lines.add(key + " expected=" + e + " actual=" + a);
    }
    return lines;
  }

  private static void put(final Map<String, Long> counts, final String key, final long count) {
    if (count != 0L)
      counts.put(key, count);
  }
}
