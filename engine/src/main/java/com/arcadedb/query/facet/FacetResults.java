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

import java.util.Collections;
import java.util.LinkedHashMap;
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

  public FacetResult get(final String alias) {
    return results.get(alias);
  }

  /** The aliases in declaration order. */
  public Set<String> aliases() {
    return Collections.unmodifiableSet(results.keySet());
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put(META, new JSONObject().put(EXACT, exact()).put(PATCHED, patched).put(LAG_MS, lagMs));
    for (final var entry : results.entrySet())
      json.put(entry.getKey(), entry.getValue().toJSON());
    return json;
  }

  public static FacetResults fromJSON(final JSONObject json) {
    final JSONObject meta = json.getJSONObject(META, new JSONObject());
    final LinkedHashMap<String, FacetResult> results = new LinkedHashMap<>();
    for (final String alias : json.keySet())
      if (!META.equals(alias))
        results.put(alias, FacetResult.fromJSON(json.getJSONObject(alias)));
    return new FacetResults(meta.getBoolean(EXACT, true), meta.getLong(PATCHED, 0L), meta.getLong(LAG_MS, 0L), results);
  }
}
