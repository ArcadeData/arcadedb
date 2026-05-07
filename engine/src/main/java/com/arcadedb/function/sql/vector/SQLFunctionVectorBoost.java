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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Score-boost reranker. Lets product teams blend vector relevance with arbitrary scalar business
 * signals (recency, popularity, user affinity) at query time, without bouncing the candidate set
 * through application code. For each upstream row computes
 * <pre>
 *   boostedScore = base_similarity + Σ_i (weight_i * row[field_i])
 * </pre>
 * and re-sorts the result by the new score descending. {@code base_similarity} is read from the
 * row's {@code score} or {@code $score} field if present, or auto-flipped from the legacy
 * {@code distance} field ({@code vector.neighbors}'s native output) so that all sources compose
 * with a single "higher is better" convention regardless of upstream shape.
 * <p>
 * Usage:
 * <pre>
 *   SELECT expand(`vector.boost`(
 *       `vector.neighbors`('Doc[embedding]', :q, 100),
 *       { boosts: [
 *           { field: 'popularity', weight: 0.1 },
 *           { field: 'recency_decay', weight: 0.05 }
 *       ], limit: 10 }
 *   ))
 * </pre>
 * Options: {@code boosts} (required, non-empty), {@code limit} (optional, default unbounded).
 * Each boost entry is a map with {@code field} (string) and {@code weight} (number). Rows whose
 * boost field is missing or null contribute 0 for that term. A non-numeric boost-field value
 * throws (silently coercing strings to numbers would mask a data-quality issue).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionVectorBoost extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.boost";

  private static final Set<String> OPTIONS = Set.of("boosts", "limit");

  public SQLFunctionVectorBoost() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object source = params[0];
    if (!(params[1] instanceof Map<?, ?> rawOpts))
      throw new CommandSQLParsingException(NAME + " 2nd argument must be an options map with 'boosts' entry");

    final FunctionOptions opts = new FunctionOptions(NAME, rawOpts, OPTIONS);
    final List<?> rawBoosts = opts.getList("boosts");
    if (rawBoosts == null || rawBoosts.isEmpty())
      throw new CommandSQLParsingException(NAME + " 'boosts' option is required and must be non-empty");
    final int limit = opts.getInt("limit", -1);

    final Boost[] boosts = parseBoosts(rawBoosts);

    final List<Scored> scored = materialize(source, boosts);
    scored.sort((a, b) -> Float.compare(b.boostedScore(), a.boostedScore()));

    final int targetSize = limit > 0 ? Math.min(limit, scored.size()) : scored.size();
    final ArrayList<Object> out = new ArrayList<>(targetSize);
    for (int i = 0; i < targetSize; i++) {
      final Scored s = scored.get(i);
      // Overwrite the score field on the existing row map so downstream {@code vector.fuse} or
      // {@code ORDER BY score} consume the boosted value. Build a new map so we never mutate
      // upstream storage (Result objects are read-only views; a shared LinkedHashMap could be
      // referenced by something else upstream).
      // <p>
      // Only Map and Result reach this point: {@link #materialize} drops any row whose score
      // cannot be extracted, and {@link SQLFunctionVectorAbstract#extractScoreFromRow} returns
      // {@code NaN} for any other shape. So no "pass-through for unknown shape" branch is needed.
      final LinkedHashMap<String, Object> rebuilt = new LinkedHashMap<>();
      if (s.row() instanceof Map<?, ?> m) {
        for (final var e : m.entrySet())
          rebuilt.put(String.valueOf(e.getKey()), e.getValue());
      } else {
        // Must be a Result - the materialize-time NaN guard ruled out everything else.
        final Result r = (Result) s.row();
        for (final String prop : r.getPropertyNames())
          rebuilt.put(prop, r.getProperty(prop));
        if (r.getIdentity().isPresent())
          rebuilt.put("@rid", r.getIdentity().get());
      }
      rebuilt.put("score", s.boostedScore());
      out.add(rebuilt);
    }
    return out;
  }

  public String getSyntax() {
    // Note on the {@code limit} option vs an outer SQL LIMIT: the option truncates the result
    // BEFORE rows leave the function (so the rebuild + sort cost scales with limit, not with the
    // upstream candidate count). An outer {@code LIMIT N} on the enclosing SELECT applies after.
    // Both can be combined: the inner limit caps reranker work, the outer limit caps final
    // result size. If only one is set, the function's truncation respects only that one.
    return NAME + "(<source>, { boosts: [{ field, weight }, ...][, limit] })";
  }

  private static List<Scored> materialize(final Object source, final Boost[] boosts) {
    if (source == null)
      return new ArrayList<>(0);
    if (!(source instanceof Iterable<?> iter))
      throw new CommandSQLParsingException(NAME + " source must be an iterable of rows, got "
          + source.getClass().getSimpleName());
    final List<Scored> out = new ArrayList<>();
    for (final Object row : iter) {
      final float base = extractScoreFromRow(row);
      if (Float.isNaN(base))
        continue;
      // Accumulate in double so a long sum of small per-field contributions does not lose
      // precision before the final cast back to float for the row's {@code score} field. Pure
      // float would be fine for typical 1-3 boost terms but the double accumulator costs
      // virtually nothing and keeps headroom.
      double boostedAcc = base;
      for (final Boost b : boosts) {
        final Object raw = readField(row, b.field);
        if (raw == null)
          continue; // missing field contributes 0
        if (!(raw instanceof Number n))
          throw new CommandSQLParsingException(NAME + " boost field '" + b.field
              + "' must be numeric on every row, got: " + raw.getClass().getSimpleName());
        boostedAcc += (double) b.weight * n.doubleValue();
      }
      out.add(new Scored(row, (float) boostedAcc));
    }
    return out;
  }

  private static Object readField(final Object row, final String field) {
    if (row instanceof Map<?, ?> m)
      return m.get(field);
    if (row instanceof Result r)
      return r.getProperty(field);
    return null;
  }

  private static Boost[] parseBoosts(final List<?> raw) {
    final Boost[] out = new Boost[raw.size()];
    for (int i = 0; i < raw.size(); i++) {
      final Object entry = raw.get(i);
      if (!(entry instanceof Map<?, ?> m))
        throw new CommandSQLParsingException(
            NAME + " boosts[" + i + "] must be a map with 'field' and 'weight', got: "
                + (entry == null ? "null" : entry.getClass().getSimpleName()));
      final Object field = m.get("field");
      if (!(field instanceof String fieldStr) || fieldStr.isEmpty())
        throw new CommandSQLParsingException(
            NAME + " boosts[" + i + "].field must be a non-empty string");
      final Object weight = m.get("weight");
      if (!(weight instanceof Number weightNum))
        throw new CommandSQLParsingException(
            NAME + " boosts[" + i + "].weight must be a number");
      out[i] = new Boost(fieldStr, weightNum.floatValue());
    }
    return out;
  }

  private record Boost(String field, float weight) {}

  private record Scored(Object row, float boostedScore) {}
}
