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

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Server-side hybrid retrieval fusion. Combines ranked output from two or more sub-pipelines into a
 * single ranked top-K using one of three strategies:
 * <ul>
 *   <li><b>RRF</b> - Reciprocal Rank Fusion. {@code score = Σ_i weight_i / (k + rank_i)}; rank-only,
 *       does not require per-row scores.</li>
 *   <li><b>DBSF</b> - Distribution-Based Score Fusion (Qdrant 1.11+). Per-source score normalization
 *       to {@code [mean - 3σ, mean + 3σ]}, then weighted sum.</li>
 *   <li><b>LINEAR</b> - Per-source min-max normalization, then weighted sum. Requires per-row scores.</li>
 * </ul>
 * <p>
 * Usage: {@code vector.fuse(source1, source2, ..., sourceN [, options])}
 * <p>
 * Each source is the output of a sub-pipeline that yields ranked rows with at least an {@code @rid}.
 * Recognised sources include {@code vector.neighbors}, {@code vector.sparseNeighbors},
 * {@code SEARCH_INDEX(...)}, and any plain {@code SELECT ... ORDER BY ... LIMIT N}.
 * <p>
 * The optional trailing options map accepts: {@code fusion} ('RRF' | 'DBSF' | 'LINEAR', default
 * 'RRF'), {@code k} (RRF constant, default 60), {@code weights} (per-source list of floats, default
 * 1.0 for all), {@code groupBy} (payload field for post-fusion grouping), {@code groupSize}
 * (max rows per group, default 1), {@code limit} (max rows returned, default unlimited).
 * <p>
 * Score conventions: this function assumes higher = better. Sources whose native score is a
 * distance (e.g. {@code vector.neighbors} returning {@code distance}) should be wrapped in a
 * sub-SELECT that exposes a similarity instead, or use {@code RRF} which is rank-only and
 * indifferent to score direction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionVectorFuse extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.fuse";

  private static final java.util.Set<String> OPTIONS = java.util.Set.of(
      "fusion", "k", "weights", "groupBy", "groupSize", "limit");

  private static final long DEFAULT_K = 60L;

  private enum Strategy { RRF, DBSF, LINEAR }

  public SQLFunctionVectorFuse() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length == 0)
      throw new CommandSQLParsingException(getSyntax());

    // Detect the trailing options map.
    int sourceCount = params.length;
    Map<?, ?> rawOpts = null;
    if (params[params.length - 1] instanceof Map<?, ?> m) {
      rawOpts = m;
      sourceCount = params.length - 1;
    }

    if (sourceCount < 2)
      throw new CommandSQLParsingException(NAME + " requires at least 2 sources, got " + sourceCount);

    final FunctionOptions opts = new FunctionOptions(NAME, rawOpts, OPTIONS);
    final Strategy strategy = parseStrategy(opts.getString("fusion", "RRF"));
    final long k = opts.getLong("k", DEFAULT_K);
    final float[] weights = parseWeights(opts.getList("weights"), sourceCount);
    final String groupBy = opts.getString("groupBy", null);
    final int groupSize = opts.getInt("groupSize", 1);
    final int limit = opts.getInt("limit", -1);

    if (groupSize < 1)
      throw new CommandSQLParsingException(NAME + " groupSize must be >= 1, got " + groupSize);

    // Materialize each source as an ordered list of (rid, score). Score may be NaN when the source
    // does not provide one; that's fine for RRF but rejected later for DBSF/LINEAR.
    final List<List<RidScore>> sources = new ArrayList<>(sourceCount);
    for (int i = 0; i < sourceCount; i++)
      sources.add(materializeSource(NAME, params[i], i));

    final HashMap<RID, Float> fused = new HashMap<>();
    switch (strategy) {
    case RRF -> applyRRF(sources, weights, k, fused);
    case LINEAR -> applyLinear(sources, weights, fused);
    case DBSF -> applyDBSF(sources, weights, fused);
    }

    if (fused.isEmpty())
      return new ArrayList<>(0);

    final ArrayList<RidScore> ranked = new ArrayList<>(fused.size());
    for (final Map.Entry<RID, Float> e : fused.entrySet())
      ranked.add(new RidScore(e.getKey(), e.getValue()));
    ranked.sort((a, b) -> Float.compare(b.score, a.score));

    // Single pass: lookupByRID once per RID, applying groupBy and limit on the fly. Doing this
    // in two passes (one to filter by group, one to serialize) would double the record I/O for
    // large result sets.
    //
    // GroupAdmissionState is used with an unbounded group count (Integer.MAX_VALUE) because
    // `vector.fuse`'s `limit` caps total rows, not distinct groups; only the per-group cap
    // applies here. For `vector.neighbors` / `vector.sparseNeighbors`, the same helper takes a
    // finite limit so it doubles as the "we have enough groups" early-exit predicate.
    final GroupAdmissionState groups = groupBy != null
        ? new GroupAdmissionState(Integer.MAX_VALUE, groupSize)
        : null;
    final int targetSize = limit > 0 ? limit : Integer.MAX_VALUE;
    final ArrayList<Object> result = new ArrayList<>(Math.min(targetSize, ranked.size()));
    final BasicDatabase db = context.getDatabase();

    for (final RidScore rs : ranked) {
      if (result.size() >= targetSize)
        break;

      final Document record;
      try {
        record = (Document) db.lookupByRID(rs.rid, true);
      } catch (final RecordNotFoundException ex) {
        continue;
      }

      if (groupBy != null && !groups.admit(readNestedField(record, groupBy)))
        continue;

      final LinkedHashMap<String, Object> entry = new LinkedHashMap<>();
      entry.put("record", record);
      for (final String prop : record.getPropertyNames())
        entry.put(prop, record.get(prop));
      entry.put("@rid", record.getIdentity());
      entry.put("@type", record.getTypeName());
      entry.put("score", rs.score);
      result.add(entry);
    }
    return result;
  }

  private static Strategy parseStrategy(final String raw) {
    final String name = raw == null ? "RRF" : raw.toUpperCase();
    try {
      return Strategy.valueOf(name);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException(
          "Unknown fusion strategy '" + raw + "'. Allowed: RRF, DBSF, LINEAR");
    }
  }

  private static float[] parseWeights(final List<?> raw, final int count) {
    if (raw == null) {
      final float[] uniform = new float[count];
      java.util.Arrays.fill(uniform, 1.0f);
      return uniform;
    }
    if (raw.size() != count)
      throw new CommandSQLParsingException(
          NAME + " weights size " + raw.size() + " does not match source count " + count);
    final float[] out = new float[count];
    for (int i = 0; i < count; i++) {
      final Object o = raw.get(i);
      if (!(o instanceof Number n))
        throw new CommandSQLParsingException(NAME + " weights[" + i + "] must be a number, got: " + o);
      out[i] = n.floatValue();
    }
    return out;
  }

  /**
   * Reads an arbitrary source representation into a flat ordered list of {@link RidScore} rows
   * (each carrying the row's RID and its score, NaN if the source has no numeric score field).
   * Handles both the {@link Map}-shaped output of native vector SQL functions and the {@link Result}
   * shaped output of plain sub-SELECT queries.
   */
  @SuppressWarnings("unchecked")
  private static List<RidScore> materializeSource(final String functionName, final Object src, final int sourceIdx) {
    if (src == null)
      return java.util.Collections.emptyList();
    if (!(src instanceof Iterable<?> iter))
      throw new CommandSQLParsingException(
          functionName + " source[" + sourceIdx + "] must be a list of rows, got: " + src.getClass().getSimpleName());

    final ArrayList<RidScore> out = new ArrayList<>();
    for (final Object row : iter) {
      final RID rid = extractRid(row);
      if (rid == null)
        continue;
      final float score = extractScore(row);
      out.add(new RidScore(rid, score));
    }
    return out;
  }

  private static RID extractRid(final Object row) {
    if (row == null)
      return null;
    if (row instanceof Map<?, ?> m) {
      Object v = m.get("@rid");
      if (v == null) v = m.get("rid");
      if (v instanceof RID r) return r;
      if (v instanceof Identifiable id) return id.getIdentity();
      return null;
    }
    if (row instanceof Result r) {
      if (r.getIdentity().isPresent())
        return r.getIdentity().get();
      Object v = r.getProperty("@rid");
      if (v == null) v = r.getProperty("rid");
      if (v instanceof RID rid) return rid;
      if (v instanceof Identifiable id) return id.getIdentity();
      return null;
    }
    if (row instanceof Identifiable id)
      return id.getIdentity();
    return null;
  }

  private static float extractScore(final Object row) {
    // "score" and "$score" carry similarity semantics (higher = better) by convention. "distance"
    // is the legacy key used by `vector.neighbors`; lower = better, so we flip its sign so the rest
    // of the fusion code can assume a single direction.
    if (row instanceof Map<?, ?> m) {
      final Object score = m.get("score");
      if (score instanceof Number n) return n.floatValue();
      final Object dollar = m.get("$score");
      if (dollar instanceof Number n) return n.floatValue();
      final Object distance = m.get("distance");
      if (distance instanceof Number n) return -n.floatValue();
      return Float.NaN;
    }
    if (row instanceof Result r) {
      final Object score = r.getProperty("score");
      if (score instanceof Number n) return n.floatValue();
      final Object dollar = r.getProperty("$score");
      if (dollar instanceof Number n) return n.floatValue();
      final Object distance = r.getProperty("distance");
      if (distance instanceof Number n) return -n.floatValue();
      return Float.NaN;
    }
    return Float.NaN;
  }

  private static void applyRRF(final List<List<RidScore>> sources, final float[] weights, final long k,
      final HashMap<RID, Float> out) {
    for (int s = 0; s < sources.size(); s++) {
      final List<RidScore> rows = sources.get(s);
      final float w = weights[s];
      for (int rank = 0; rank < rows.size(); rank++) {
        // Position 0 = rank 1 in the RRF formula.
        final float contrib = (float) (w / (k + (rank + 1L)));
        out.merge(rows.get(rank).rid, contrib, Float::sum);
      }
    }
  }

  private static void applyLinear(final List<List<RidScore>> sources, final float[] weights,
      final HashMap<RID, Float> out) {
    for (int s = 0; s < sources.size(); s++) {
      final List<RidScore> rows = sources.get(s);
      requireScores(rows, "LINEAR", s);
      // Min-max normalize per source.
      float min = Float.POSITIVE_INFINITY;
      float max = Float.NEGATIVE_INFINITY;
      for (final RidScore r : rows) {
        if (r.score < min) min = r.score;
        if (r.score > max) max = r.score;
      }
      final float range = max - min;
      final float w = weights[s];
      for (final RidScore r : rows) {
        final float normalized = range == 0.0f ? 1.0f : (r.score - min) / range;
        out.merge(r.rid, w * normalized, Float::sum);
      }
    }
  }

  private static void applyDBSF(final List<List<RidScore>> sources, final float[] weights,
      final HashMap<RID, Float> out) {
    for (int s = 0; s < sources.size(); s++) {
      final List<RidScore> rows = sources.get(s);
      requireScores(rows, "DBSF", s);
      // mean and population stddev
      final int n = rows.size();
      double sum = 0.0;
      for (final RidScore r : rows) sum += r.score;
      final double mean = n == 0 ? 0.0 : sum / n;
      double sqSum = 0.0;
      for (final RidScore r : rows) {
        final double d = r.score - mean;
        sqSum += d * d;
      }
      final double std = n == 0 ? 0.0 : Math.sqrt(sqSum / n);
      // Qdrant DBSF: clip to [mean - 3σ, mean + 3σ], then linear-normalize that band to [0, 1].
      final double lo = mean - 3.0 * std;
      final double hi = mean + 3.0 * std;
      final double band = hi - lo;
      final float w = weights[s];
      for (final RidScore r : rows) {
        final double clipped = Math.max(lo, Math.min(hi, r.score));
        final float normalized = band == 0.0 ? 1.0f : (float) ((clipped - lo) / band);
        out.merge(r.rid, w * normalized, Float::sum);
      }
    }
  }

  private static void requireScores(final List<RidScore> rows, final String strategy, final int sourceIdx) {
    for (final RidScore r : rows)
      if (Float.isNaN(r.score))
        throw new CommandSQLParsingException(
            NAME + " strategy " + strategy + " requires a numeric score on every row of source[" + sourceIdx + "]");
  }

  @Override
  public String getSyntax() {
    return NAME + "(<source1>, <source2>, ..., <sourceN>[, { fusion: 'RRF'|'DBSF'|'LINEAR', "
        + "k: <long>, weights: [<float>, ...], groupBy: <field>, groupSize: <int>, limit: <int> }])";
  }

  /**
   * Carries one ranked record across the materialization, fusion, grouping, and result-building
   * stages. Both per-source ranked rows (the input to the fusion strategies) and the fused output
   * use the same shape, so a single record type avoids an unnecessary translation pass.
   */
  private static final class RidScore {
    final RID   rid;
    final float score;

    RidScore(final RID rid, final float score) {
      this.rid = rid;
      this.score = score;
    }
  }
}
