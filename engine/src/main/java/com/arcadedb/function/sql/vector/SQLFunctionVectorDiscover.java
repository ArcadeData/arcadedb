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
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Qdrant-style context-pair Discovery API. Given a list of {@code (positive, negative)} example
 * pairs, ranks candidates by how consistently each one is closer to the positive than to the
 * negative across every pair. Useful when there's no single query target but a notion of "more
 * like the green side of these comparisons, less like the red side" - e.g. exploratory search
 * steered by relative-preference signals.
 * <p>
 * Scoring (soft-margin variant, higher is better):
 * <pre>
 *   score(c) = Σ over (pos_i, neg_i) pairs of (cos(c, pos_i) - cos(c, neg_i))
 * </pre>
 * A candidate fully on the positive side of every pair gets a score in
 * {@code (0, num_pairs * 2.0]}; one fully on the negative side gets a score in
 * {@code [-num_pairs * 2.0, 0)}.
 * <p>
 * Candidate generation: ArcadeDB does not (yet) have a native HNSW exploratory walk that
 * optimises this metric directly, so the function picks a candidate pool by running
 * {@link SQLFunctionVectorNeighbors} against a seed vector built from the pair differences
 * (sum of {@code pos_i - neg_i}), then re-ranks the pool by the discovery score above.
 * Over-fetch factor 5x bridges the gap between the seed-vector candidates and the actually-best
 * discovery candidates - increase via the {@code overfetch} option for higher recall on
 * pathological pair distributions.
 * <p>
 * Usage:
 * <pre>
 *   SELECT expand(`vector.discover`('Doc[embedding]',
 *       [[#1:1, #1:9], [#1:2, #1:8]],   -- (positive, negative) pairs
 *       10[, options]))
 * </pre>
 * Options: {@code efSearch} (forwarded to the candidate-generation HNSW search),
 * {@code overfetch} (multiplier on {@code k} for the candidate pool, default 5).
 * Example RIDs are excluded from the result automatically.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionVectorDiscover extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.discover";

  private static final Set<String> OPTIONS = Set.of("efSearch", "overfetch");
  private static final int DEFAULT_OVERFETCH = 5;
  private static final int MAX_OVERFETCH_K = 100_000;

  public SQLFunctionVectorDiscover() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 3 || params.length > 4)
      throw new CommandSQLParsingException(getSyntax());

    final String indexSpec = params[0].toString();
    final List<Pair> pairs = parsePairs(params[1]);
    if (pairs.isEmpty())
      throw new CommandSQLParsingException(NAME + " requires at least one (positive, negative) pair");

    final int k = params[2] instanceof Number n ? n.intValue() : Integer.parseInt(params[2].toString());
    if (k <= 0)
      return new ArrayList<>(0);

    Map<?, ?> rawOpts = null;
    if (params.length == 4 && params[3] != null) {
      if (!(params[3] instanceof Map<?, ?> m))
        throw new CommandSQLParsingException(NAME + " 4th argument must be an options map");
      rawOpts = m;
    }
    final com.arcadedb.function.sql.FunctionOptions opts =
        new com.arcadedb.function.sql.FunctionOptions(NAME, rawOpts, OPTIONS);
    final int efSearch = opts.getInt("efSearch", -1);
    final int overfetchFactor = opts.getInt("overfetch", DEFAULT_OVERFETCH);
    if (overfetchFactor < 1)
      throw new CommandSQLParsingException(NAME + " overfetch must be >= 1, got " + overfetchFactor);
    final long candidatePoolL = (long) k * overfetchFactor;
    if (candidatePoolL > MAX_OVERFETCH_K)
      throw new CommandSQLParsingException(NAME + " overfetch budget exceeded: k=" + k
          + ", overfetch=" + overfetchFactor + " would request " + candidatePoolL
          + " candidates (cap " + MAX_OVERFETCH_K + ")");
      final int candidatePool = (int) candidatePoolL;

    final int bracketStart = indexSpec.indexOf('[');
    if (bracketStart < 0 || !indexSpec.endsWith("]"))
      throw new CommandSQLParsingException(NAME + " indexSpec must be in 'Type[property]' form, got: " + indexSpec);
    final String propertyName = indexSpec.substring(bracketStart + 1, indexSpec.length() - 1);

    final BasicDatabase db = context.getDatabase();

    // Pre-fetch every (positive, negative) pair's embeddings. Drop pairs where either side was
    // deleted between selection and this call - dropping a pair just reduces the discovery
    // signal, it does not corrupt the math. If every pair drops, return empty rather than
    // running a degenerate KNN with a zero seed vector.
    final Map<RID, float[]> embeddingCache = new HashMap<>();
    final List<float[][]> resolvedPairs = new ArrayList<>(pairs.size());
    int expectedDim = -1;
    for (final Pair p : pairs) {
      final float[] posVec = loadEmbedding(db, p.positive, propertyName, embeddingCache);
      if (posVec == null) continue;
      final float[] negVec = loadEmbedding(db, p.negative, propertyName, embeddingCache);
      if (negVec == null) continue;
      if (expectedDim == -1) expectedDim = posVec.length;
      if (posVec.length != expectedDim || negVec.length != expectedDim)
        throw new CommandSQLParsingException(NAME + " pair " + p
            + " has embedding dimension(s) " + posVec.length + "/" + negVec.length + ", expected " + expectedDim);
      resolvedPairs.add(new float[][] { posVec, negVec });
    }
    if (resolvedPairs.isEmpty())
      return new ArrayList<>(0);

    // Seed vector: sum of (pos_i - neg_i). Steers the HNSW candidate generation toward the
    // region of the embedding space where pos vectors dominate. Not the optimisation target of
    // discovery scoring (that's the per-pair-margin sum we apply post-fetch), but a reasonable
    // proxy for "where do positive examples cluster".
    final float[] seedVector = new float[expectedDim];
    for (final float[][] pair : resolvedPairs) {
      for (int i = 0; i < expectedDim; i++)
        seedVector[i] += pair[0][i] - pair[1][i];
    }

    final SQLFunctionVectorNeighbors neighbors = new SQLFunctionVectorNeighbors();
    final LinkedHashMap<String, Object> innerOpts = new LinkedHashMap<>();
    if (efSearch > 0) innerOpts.put("efSearch", efSearch);

    final Object rawResult = neighbors.execute(self, currentRecord, currentResult,
        new Object[] { indexSpec, seedVector, candidatePool, innerOpts },
        context);
    if (!(rawResult instanceof List<?> rawList))
      return rawResult;

    // Re-rank the candidate pool by per-pair margin sum. Skip example RIDs.
    final HashSet<RID> exampleRids = new HashSet<>(embeddingCache.keySet());
    final ArrayList<Scored> rescored = new ArrayList<>(rawList.size());
    for (final Object row : rawList) {
      if (!(row instanceof Map<?, ?> m))
        continue;
      final Object ridObj = m.get("@rid");
      final RID rid;
      if (ridObj instanceof RID r) rid = r;
      else if (ridObj instanceof Identifiable id) rid = id.getIdentity();
      else continue;
      if (exampleRids.contains(rid))
        continue;
      // The vector.neighbors output already includes the candidate's record; read the embedding
      // off the projected map directly to skip a redundant lookupByRID.
      final Object embeddingObj = m.get(propertyName);
      if (embeddingObj == null)
        continue;
      final float[] cVec;
      try {
        cVec = toFloatArray(embeddingObj);
      } catch (final RuntimeException ignored) {
        continue;
      }
      if (cVec.length != expectedDim)
        continue;
      float score = 0.0f;
      for (final float[][] pair : resolvedPairs)
        score += VectorUtils.cosineSimilarity(cVec, pair[0]) - VectorUtils.cosineSimilarity(cVec, pair[1]);
      rescored.add(new Scored(row, score));
    }
    rescored.sort((a, b) -> Float.compare(b.score, a.score));

    final ArrayList<Object> out = new ArrayList<>(Math.min(k, rescored.size()));
    for (int i = 0; i < rescored.size() && out.size() < k; i++) {
      // Override the upstream "distance"/"score" with the discovery score so a downstream
      // {@code vector.fuse} or `ORDER BY score` sees the right signal. Always rebuild into a
      // fresh map: mutating the caller-supplied map in place would let one round of discover
      // poison the next (e.g. inside a LET / subquery the same row dict gets reused with the
      // previous round's score still attached). Building a copy is cheap and the safer default.
      final Object row = rescored.get(i).row;
      if (row instanceof Map<?, ?> mm) {
        final java.util.LinkedHashMap<String, Object> rebuilt = new java.util.LinkedHashMap<>();
        for (final var e : mm.entrySet())
          rebuilt.put(String.valueOf(e.getKey()), e.getValue());
        rebuilt.put("score", rescored.get(i).score);
        out.add(rebuilt);
      } else {
        out.add(row);
      }
    }
    return out;
  }

  public String getSyntax() {
    return NAME + "(<indexSpec>, <pairs: list of [pos, neg]>, <k>[, options])";
  }

  private float[] loadEmbedding(final BasicDatabase db, final RID rid, final String propertyName,
      final Map<RID, float[]> cache) {
    final float[] cached = cache.get(rid);
    if (cached != null)
      return cached;
    try {
      final Document rec = (Document) db.lookupByRID(rid, true);
      final Object raw = rec.get(propertyName);
      if (raw == null)
        return null;
      final float[] v;
      try {
        v = toFloatArray(raw);
      } catch (final RuntimeException ignored) {
        return null;
      }
      cache.put(rid, v);
      return v;
    } catch (final RecordNotFoundException ignored) {
      return null;
    }
  }

  /** Parses the 2nd argument into a list of (positive, negative) pairs. */
  private List<Pair> parsePairs(final Object raw) {
    if (raw == null)
      throw new CommandSQLParsingException(NAME + " pairs argument is null");
    if (!(raw instanceof Iterable<?> outer))
      throw new CommandSQLParsingException(NAME + " pairs must be a list of 2-element lists, got: "
          + raw.getClass().getSimpleName());
    final ArrayList<Pair> out = new ArrayList<>();
    for (final Object element : outer) {
      out.add(parseOnePair(element));
    }
    return out;
  }

  private Pair parseOnePair(final Object raw) {
    final List<RID> rids = new ArrayList<>(2);
    if (raw instanceof Iterable<?> iter) {
      for (final Object o : iter)
        rids.add(asRid(o));
    } else if (raw != null && raw.getClass().isArray()) {
      final int n = java.lang.reflect.Array.getLength(raw);
      for (int i = 0; i < n; i++)
        rids.add(asRid(java.lang.reflect.Array.get(raw, i)));
    } else {
      throw new CommandSQLParsingException(NAME + " each pair must be a 2-element list, got: " + raw);
    }
    if (rids.size() != 2)
      throw new CommandSQLParsingException(NAME + " each pair must have exactly 2 entries (positive, negative), got "
          + rids.size());
    if (rids.get(0).equals(rids.get(1)))
      throw new CommandSQLParsingException(NAME + " positive and negative cannot be the same RID: " + rids.get(0));
    return new Pair(rids.get(0), rids.get(1));
  }

  private static RID asRid(final Object o) {
    if (o == null)
      throw new CommandSQLParsingException(NAME + " null RID inside pair");
    if (o instanceof RID r) return r;
    if (o instanceof Identifiable id) return id.getIdentity();
    if (o instanceof String s) {
      try { return new RID(s); }
      catch (final Exception e) { throw new CommandSQLParsingException(NAME + " bad RID string: " + s); }
    }
    throw new CommandSQLParsingException(NAME + " pair entry must be a RID or RID-string, got: " + o.getClass().getSimpleName());
  }

  private record Pair(RID positive, RID negative) {
    @Override
    public String toString() { return "(" + positive + ", " + negative + ")"; }
  }

  private static final class Scored {
    final Object row;
    final float  score;
    Scored(final Object row, final float score) { this.row = row; this.score = score; }
  }
}
