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
import com.arcadedb.index.sparsevector.RidScore;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Maximal Marginal Relevance (MMR) reranker. Diversifies a scored candidate set by greedily
 * picking the candidate that maximises {@code lambda * score - (1 - lambda) * max(cos(c, s))}
 * for every {@code s} already selected, where {@code score} is the candidate's relevance to
 * the query (carried in from the upstream {@code vector.neighbors} / {@code vector.fuse} /
 * {@code SELECT ... ORDER BY score} source) and {@code cos(c, s)} is the cosine similarity
 * between the candidate's and the selected one's embedding vectors. Closes a P1 gap from the
 * Qdrant/Milvus comparison: RAG systems typically want a diverse top-K, not near-duplicates.
 * <p>
 * Usage: {@code vector.mmr(source, embeddingProperty[, options])}
 * <ul>
 *   <li><b>source</b>: any iterable of rows with at least an {@code @rid} and a numeric
 *       {@code score}. Same shape contract as {@link SQLFunctionVectorFuse}.</li>
 *   <li><b>embeddingProperty</b>: name of the vector property to read from each candidate's
 *       record (e.g. {@code 'embedding'}). Must contain a {@code float[]} or any {@code List}
 *       of numbers; all candidates' vectors must share the same dimension.</li>
 *   <li><b>options.lambda</b> (default {@code 0.5}): tradeoff between relevance and diversity.
 *       {@code 1.0} = relevance only (returns top-K by score), {@code 0.0} = diversity only
 *       (after the seed pick).</li>
 *   <li><b>options.k</b> (default = candidate count): max rows to return.</li>
 * </ul>
 * <p>
 * Score conventions: the function reads {@code score} or {@code $score} (similarity-shaped) and
 * auto-flips a {@code distance} field (lower-better, native to {@code vector.neighbors}) so
 * upstream sources compose with one direction.
 * <p>
 * <b>Heap pressure.</b> All N candidate embeddings are loaded into JVM heap before the greedy
 * loop starts (cost: O(N * dim) memory). This is the standard MMR tradeoff but worth bounding
 * the upstream source: a {@code vector.neighbors} call that fetches 100k candidates with
 * 1024-dim float embeddings would use ~400 MB. Always cap the upstream candidate pool with a
 * reasonable {@code k}; the diversity benefit plateaus well before tens of thousands of
 * candidates.
 * <p>
 * <b>Negative-cosine clipping.</b> The diversity penalty clips to {@code 0} for any
 * {@code maxCosToSelected} that is negative ({@code Math.max(0.0f, ...)}). Two candidates
 * pointing in <i>opposite</i> directions therefore do not contribute a diversity bonus; they
 * just have no penalty. This is a deliberate simplification - Qdrant's MMR does not clip and
 * lets opposite candidates "earn" extra negative penalty (i.e. be preferred over orthogonal
 * ones). For most embedding workloads (where cosine similarity is roughly in {@code [0, 1]}
 * because embeddings sit in the positive half-sphere) the difference is invisible. If you need
 * Qdrant-identical scores for opposite-direction candidates, this is the documented gap.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionVectorMmr extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.mmr";

  private static final Set<String> OPTIONS = Set.of("lambda", "k");

  private static final float DEFAULT_LAMBDA = 0.5f;

  /**
   * Threshold for the WARNING-level log when the in-memory candidate pool gets large. Ten
   * million floats is roughly 40 MB; below this, the heap impact is small enough that the
   * Javadoc warning suffices and a runtime log would just be noise. Above it, operators want
   * to see the line in their server log rather than diagnose unexplained GC pressure later.
   */
  private static final long MMR_HEAP_WARN_FLOATS = 10_000_000L;

  public SQLFunctionVectorMmr() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object source = params[0];
    if (!(params[1] instanceof String embeddingProperty) || embeddingProperty.isEmpty())
      throw new CommandSQLParsingException(NAME + " 2nd argument must be the embedding property name (string)");

    Map<?, ?> rawOpts = null;
    if (params.length > 2) {
      if (!(params[2] instanceof Map<?, ?> m))
        throw new CommandSQLParsingException(NAME + " 3rd argument must be an options map");
      rawOpts = m;
    }
    final FunctionOptions opts = new FunctionOptions(NAME, rawOpts, OPTIONS);
    final float lambda = (float) opts.getDouble("lambda", DEFAULT_LAMBDA);
    if (Float.isNaN(lambda) || lambda < 0.0f || lambda > 1.0f)
      throw new CommandSQLParsingException(NAME + " lambda must be in [0, 1], got " + lambda);

    final List<RidScore> rawCandidates = materializeCandidates(source);
    if (rawCandidates.isEmpty())
      return new ArrayList<>(0);

    final int k = opts.getInt("k", rawCandidates.size());
    if (k <= 0)
      return new ArrayList<>(0);

    final BasicDatabase db = context.getDatabase();

    // Pre-fetch each candidate's embedding once and build a fully-populated Candidate. Skip
    // candidates whose record was deleted or whose embedding property is missing/wrong-shape;
    // they are silently dropped (the caller already paid to score them upstream, so a missing
    // embedding here is a data-quality issue rather than a query error). Dimension mismatch IS
    // an error - the cosine math would silently produce garbage.
    final ArrayList<Candidate> usable = new ArrayList<>(rawCandidates.size());
    int expectedDim = -1;
    for (final RidScore rs : rawCandidates) {
      try {
        final Document rec = (Document) db.lookupByRID(rs.rid(), true);
        final Object raw = rec.get(embeddingProperty);
        if (raw == null)
          continue;
        final float[] vector = toFloatArray(raw);
        if (expectedDim == -1) {
          expectedDim = vector.length;
        } else if (vector.length != expectedDim) {
          throw new CommandSQLParsingException(
              NAME + " embedding dimension mismatch on " + rs.rid() + ": " + vector.length
                  + " vs " + expectedDim);
        }
        usable.add(new Candidate(rs.score(), vector, rec));
      } catch (final RecordNotFoundException ignored) {
        // candidate's record was deleted between the upstream scoring and now; drop it
      }
    }
    if (usable.isEmpty())
      return new ArrayList<>(0);

    // Surface oversized candidate pools at query time. The Javadoc warns about the O(N * dim)
    // heap cost; this WARNING fires when the actual cost crosses ~40 MB (10M float estimate),
    // turning an unbounded mmr() call into an observable signal in the server log instead of an
    // unexplained heap pressure event later. Heuristic-only: integer overflow in the multiply is
    // possible at extreme values but the threshold check would short-circuit well before that.
    if ((long) usable.size() * expectedDim > MMR_HEAP_WARN_FLOATS)
      com.arcadedb.log.LogManager.instance().log(this, java.util.logging.Level.WARNING,
          NAME + " loaded %d candidates x %d-dim embeddings (~%d MB) - bound the upstream candidate "
              + "pool with a smaller k to reduce heap pressure",
          usable.size(), expectedDim, (long) usable.size() * expectedDim * 4L / (1024L * 1024L));

    // Greedy MMR. We track each remaining candidate's max cosine to the selected set; on each
    // round we update only the just-added selectee's contribution to those running maxes,
    // turning the inner loop into O(|usable|) per round instead of O(|usable| * |selected|).
    // Total cost: O(k * |usable|) similarity computations, plus the pre-fetch above. Memory:
    // O(|usable|) for the running-max array.
    final int targetK = Math.min(k, usable.size());
    final boolean[] picked = new boolean[usable.size()];
    final float[] maxCosToSelected = new float[usable.size()];
    Arrays.fill(maxCosToSelected, Float.NEGATIVE_INFINITY);
    final List<Candidate> selected = new ArrayList<>(targetK);

    for (int step = 0; step < targetK; step++) {
      int bestIdx = -1;
      float bestObjective = Float.NEGATIVE_INFINITY;
      for (int i = 0; i < usable.size(); i++) {
        if (picked[i])
          continue;
        // First pick: no diversity penalty (no selected set), so the objective collapses to
        // {@code lambda * score} - i.e. the top-scored candidate wins, which matches the MMR
        // formal definition where the running-max is undefined / -inf.
        final float diversityPenalty = (step == 0)
            ? 0.0f
            : Math.max(0.0f, maxCosToSelected[i]);  // negative cos rounded up (no anti-bonus)
        final float objective = lambda * usable.get(i).score() - (1.0f - lambda) * diversityPenalty;
        if (objective > bestObjective) {
          bestObjective = objective;
          bestIdx = i;
        }
      }
      if (bestIdx < 0)
        break;
      picked[bestIdx] = true;
      final Candidate winner = usable.get(bestIdx);
      selected.add(winner);
      // Update running max-cos for everyone still in the pool.
      for (int i = 0; i < usable.size(); i++) {
        if (picked[i])
          continue;
        final float cos = VectorUtils.cosineSimilarity(usable.get(i).vector(), winner.vector());
        if (cos > maxCosToSelected[i])
          maxCosToSelected[i] = cos;
      }
    }

    final ArrayList<Object> result = new ArrayList<>(selected.size());
    for (final Candidate c : selected) {
      final LinkedHashMap<String, Object> entry = new LinkedHashMap<>();
      entry.put("record", c.record());
      for (final String prop : c.record().getPropertyNames())
        entry.put(prop, c.record().get(prop));
      entry.put("@rid", c.record().getIdentity());
      entry.put("@type", c.record().getTypeName());
      entry.put("score", c.score());
      result.add(entry);
    }
    return result;
  }

  public String getSyntax() {
    return NAME + "(<source>, <embeddingProperty>[, {lambda: 0.5, k: <int>}])";
  }

  /**
   * Fully-hydrated candidate: an immutable record carrying the score, the embedding vector, and
   * the underlying document. Built in one shot inside the execute method's lookup loop, so the
   * MMR loop never sees a partial-state Candidate.
   */
  private record Candidate(float score, float[] vector, Document record) {}

  private static List<RidScore> materializeCandidates(final Object src) {
    if (src == null)
      return Collections.emptyList();
    if (!(src instanceof Iterable<?> iter))
      throw new CommandSQLParsingException(NAME + " 1st argument must be an iterable of scored rows, got "
          + src.getClass().getSimpleName());
    final ArrayList<RidScore> out = new ArrayList<>();
    for (final Object row : iter) {
      final RID rid = extractRidFromRow(row);
      if (rid == null)
        continue;
      // Use the shared score extractor: it auto-flips a {@code distance} field to similarity, so
      // {@code vector.mmr} accepts the output of {@code vector.neighbors} (which emits distance,
      // not score) directly. A bare-{@code score}-only extractor here would silently drop every
      // row piped from {@code vector.neighbors} - exactly the silent-data-loss class of bug.
      final float score = extractScoreFromRow(row);
      if (Float.isNaN(score))
        throw new CommandSQLParsingException(
            NAME + " requires every source row to carry a numeric 'score' or 'distance' field; missing on row " + rid);
      out.add(new RidScore(rid, score));
    }
    return out;
  }
}
