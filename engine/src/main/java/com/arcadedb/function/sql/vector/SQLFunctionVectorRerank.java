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
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Declarative multi-stage retrieval (Qdrant-style "prefetch + rerank"). Takes a stage-1
 * candidate set - typically the output of a cheap-but-coarse search like
 * {@code vector.neighbors} on a quantized-vector index - and re-scores each candidate against a
 * {@code query} vector using cosine similarity over a freshly-read property
 * ({@code embeddingProperty}). The resulting top-{@code k} respects the new ranking.
 * <p>
 * The point: stage 1 is allowed to use fast lossy primitives (PQ/binary/SQ/sparse with high
 * over-fetch) because the rerank stage compares against full-precision vectors and corrects
 * the ranking. Server-side and declarative, so the application no longer round-trips between
 * stages.
 * <p>
 * Usage:
 * <pre>
 *   SELECT expand(`vector.rerank`(
 *       `vector.neighbors`('Doc[binary_embedding]', :q_binary, 1000),
 *       :q_full,           -- full-precision query vector
 *       'embedding',       -- full-precision property to read off each record
 *       100))
 * </pre>
 * A candidate row that already carries its loaded record (a {@code record} key, a {@code Result} wrapping the
 * element, or the record itself, as the rows of {@code vector.fuse} and {@code vector.neighbors} do) is scored on
 * that record; only a row carrying just a RID is loaded here. Such a RID whose record disappeared between stage 1
 * and rerank (deleted, schema removed) is silently dropped, so a transient inconsistency cannot crash the query.
 * <p>
 * Only the best {@code k} candidates are kept while scanning, and output rows are built for those alone, so the
 * cost beyond reading each candidate's embedding does not grow with the candidate count (issue #10129).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionVectorRerank extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.rerank";

  /**
   * Ascending order of preference: lower score first and, among equal scores, the later candidate first.
   */
  private static final Comparator<Scored> EVICTION_ORDER = (a, b) -> {
    final int byScore = Float.compare(a.score(), b.score());
    return byScore != 0 ? byScore : Integer.compare(b.sequence(), a.sequence());
  };
  private static final Comparator<Scored> RANK_ORDER     = EVICTION_ORDER.reversed();

  public SQLFunctionVectorRerank() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 4)
      throw new CommandSQLParsingException(getSyntax());

    final Object source = params[0];
    final Object queryRaw = params[1];
    if (queryRaw == null)
      throw new CommandSQLParsingException(NAME + " query vector cannot be null");
    final float[] queryVector = toFloatArray(queryRaw);

    if (!(params[2] instanceof String embeddingProperty) || embeddingProperty.isEmpty())
      throw new CommandSQLParsingException(NAME + " 3rd argument must be the embedding property name");

    final int k = params[3] instanceof Number n ? n.intValue() : Integer.parseInt(params[3].toString());
    if (k <= 0)
      return new ArrayList<>(0);

    if (!(source instanceof Iterable<?> iter))
      throw new CommandSQLParsingException(NAME + " source must be an iterable of rows, got: "
          + (source == null ? "null" : source.getClass().getSimpleName()));

    final BasicDatabase db = context.getDatabase();
    // Only the best k candidates are kept while scanning: a min-heap whose root is the candidate to evict next (lowest
    // score, and among equal scores the one seen last), so the survivors and their order are those of a stable sort of
    // every candidate by descending score, without sorting them all (issue #10129).
    final PriorityQueue<Scored> best = new PriorityQueue<>(Math.min(k, 1024) + 1, EVICTION_ORDER);
    final int expectedDim = queryVector.length;
    int sequence = 0;
    for (final Object row : iter) {
      final RID rid = extractRidFromRow(row);
      if (rid == null)
        continue;
      // A row that already carries its loaded record (the output of `vector.fuse`, `vector.neighbors`, a plain
      // SELECT) is scored on that record: loading it again is the second load issue #10129 is about. A RID of an
      // internal record (an edge segment, ...) has no properties to score: skipped like a missing one
      final Document rec = candidateRecord(db, row, rid);
      if (rec == null)
        continue;
      final Object raw = rec.get(embeddingProperty);
      if (raw == null)
        continue;
      final float[] candVec;
      try {
        candVec = toFloatArray(raw);
      } catch (final RuntimeException ignored) {
        continue;
      }
      if (candVec.length != expectedDim)
        throw new CommandSQLParsingException(NAME + " candidate " + rid
            + " has embedding dimension " + candVec.length + ", expected " + expectedDim
            + " (matching the query vector)");

      final Scored candidate = new Scored(rec, VectorUtils.cosineSimilarity(queryVector, candVec), sequence++);
      if (best.size() < k)
        best.add(candidate);
      else if (EVICTION_ORDER.compare(candidate, best.peek()) > 0) {
        best.poll();
        best.add(candidate);
      }
    }

    final Scored[] ranked = best.toArray(new Scored[0]);
    Arrays.sort(ranked, RANK_ORDER);
    final ArrayList<Object> out = new ArrayList<>(ranked.length);
    for (final Scored s : ranked)
      out.add(new ScoredRecordRow(s.record(), "score", s.score()));
    return out;
  }

  public String getSyntax() {
    return NAME + "(<source>, <queryVector>, <embeddingProperty>, <k>)";
  }

  private record Scored(Document record, float score, int sequence) {}
}
