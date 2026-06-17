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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reciprocal Rank Fusion (RRF) scoring function for combining multiple ranking lists.
 * Computes: RRF = Σ (1 / (k + rank_i)) for each ranking provided.
 *
 * Usage:
 * - vectorRRFScore(rank1, rank2, rank3, ..., [{ k: <long> }])   (variadic ranks)
 * - vectorRRFScore([rank1, rank2, ...], [{ k: <long> }])         (ranks grouped in an array)
 *
 * k is the constant (default 60, set only via the trailing options map), and ranks are integer
 * positions. Every positional numeric argument is always treated as a rank.
 *
 * Example: vectorRRFScore(1, 5, 10) = 1/61 + 1/65 + 1/70 ≈ 0.0456
 *          vectorRRFScore([1, 5, 10], { k: 100 }) = 1/101 + 1/105 + 1/110
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorRRFScore extends SQLFunctionVectorAbstract {
  public static final String NAME      = "vector.rrfScore";
  private static final long  DEFAULT_K = 60;

  private static final Set<String> OPTIONS = Set.of("k");

  public SQLFunctionVectorRRFScore() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 1)
      throw new CommandSQLParsingException(getSyntax());

    if (isArrayLike(params[0])) {
      // Array form: vectorRRFScore([r1, r2, ...] [, { k: <long> }]) - ranks grouped in a single array/list,
      // consistent with vector.multiScore's array input.
      long k = DEFAULT_K;
      if (params.length == 2) {
        if (params[1] instanceof Map<?, ?> rawMap)
          k = new FunctionOptions(NAME, rawMap, OPTIONS).getLong("k", DEFAULT_K);
        else
          throw new CommandSQLParsingException("Second argument of the array form must be an options map { k: <long> }");
      } else if (params.length > 2) {
        throw new CommandSQLParsingException(getSyntax());
      }
      return (float) rrfFromArray(params[0], k);
    }

    // Variadic form: ranks as positional args, optional trailing { k } map.
    // k is configured ONLY via a trailing options map { k: <long> }. A bare trailing number is always a
    // rank, never k: ranks of 60+ are legitimate, so the previous "last number >= 60 is k" heuristic
    // silently dropped a real rank and produced wrong results for >2 ranking lists (issue #3099).
    long k = DEFAULT_K;
    int rankCount = params.length;
    final Object lastParam = params[params.length - 1];
    if (lastParam instanceof Map<?, ?> rawMap) {
      k = new FunctionOptions(NAME, rawMap, OPTIONS).getLong("k", DEFAULT_K);
      rankCount = params.length - 1;
    }

    // A null rank is skipped (the item is absent from that ranking list); every present rank must be a
    // positive integer (same handling as the array form).
    double rrfScore = 0.0;
    for (int i = 0; i < rankCount; i++) {
      final Object rankObj = params[i];
      if (rankObj == null)
        continue;
      rrfScore += rankTerm(toDouble(rankObj), k);
    }
    return (float) rrfScore;
  }

  // Ranks are integer positions, so only integer-typed arrays/collections are accepted here. Object[] and
  // List are the shapes the query engine produces for a literal ranking list (e.g. SELECT vectorRRFScore([
  // r1, r2, ...])); this also covers boxed arrays like Integer[]/Long[] (an Integer[] is an Object[]). Both
  // stay array-like even though their elements are validated as integers per item.
  // A float[]/double[] (typically a score or embedding vector passed by mistake) is deliberately NOT
  // array-like: it falls through to the variadic path and is rejected as a non-number rank, a clearer error
  // than treating each element as a rank and complaining it is not an integer. Other integer-typed primitive
  // arrays (short[], byte[]) are too rare as rank inputs to special-case; they take the same variadic
  // non-number rejection.
  private static boolean isArrayLike(final Object value) {
    return value instanceof int[] || value instanceof long[] || value instanceof Object[] || value instanceof List;
  }

  /** Sums the RRF terms over an array-like rank source (caller guarantees {@link #isArrayLike}). */
  private static double rrfFromArray(final Object arrayLike, final long k) {
    double score = 0.0;
    switch (arrayLike) {
    // Primitive arrays are iterated directly (no boxing, per the engine's GC-awareness policy).
    case int[] a -> { for (final int r : a) score += rankTerm(r, k); }
    case long[] a -> { for (final long r : a) score += rankTerm(r, k); }
    case Object[] a -> { for (final Object o : a) if (o != null) score += rankTerm(toDouble(o), k); }
    case List<?> l -> { for (final Object o : l) if (o != null) score += rankTerm(toDouble(o), k); }
    default -> throw new AssertionError("rrfFromArray reached with non-array-like: " + arrayLike.getClass().getSimpleName());
    }
    return score;
  }

  private static double toDouble(final Object rankObj) {
    if (rankObj instanceof Number num)
      return num.doubleValue();
    throw new CommandSQLParsingException("Rank values must be numbers, found: " + rankObj.getClass().getSimpleName());
  }

  /** Returns the RRF term {@code 1/(k+rank)}, validating that {@code rank} is a positive integer. */
  private static double rankTerm(final double rank, final long k) {
    if (Double.isNaN(rank) || Double.isInfinite(rank))
      throw new CommandSQLParsingException("Rank values must be finite numbers, found: " + rank);
    if (rank <= 0)
      throw new CommandSQLParsingException("Rank values must be positive integers, found: " + rank);
    // Math.rint (not a (long) cast) so the integer test stays correct for magnitudes beyond long range:
    // a value equal to its nearest integer is integer-valued; 1.5 != rint(1.5)==2.0 correctly rejects.
    if (rank != Math.rint(rank))
      throw new CommandSQLParsingException("Rank values must be integers, found: " + rank);
    return 1.0 / (k + rank);
  }

  public String getSyntax() {
    return NAME + "(<rank1>, <rank2>, ... [, { k: <long> }]) | " + NAME + "([<ranks>] [, { k: <long> }])";
  }
}
