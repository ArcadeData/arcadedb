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

    final Object[] rankElements;
    long k = DEFAULT_K;

    if (isArrayLike(params[0])) {
      // Array form: vectorRRFScore([r1, r2, ...] [, { k: <long> }]) - ranks grouped in a single array/list,
      // consistent with vector.multiScore's array input.
      if (params.length == 2) {
        if (params[1] instanceof Map<?, ?> rawMap)
          k = new FunctionOptions(NAME, rawMap, OPTIONS).getLong("k", DEFAULT_K);
        else
          throw new CommandSQLParsingException("Second argument of the array form must be an options map { k: <long> }");
      } else if (params.length > 2) {
        throw new CommandSQLParsingException(getSyntax());
      }
      rankElements = toObjectArray(params[0]);
    } else {
      // Variadic form: ranks as positional args, optional trailing { k } map.
      // k is configured ONLY via a trailing options map { k: <long> }. A bare trailing number is always a
      // rank, never k: ranks of 60+ are legitimate, so the previous "last number >= 60 is k" heuristic
      // silently dropped a real rank and produced wrong results for >2 ranking lists (issue #3099).
      int rankCount = params.length;
      final Object lastParam = params[params.length - 1];
      if (lastParam instanceof Map<?, ?> rawMap) {
        k = new FunctionOptions(NAME, rawMap, OPTIONS).getLong("k", DEFAULT_K);
        rankCount = params.length - 1;
      }
      rankElements = new Object[rankCount];
      System.arraycopy(params, 0, rankElements, 0, rankCount);
    }

    // Both forms share the same per-rank handling: a null rank is skipped (the item is absent from that
    // ranking list), and every present rank must be a positive integer.
    double rrfScore = 0.0;
    for (final Object rankObj : rankElements) {
      if (rankObj == null)
        continue;
      rrfScore += 1.0 / (k + toRank(rankObj));
    }

    return (float) rrfScore;
  }

  private static boolean isArrayLike(final Object value) {
    return value instanceof float[] || value instanceof double[] || value instanceof int[] || value instanceof long[]
        || value instanceof Object[] || value instanceof List;
  }

  /** Boxes any array-like (primitive array, Object[] or List) into an Object[] so null elements survive. */
  private static Object[] toObjectArray(final Object arrayLike) {
    if (arrayLike instanceof Object[] o)
      return o;
    if (arrayLike instanceof List<?> l)
      return l.toArray();
    if (arrayLike instanceof int[] a) {
      final Object[] r = new Object[a.length];
      for (int i = 0; i < a.length; i++)
        r[i] = a[i];
      return r;
    }
    if (arrayLike instanceof long[] a) {
      final Object[] r = new Object[a.length];
      for (int i = 0; i < a.length; i++)
        r[i] = a[i];
      return r;
    }
    if (arrayLike instanceof float[] a) {
      final Object[] r = new Object[a.length];
      for (int i = 0; i < a.length; i++)
        r[i] = a[i];
      return r;
    }
    final double[] a = (double[]) arrayLike;
    final Object[] r = new Object[a.length];
    for (int i = 0; i < a.length; i++)
      r[i] = a[i];
    return r;
  }

  /** Validates a single rank value: must be a positive integer. */
  private static long toRank(final Object rankObj) {
    if (!(rankObj instanceof Number num))
      throw new CommandSQLParsingException("Rank values must be numbers, found: " + rankObj.getClass().getSimpleName());
    final double d = num.doubleValue();
    if (d <= 0)
      throw new CommandSQLParsingException("Rank values must be positive integers, found: " + num);
    if (Double.isInfinite(d) || d != Math.rint(d))
      throw new CommandSQLParsingException("Rank values must be integers, found: " + num);
    return num.longValue();
  }

  public String getSyntax() {
    return NAME + "(<rank1>, <rank2>, ... | [<ranks>], [{ k: <long> }])";
  }
}
