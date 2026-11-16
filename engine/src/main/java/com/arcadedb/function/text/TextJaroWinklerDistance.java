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
package com.arcadedb.function.text;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Arrays;

/**
 * text.jaroWinklerDistance(str1, str2) - The Jaro-Winkler distance, 1 minus the Jaro-Winkler similarity, as
 * {@code apoc.text.jaroWinklerDistance} answers (Apache Commons Text {@code JaroWinklerDistance}): 0.0 for equal strings, 1.0 for
 * strings with nothing in common (issue #12104).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextJaroWinklerDistance extends AbstractTextFunction {
  private static final double WINKLER_PREFIX_WEIGHT = 0.1;
  private static final int    WINKLER_MAX_PREFIX    = 4;
  private static final double WINKLER_THRESHOLD     = 0.7;

  @Override
  protected String getSimpleName() {
    return "jaroWinklerDistance";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Calculate the Jaro-Winkler distance (0-1, 0 for equal strings) between two strings";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str1 = asString(args[0]);
    final String str2 = asString(args[1]);

    if (str1 == null || str2 == null)
      return null;

    // characters, not UTF-16 units: a character outside the BMP is one position of the window, one match, one prefix character
    return 1 - similarity(codePointsOf(str1), codePointsOf(str2));
  }

  /**
   * The Jaro-Winkler similarity of Apache Commons Text {@code JaroWinklerSimilarity}: the shorter string is matched against the
   * longer, and the prefix bonus only applies from a Jaro similarity of 0.7.
   */
  private static double similarity(final int[] first, final int[] second) {
    if (Arrays.equals(first, second))
      return 1.0;

    final int[] max = first.length > second.length ? first : second;
    final int[] min = max == first ? second : first;

    final int range = Math.max(max.length / 2 - 1, 0);
    final int[] matchIndexes = new int[min.length];
    Arrays.fill(matchIndexes, -1);
    final boolean[] matchFlags = new boolean[max.length];
    int matches = 0;
    for (int mi = 0; mi < min.length; mi++) {
      for (int xi = Math.max(mi - range, 0), xn = Math.min(mi + range + 1, max.length); xi < xn; xi++) {
        if (!matchFlags[xi] && min[mi] == max[xi]) {
          matchIndexes[mi] = xi;
          matchFlags[xi] = true;
          matches++;
          break;
        }
      }
    }
    if (matches == 0)
      return 0.0;

    // the matched characters of each string in order: a position where the two differ is half a transposition
    final int[] matched1 = new int[matches];
    final int[] matched2 = new int[matches];
    for (int i = 0, si = 0; i < min.length; i++)
      if (matchIndexes[i] != -1)
        matched1[si++] = min[i];
    for (int i = 0, si = 0; i < max.length; i++)
      if (matchFlags[i])
        matched2[si++] = max[i];
    int halfTranspositions = 0;
    for (int i = 0; i < matches; i++)
      if (matched1[i] != matched2[i])
        halfTranspositions++;

    int prefix = 0;
    for (int i = 0; i < Math.min(WINKLER_MAX_PREFIX, min.length) && first[i] == second[i]; i++)
      prefix++;

    final double m = matches;
    final double jaro = (m / first.length + m / second.length + (m - (double) halfTranspositions / 2) / m) / 3;
    return jaro < WINKLER_THRESHOLD ? jaro : jaro + WINKLER_PREFIX_WEIGHT * prefix * (1.0 - jaro);
  }
}
