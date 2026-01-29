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
package com.arcadedb.query.opencypher.functions.text;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * text.levenshteinDistance(str1, str2) - Calculate edit distance between strings.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextLevenshteinDistance extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "levenshteinDistance";
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
    return "Calculate the Levenshtein edit distance between two strings";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str1 = asString(args[0]);
    final String str2 = asString(args[1]);

    if (str1 == null || str2 == null)
      return null;

    return (long) levenshteinDistance(str1, str2);
  }

  /**
   * Calculate Levenshtein distance using dynamic programming.
   */
  public static int levenshteinDistance(final String s1, final String s2) {
    final int len1 = s1.length();
    final int len2 = s2.length();

    // Use space-optimized approach with two rows
    int[] prev = new int[len2 + 1];
    int[] curr = new int[len2 + 1];

    // Initialize first row
    for (int j = 0; j <= len2; j++) {
      prev[j] = j;
    }

    // Fill the matrix
    for (int i = 1; i <= len1; i++) {
      curr[0] = i;
      for (int j = 1; j <= len2; j++) {
        final int cost = s1.charAt(i - 1) == s2.charAt(j - 1) ? 0 : 1;
        curr[j] = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
      }
      // Swap rows
      final int[] tmp = prev;
      prev = curr;
      curr = tmp;
    }

    return prev[len2];
  }
}
