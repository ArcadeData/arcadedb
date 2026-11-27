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

/**
 * text.levenshteinDistance(str1, str2) - Calculate edit distance between strings.
 *
 * <p><b>Performance:</b> O(n*m) time complexity and O(m) space complexity,
 * where n and m are the lengths of the input strings. For large strings,
 * this operation can be expensive.</p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextLevenshteinDistance extends AbstractTextFunction {

  /** Longest either argument may be; beyond it the O(n*m) computation is refused rather than run. */
  public static final int MAX_STRING_LENGTH = 10000;

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
    return "Calculate the Levenshtein edit distance between two strings (O(n*m) complexity)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str1 = asString(args[0]);
    final String str2 = asString(args[1]);

    if (str1 == null || str2 == null)
      return null;

    requireWithinMaxLength("Levenshtein distance", str1, str2);

    return (long) levenshteinDistance(str1, str2);
  }

  /**
   * Validates the length, in characters, of the two strings a function compares character by character in O(n*m) time, so that the
   * work stays bounded. Shared by every such function ({@code levenshteinDistance}, {@code levenshteinSimilarity},
   * {@code jaroWinklerDistance}). The count comes before the strings are copied and allocates nothing, so a huge string is refused
   * without being copied.
   *
   * @param comparison what the function computes, for the message
   *
   * @throws IllegalArgumentException when either string is longer than {@link #MAX_STRING_LENGTH} characters
   */
  static void requireWithinMaxLength(final String comparison, final String str1, final String str2) {
    requireWithinMaxLength(comparison, str1);
    requireWithinMaxLength(comparison, str2);
  }

  private static void requireWithinMaxLength(final String comparison, final String str) {
    final int units = str.length();
    // a character takes one or two UTF-16 units: a string with no more units than the limit has no more characters either
    if (units <= MAX_STRING_LENGTH)
      return;

    final int length = str.codePointCount(0, units);
    if (length > MAX_STRING_LENGTH)
      throw new IllegalArgumentException("String length exceeds maximum allowed for " + comparison + " (" + MAX_STRING_LENGTH + "): " + length);
  }

  /**
   * Calculate Levenshtein distance, over the characters of the strings, using dynamic programming.
   * Time complexity: O(n*m), Space complexity: O(m) where n,m are string lengths.
   */
  public static int levenshteinDistance(final String s1, final String s2) {
    return levenshteinDistance(codePointsOf(s1), codePointsOf(s2));
  }

  /**
   * Calculate Levenshtein distance between two strings given as their characters (see {@link #codePointsOf}).
   * Time complexity: O(n*m), Space complexity: O(m) where n,m are string lengths.
   */
  public static int levenshteinDistance(final int[] s1, final int[] s2) {
    final int len1 = s1.length;
    final int len2 = s2.length;

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
        final int cost = s1[i - 1] == s2[j - 1] ? 0 : 1;
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
