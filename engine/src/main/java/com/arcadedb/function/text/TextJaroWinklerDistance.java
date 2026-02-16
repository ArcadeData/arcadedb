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
 * text.jaroWinklerDistance(str1, str2) - Calculate Jaro-Winkler similarity.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextJaroWinklerDistance extends AbstractTextFunction {
  private static final double WINKLER_PREFIX_WEIGHT = 0.1;
  private static final int WINKLER_MAX_PREFIX = 4;

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
    return "Calculate the Jaro-Winkler similarity (0-1) between two strings";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str1 = asString(args[0]);
    final String str2 = asString(args[1]);

    if (str1 == null || str2 == null)
      return null;

    if (str1.equals(str2))
      return 1.0;

    if (str1.isEmpty() || str2.isEmpty())
      return 0.0;

    // Calculate Jaro similarity
    final double jaroSim = jaroSimilarity(str1, str2);

    // Calculate common prefix length (up to 4 chars)
    int prefixLen = 0;
    final int maxPrefix = Math.min(WINKLER_MAX_PREFIX, Math.min(str1.length(), str2.length()));
    for (int i = 0; i < maxPrefix && str1.charAt(i) == str2.charAt(i); i++) {
      prefixLen++;
    }

    // Jaro-Winkler similarity
    return jaroSim + (prefixLen * WINKLER_PREFIX_WEIGHT * (1.0 - jaroSim));
  }

  private double jaroSimilarity(final String s1, final String s2) {
    final int len1 = s1.length();
    final int len2 = s2.length();

    // Matching window
    final int matchWindow = Math.max(0, Math.max(len1, len2) / 2 - 1);

    final boolean[] s1Matches = new boolean[len1];
    final boolean[] s2Matches = new boolean[len2];

    int matches = 0;
    int transpositions = 0;

    // Find matches
    for (int i = 0; i < len1; i++) {
      final int start = Math.max(0, i - matchWindow);
      final int end = Math.min(i + matchWindow + 1, len2);

      for (int j = start; j < end; j++) {
        if (s2Matches[j] || s1.charAt(i) != s2.charAt(j))
          continue;
        s1Matches[i] = true;
        s2Matches[j] = true;
        matches++;
        break;
      }
    }

    if (matches == 0)
      return 0.0;

    // Count transpositions
    int k = 0;
    for (int i = 0; i < len1; i++) {
      if (!s1Matches[i])
        continue;
      while (!s2Matches[k])
        k++;
      if (s1.charAt(i) != s2.charAt(k))
        transpositions++;
      k++;
    }

    return ((double) matches / len1 + (double) matches / len2 +
        (double) (matches - transpositions / 2) / matches) / 3.0;
  }
}
