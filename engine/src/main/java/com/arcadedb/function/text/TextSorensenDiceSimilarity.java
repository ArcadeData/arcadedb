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
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * text.sorensenDiceSimilarity(str1, str2, [languageTag]) - The Sorensen-Dice coefficient of the character bigrams of the two strings,
 * as {@code apoc.text.sorensenDiceSimilarity} computes it (issue #12104): each string is upper-cased in the language of the IETF tag
 * (default {@code 'en'}) and split into words at blanks, a bigram never spans two words, and the bigrams are a multiset, so
 * {@code 'aa'} and {@code 'aaaa'} share one of their four bigrams and score 0.5.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextSorensenDiceSimilarity extends AbstractTextFunction {
  private static final Pattern BLANKS = Pattern.compile("\\s+");

  @Override
  protected String getSimpleName() {
    return "sorensenDiceSimilarity";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Calculate the Sorensen-Dice similarity coefficient of the bigrams of the words, upper-cased in the language given "
        + "(default 'en')";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str1 = asString(args[0]);
    final String str2 = asString(args[1]);
    final String languageTag = args.length > 2 ? asString(args[2]) : "en";

    if (str1 == null || str2 == null || languageTag == null)
      return null;

    if (str1.equals(str2))
      return 1.0;

    final Locale locale = Locale.forLanguageTag(languageTag);
    final String[] words1 = BLANKS.split(str1.trim().toUpperCase(locale));
    final String[] words2 = BLANKS.split(str2.trim().toUpperCase(locale));
    if (Arrays.equals(words1, words2))
      return 1.0;

    final long[] bigrams1 = sortedBigrams(words1);
    final long[] bigrams2 = sortedBigrams(words2);
    // APOC answers NaN (0 / 0) when neither side has a bigram, 'x' against 'y'
    if (bigrams1.length + bigrams2.length == 0)
      return 0.0;

    int matches = 0;
    for (int i1 = 0, i2 = 0; i1 < bigrams1.length && i2 < bigrams2.length; ) {
      if (bigrams1[i1] == bigrams2[i2]) {
        matches++;
        i1++;
        i2++;
      } else if (bigrams1[i1] < bigrams2[i2])
        i1++;
      else
        i2++;
    }
    return 2.0 * matches / (bigrams1.length + bigrams2.length);
  }

  /**
   * The bigrams of every word, sorted, each one the two code points in a long. Characters, not UTF-16 units: a bigram is never cut
   * inside a surrogate pair, where APOC's are.
   */
  private static long[] sortedBigrams(final String[] words) {
    int count = 0;
    final int[][] characters = new int[words.length][];
    for (int i = 0; i < words.length; i++) {
      characters[i] = codePointsOf(words[i]);
      count += Math.max(characters[i].length - 1, 0);
    }

    final long[] bigrams = new long[count];
    int n = 0;
    for (final int[] word : characters)
      for (int i = 0; i < word.length - 1; i++)
        bigrams[n++] = (long) word[i] << Integer.SIZE | word[i + 1];
    Arrays.sort(bigrams);
    return bigrams;
  }
}
