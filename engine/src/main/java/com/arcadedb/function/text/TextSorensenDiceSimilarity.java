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

import java.util.HashSet;
import java.util.Set;

/**
 * text.sorensenDiceSimilarity(str1, str2) - Calculate Sorensen-Dice coefficient.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextSorensenDiceSimilarity extends AbstractTextFunction {
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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Calculate the Sorensen-Dice similarity coefficient based on bigrams";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str1 = asString(args[0]);
    final String str2 = asString(args[1]);

    if (str1 == null || str2 == null)
      return null;

    if (str1.isEmpty() && str2.isEmpty())
      return 1.0;

    if (str1.length() < 2 || str2.length() < 2) {
      // For very short strings, fall back to equality check
      return str1.equals(str2) ? 1.0 : 0.0;
    }

    final Set<String> bigrams1 = getBigrams(str1);
    final Set<String> bigrams2 = getBigrams(str2);

    // Count intersection
    int intersection = 0;
    for (final String bigram : bigrams1) {
      if (bigrams2.contains(bigram)) {
        intersection++;
      }
    }

    // Sorensen-Dice coefficient: 2 * |intersection| / (|set1| + |set2|)
    return (2.0 * intersection) / (bigrams1.size() + bigrams2.size());
  }

  private Set<String> getBigrams(final String s) {
    final Set<String> bigrams = new HashSet<>();
    for (int i = 0; i < s.length() - 1; i++) {
      bigrams.add(s.substring(i, i + 2).toLowerCase());
    }
    return bigrams;
  }
}
