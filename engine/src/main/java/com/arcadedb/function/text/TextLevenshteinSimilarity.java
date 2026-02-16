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
 * text.levenshteinSimilarity(str1, str2) - Calculate similarity (0-1) based on edit distance.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextLevenshteinSimilarity extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "levenshteinSimilarity";
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
    return "Calculate similarity (0-1) based on Levenshtein edit distance";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str1 = asString(args[0]);
    final String str2 = asString(args[1]);

    if (str1 == null || str2 == null)
      return null;

    if (str1.isEmpty() && str2.isEmpty())
      return 1.0;

    final int maxLen = Math.max(str1.length(), str2.length());
    if (maxLen == 0)
      return 1.0;

    final int distance = TextLevenshteinDistance.levenshteinDistance(str1, str2);
    return 1.0 - ((double) distance / maxLen);
  }
}
