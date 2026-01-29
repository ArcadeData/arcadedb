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
 * text.hammingDistance(str1, str2) - Calculate Hamming distance (strings must be equal length).
 *
 * @author ArcadeDB Team
 */
public class TextHammingDistance extends AbstractTextFunction {
  @Override
  protected String getSimpleName() {
    return "hammingDistance";
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
    return "Calculate the Hamming distance between two equal-length strings";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str1 = asString(args[0]);
    final String str2 = asString(args[1]);

    if (str1 == null || str2 == null)
      return null;

    if (str1.length() != str2.length()) {
      throw new IllegalArgumentException(
          "text.hammingDistance() requires strings of equal length, got " +
              str1.length() + " and " + str2.length());
    }

    int distance = 0;
    for (int i = 0; i < str1.length(); i++) {
      if (str1.charAt(i) != str2.charAt(i)) {
        distance++;
      }
    }

    return (long) distance;
  }
}
