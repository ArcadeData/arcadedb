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
package com.arcadedb.utility;

/**
 * String helpers that the engine needs on paths where the obvious form allocates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class StringUtils {
  protected StringUtils() {
  }

  /**
   * Case-insensitive {@code contains} that allocates nothing.
   * <p>
   * The obvious form, {@code haystack.toUpperCase().contains(needle)}, copies the whole haystack to answer a question
   * about a needle a dozen characters long - which is why every caller of this had written its own copy rather than
   * use it: the async DDL classifier scanning a whole script, the Cypher parser deciding on its own parse hot path,
   * the HTTP handler looking for a LIMIT in a command, and the MCP prompt fence looking for its own delimiter. Four
   * private copies of six lines, and this is the one they share.
   *
   * @return true when {@code needle} occurs anywhere in {@code haystack}, ignoring case. An empty needle is contained
   *     by everything, as {@code String.contains} has it.
   */
  public static boolean containsIgnoreCase(final String haystack, final String needle) {
    final int needleLength = needle.length();
    if (needleLength == 0)
      return true;

    final int last = haystack.length() - needleLength;
    for (int i = 0; i <= last; i++)
      if (haystack.regionMatches(true, i, needle, 0, needleLength))
        return true;

    return false;
  }
}
