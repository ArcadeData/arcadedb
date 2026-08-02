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
package com.arcadedb.mcp.prompts;

import java.security.SecureRandom;

/**
 * Makes the delimiter block a prompt wraps around a caller-supplied value into a boundary the value cannot cross.
 * <p>
 * A prompt that fences an argument asserts that everything inside the block is data and everything outside it is
 * procedure. With a constant delimiter that assertion is advisory: a value carrying the closing tag ends the block
 * early, and what follows is read as procedure. Escaping the value would enforce the boundary at the cost of
 * showing the model a value the caller did not send, which the verbatim-substitution rule the prompts are built on
 * rules out.
 * <p>
 * The suffix returned here moves the delimiter instead of the value. It is empty whenever the value carries no
 * closing tag at all, so the overwhelmingly common render is byte for byte the constant template it always was, and
 * it is an unpredictable token otherwise, which the value cannot match because it was fixed before the token
 * existed. The token is drawn rather than derived from the value on purpose: a suffix computed from the value would
 * be computable by whoever wrote it, and a payload could then carry the delimiter its own text produces.
 */
final class PromptFence {
  private static final SecureRandom RANDOM = new SecureRandom();

  private PromptFence() {
  }

  /**
   * The suffix to append to both delimiters of {@code tag} so that {@code value} cannot close the block it opens.
   * Empty when the value contains no closing tag, in any case, in which case the delimiters stay constant.
   */
  static String suffixFor(final String tag, final String value) {
    if (!containsIgnoreCase(value, "</" + tag))
      return "";

    String suffix;
    do {
      suffix = "_%016x".formatted(RANDOM.nextLong());
    } while (containsIgnoreCase(value, suffix));

    return suffix;
  }

  /**
   * Scans in place rather than lower-casing the value, which for a document-sized argument would copy the whole
   * string to answer a question about a needle a dozen characters long.
   */
  private static boolean containsIgnoreCase(final String haystack, final String needle) {
    final int last = haystack.length() - needle.length();
    for (int i = 0; i <= last; i++)
      if (haystack.regionMatches(true, i, needle, 0, needle.length()))
        return true;

    return false;
  }
}
