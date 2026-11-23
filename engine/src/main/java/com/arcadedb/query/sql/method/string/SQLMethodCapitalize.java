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
package com.arcadedb.query.sql.method.string;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.Locale;

/**
 * Capitalizes every word of the text: the first letter of a word goes to title case and the rest of the word to lower case,
 * everything between words is copied as it is.
 *
 * @author Christian Himpe (gramian)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodCapitalize extends AbstractSQLMethod {

  public static final String NAME = "capitalize";

  public SQLMethodCapitalize() {
    super(NAME);
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    return value != null ? capitalize(value.toString()) : null;
  }

  /**
   * One pass over the code points, with no regular expression (issue #12869). The regex this replaces cut words at the ASCII-only
   * {@code \b}, so {@code "café"} became {@code "CafÉ"} and Cyrillic words were left untouched, and it handed the built text to
   * {@code Matcher.replaceAll} as a replacement string, where a {@code $} or a backslash of the data threw. Here nothing is ever
   * parsed as syntax. A word is lower-cased as a whole, so a context-sensitive mapping such as the Greek final sigma still sees
   * the letters before it ({@code "ΟΣ"} is {@code "Ος"}), and its first letter is title-cased by code point, so the {@code ǆ}
   * digraph becomes {@code ǅ}, {@code ß} is not expanded to {@code SS} and a letter outside the BMP is not split in two.
   */
  private static String capitalize(final String text) {
    final int length = text.length();
    final StringBuilder result = new StringBuilder(length);
    int i = 0;
    while (i < length) {
      final int first = text.codePointAt(i);
      if (!isWordCharacter(first)) {
        result.appendCodePoint(first);
        i += Character.charCount(first);
        continue;
      }

      int end = i + Character.charCount(first);
      while (end < length) {
        final int cp = text.codePointAt(end);
        if (!isWordCharacter(cp))
          break;
        end += Character.charCount(cp);
      }

      result.appendCodePoint(Character.toTitleCase(first));
      final String lower = text.substring(i, end).toLowerCase(Locale.ENGLISH);
      result.append(lower, lowerCaseLength(first), lower.length());
      i = end;
    }
    return result.toString();
  }

  /**
   * The same set as {@code \w} under {@code Pattern.UNICODE_CHARACTER_CLASS}: letters, marks, decimal digits, connector
   * punctuation and the joiners, so a combining accent stays in its word and {@code _} keeps joining words as it always did.
   */
  private static boolean isWordCharacter(final int codePoint) {
    if (codePoint < 0x80)
      return (codePoint >= 'a' && codePoint <= 'z') || (codePoint >= 'A' && codePoint <= 'Z') || (codePoint >= '0' && codePoint <= '9')
          || codePoint == '_';
    if (Character.isAlphabetic(codePoint) || Character.isDigit(codePoint) || codePoint == 0x200C || codePoint == 0x200D)
      return true;
    return switch (Character.getType(codePoint)) {
      case Character.NON_SPACING_MARK, Character.ENCLOSING_MARK, Character.COMBINING_SPACING_MARK, Character.CONNECTOR_PUNCTUATION -> true;
      default -> false;
    };
  }

  /**
   * How many chars the first code point of a word takes once the word is lower-cased under {@code Locale.ENGLISH}. The only
   * code point whose lower case is not a single code point there is {@code İ} (capital I with dot above), which becomes
   * {@code "i̇"}; skipping a single char would leave its dot behind.
   */
  private static int lowerCaseLength(final int codePoint) {
    return codePoint == 0x130 ? 2 : Character.charCount(Character.toLowerCase(codePoint));
  }
}
