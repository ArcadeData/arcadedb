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

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * text.snakeCase(string) - Lower-case words joined by '-', as {@code apoc.text.snakeCase} answers (issue #12104): a word starts at
 * every character that is neither a lower-case letter nor an ASCII digit, so {@code 'fooBarBaz'} is {@code 'foo-bar-baz'}, and a
 * string of upper-case letters, digits and underscores ({@code 'FOO_BAR'}) is first read as words separated by the underscores.
 * Blanks around a word are dropped, and every other character stays at the start of its word ({@code 'a.b'} is {@code 'a-.b'}).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextSnakeCase extends AbstractTextFunction {
  private static final Pattern CONSTANT_CASE = Pattern.compile("[\\p{Lu}0-9_]+");

  @Override
  protected String getSimpleName() {
    return "snakeCase";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Convert string to snake case: lower-case words joined by '-', a word starting at each upper-case letter or separator";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    if (str == null)
      return null;

    final String text = CONSTANT_CASE.matcher(str).matches() ? str.toLowerCase(Locale.ROOT).replace('_', ' ') : str;

    final StringBuilder result = new StringBuilder(text.length() + 8);
    int begin = 0;
    // By code point (issue #10484): APOC also starts a word at the second half of a surrogate pair
    for (int i = 0; i < text.length(); ) {
      final int c = text.codePointAt(i);
      if (i > 0 && Character.getType(c) != Character.LOWERCASE_LETTER && (c < '0' || c > '9')) {
        appendWord(result, text.substring(begin, i));
        begin = i;
      }
      i += Character.charCount(c);
    }
    appendWord(result, text.substring(begin));

    return result.toString().toLowerCase(Locale.ROOT).replace("--", "-");
  }

  private static void appendWord(final StringBuilder result, final String word) {
    final String trimmed = word.trim();
    if (trimmed.isEmpty())
      return;
    if (!result.isEmpty())
      result.append('-');
    result.append(trimmed.toLowerCase(Locale.ROOT));
  }
}
