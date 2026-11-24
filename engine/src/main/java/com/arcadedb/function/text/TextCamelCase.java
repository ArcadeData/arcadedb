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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * text.camelCase(string) - Convert to camelCase, as {@code apoc.text.camelCase} does (issue #12104): the words are the runs of letters
 * and ASCII digits, each one lower-cased and then capitalized, and the first character of the result is lower case, so the blanks and
 * separators around the words never capitalize the first one ({@code '  padded  '} is {@code 'padded'}).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class TextCamelCase extends AbstractTextFunction {
  private static final Pattern WORD = Pattern.compile("[\\p{L}0-9]+");

  @Override
  protected String getSimpleName() {
    return "camelCase";
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
    return "Convert string to camelCase: the words (letters and digits) lower-cased and capitalized, the first character lower case";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final String str = asString(args[0]);
    return str == null ? null : camelCase(str);
  }

  /**
   * The camelCase of a string, shared with text.upperCamelCase(). By code point (issue #10484): a letter outside the BMP is a letter of
   * the word, never a word break. A string without a letter or a digit is the empty string, where APOC fails.
   */
  static String camelCase(final String str) {
    final StringBuilder result = new StringBuilder(str.length());
    final Matcher words = WORD.matcher(str);
    while (words.find())
      result.append(titleCaseFirst(words.group().toLowerCase(Locale.ROOT)));
    if (result.isEmpty())
      return "";

    final int firstLength = Character.charCount(result.codePointAt(0));
    return result.substring(0, firstLength).toLowerCase(Locale.ROOT) + result.substring(firstLength);
  }
}
