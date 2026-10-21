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

import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10524: {@code text.camelCase()}, {@code text.upperCamelCase()} and {@code text.snakeCase()} deleted every character above
 * U+FFFF and put a word break where it was ({@code text.snakeCase('x𝐚y')} answered {@code 'x_y'}). The three were fixed with #10484, which
 * named them; this test is the one the report asks for, which names none: it enumerates every registered {@code text.*} function that
 * takes one string and answers a string, and holds each to the two rules a transformation of a string has to keep.
 * <ul>
 *   <li>the answer holds no unpaired surrogate, whatever the input</li>
 *   <li>a letter outside the BMP is kept wherever the same function keeps a BMP letter of the same case: the loss is the encoding, not
 *   the letter class</li>
 * </ul>
 * A function added later is held to both without being listed here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10524TextFunctionsKeepEveryCharacterTest {
  // lower case letters: é, and MATHEMATICAL BOLD SMALL A
  private static final String BMP_LOWER   = "é";
  private static final String ASTRAL_LOWER = new String(Character.toChars(0x1D41A));
  // upper case letters: É, and MATHEMATICAL BOLD CAPITAL A
  private static final String BMP_UPPER   = "É";
  private static final String ASTRAL_UPPER = new String(Character.toChars(0x1D400));
  // a letter with no case outside the BMP, and one inside: CJK Extension B, and a CJK ideograph
  private static final String BMP_CASELESS   = "中";
  private static final String ASTRAL_CASELESS = new String(Character.toChars(0x20000));

  @Test
  void everyStringToStringTextFunctionKeepsALetterOutsideTheBmpWhereItKeepsItsTwin() {
    final List<StatelessFunction> functions = singleStringTextFunctions();
    // the registry is not empty and holds the three of the report, or the loop below proves nothing
    assertThat(functions).extracting(StatelessFunction::getName).contains("text.camelCase", "text.upperCamelCase", "text.snakeCase");

    final List<String> lost = new ArrayList<>();
    for (final StatelessFunction function : functions) {
      check(function, BMP_LOWER, ASTRAL_LOWER, lost);
      check(function, BMP_UPPER, ASTRAL_UPPER, lost);
      check(function, BMP_CASELESS, ASTRAL_CASELESS, lost);
    }

    assertThat(lost).as("functions that lose or split a character outside the BMP").isEmpty();
  }

  private static void check(final StatelessFunction function, final String bmp, final String astral, final List<String> lost) {
    final String twin = answer(function, "x" + bmp + "y");
    if (twin == null)
      // does not answer a string for this input, or refuses it: nothing to hold the astral answer to
      return;

    final String answer;
    try {
      answer = (String) function.execute(new Object[] { "x" + astral + "y" }, new BasicCommandContext());
    } catch (final RuntimeException e) {
      lost.add(function.getName() + " fails on '" + astral + "' where it answers '" + twin + "' for '" + bmp + "': " + e);
      return;
    }
    if (answer == null) {
      // a function that answers null for the astral letter, where its BMP twin is kept, has refused the letter
      if (twin.contains(bmp))
        lost.add(function.getName() + " keeps '" + bmp + "' but answers null for '" + astral + "'");
      return;
    }

    if (hasUnpairedSurrogate(answer))
      lost.add(function.getName() + " answers an unpaired surrogate for 'x" + astral + "y': " + escaped(answer));
    else if (twin.contains(bmp) && !answer.contains(astral))
      lost.add(function.getName() + " keeps '" + bmp + "' but drops '" + astral + "': " + escaped(twin) + " vs " + escaped(answer));
  }

  private static List<StatelessFunction> singleStringTextFunctions() {
    final List<StatelessFunction> functions = new ArrayList<>();
    for (final StatelessFunction function : CypherFunctionRegistry.getAllFunctions())
      if (function.getName().startsWith("text.") && function.getMinArgs() <= 1 && function.getMaxArgs() >= 1 && answer(function, "ab") != null
          && onlyArgumentIsTheString(function))
        functions.add(function);
    return functions;
  }

  /** Functions that need more than the string are not held to the rule here: a second argument changes what they are asked. */
  private static boolean onlyArgumentIsTheString(final StatelessFunction function) {
    return function.getMinArgs() == 1;
  }

  private static String answer(final StatelessFunction function, final String input) {
    try {
      return function.execute(new Object[] { input }, new BasicCommandContext()) instanceof final String text ? text : null;
    } catch (final RuntimeException e) {
      return null;
    }
  }

  private static boolean hasUnpairedSurrogate(final String text) {
    for (int i = 0; i < text.length(); i++) {
      final char c = text.charAt(i);
      if (Character.isHighSurrogate(c)) {
        if (i + 1 >= text.length() || !Character.isLowSurrogate(text.charAt(i + 1)))
          return true;
        i++;
      } else if (Character.isLowSurrogate(c))
        return true;
    }
    return false;
  }

  private static String escaped(final String text) {
    final StringBuilder escaped = new StringBuilder("'");
    text.codePoints().forEach(codePoint -> escaped.append(codePoint < 0x80 ? String.valueOf((char) codePoint) : "\\u{" + Integer.toHexString(codePoint) + "}"));
    return escaped.append('\'').toString();
  }
}
