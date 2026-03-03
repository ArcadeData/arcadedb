/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for SQL-style text functions (ToUpper, ToLower, Trim, etc.)
 * that directly implement StatelessFunction.
 */
class TextStatelessFunctionsTest {

  // ============ ToUpperFunction tests ============

  @Test
  void toUpperBasic() {
    final ToUpperFunction fn = new ToUpperFunction();
    assertThat(fn.getName()).isEqualTo("toUpper");

    assertThat(fn.execute(new Object[]{"hello"}, null)).isEqualTo("HELLO");
    assertThat(fn.execute(new Object[]{"Hello World"}, null)).isEqualTo("HELLO WORLD");
  }

  @Test
  void toUpperNullReturnsNull() {
    final ToUpperFunction fn = new ToUpperFunction();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void toUpperWrongArgCount() {
    final ToUpperFunction fn = new ToUpperFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"a", "b"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ ToLowerFunction tests ============

  @Test
  void toLowerBasic() {
    final ToLowerFunction fn = new ToLowerFunction();
    assertThat(fn.getName()).isEqualTo("toLower");

    assertThat(fn.execute(new Object[]{"HELLO"}, null)).isEqualTo("hello");
    assertThat(fn.execute(new Object[]{"Hello World"}, null)).isEqualTo("hello world");
  }

  @Test
  void toLowerNullReturnsNull() {
    final ToLowerFunction fn = new ToLowerFunction();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void toLowerWrongArgCount() {
    final ToLowerFunction fn = new ToLowerFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"a", "b"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ TrimFunction tests ============

  @Test
  void trimSimple() {
    final TrimFunction fn = new TrimFunction();
    assertThat(fn.getName()).isEqualTo("trim");

    assertThat(fn.execute(new Object[]{"  hello  "}, null)).isEqualTo("hello");
    assertThat(fn.execute(new Object[]{"hello"}, null)).isEqualTo("hello");
  }

  @Test
  void trimNullReturnsNull() {
    final TrimFunction fn = new TrimFunction();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void trimExtendedBoth() {
    final TrimFunction fn = new TrimFunction();

    assertThat(fn.execute(new Object[]{"BOTH", "x", "xxxhelloxxx"}, null)).isEqualTo("hello");
  }

  @Test
  void trimExtendedLeading() {
    final TrimFunction fn = new TrimFunction();

    assertThat(fn.execute(new Object[]{"LEADING", "x", "xxxhelloxxx"}, null)).isEqualTo("helloxxx");
  }

  @Test
  void trimExtendedTrailing() {
    final TrimFunction fn = new TrimFunction();

    assertThat(fn.execute(new Object[]{"TRAILING", "x", "xxxhelloxxx"}, null)).isEqualTo("xxxhello");
  }

  @Test
  void trimExtendedNullSource() {
    final TrimFunction fn = new TrimFunction();

    assertThat(fn.execute(new Object[]{"BOTH", "x", null}, null)).isNull();
  }

  @Test
  void trimExtendedNullTrimChar() {
    final TrimFunction fn = new TrimFunction();

    // Null trimChar should fall back to whitespace stripping
    assertThat(fn.execute(new Object[]{"LEADING", null, "  hello  "}, null)).isEqualTo("hello  ");
  }

  @Test
  void trimWrongArgCount() {
    final TrimFunction fn = new TrimFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"a", "b"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ LTrimFunction tests ============

  @Test
  void lTrimBasic() {
    final LTrimFunction fn = new LTrimFunction();
    assertThat(fn.getName()).isEqualTo("lTrim");

    assertThat(fn.execute(new Object[]{"  hello  "}, null)).isEqualTo("hello  ");
    assertThat(fn.execute(new Object[]{"hello"}, null)).isEqualTo("hello");
  }

  @Test
  void lTrimNullReturnsNull() {
    final LTrimFunction fn = new LTrimFunction();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void lTrimWrongArgCount() {
    final LTrimFunction fn = new LTrimFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"a", "b"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ RTrimFunction tests ============

  @Test
  void rTrimBasic() {
    final RTrimFunction fn = new RTrimFunction();
    assertThat(fn.getName()).isEqualTo("rTrim");

    assertThat(fn.execute(new Object[]{"  hello  "}, null)).isEqualTo("  hello");
    assertThat(fn.execute(new Object[]{"hello"}, null)).isEqualTo("hello");
  }

  @Test
  void rTrimNullReturnsNull() {
    final RTrimFunction fn = new RTrimFunction();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void rTrimWrongArgCount() {
    final RTrimFunction fn = new RTrimFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"a", "b"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ LeftFunction tests ============

  @Test
  void leftBasic() {
    final LeftFunction fn = new LeftFunction();
    assertThat(fn.getName()).isEqualTo("left");

    assertThat(fn.execute(new Object[]{"hello", 3}, null)).isEqualTo("hel");
    assertThat(fn.execute(new Object[]{"hello", 10}, null)).isEqualTo("hello");
    assertThat(fn.execute(new Object[]{"hello", 0}, null)).isEqualTo("");
  }

  @Test
  void leftNullReturnsNull() {
    final LeftFunction fn = new LeftFunction();
    assertThat(fn.execute(new Object[]{null, 3}, null)).isNull();
  }

  @Test
  void leftWrongArgCount() {
    final LeftFunction fn = new LeftFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"hello"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ RightFunction tests ============

  @Test
  void rightBasic() {
    final RightFunction fn = new RightFunction();
    assertThat(fn.getName()).isEqualTo("right");

    assertThat(fn.execute(new Object[]{"hello", 3}, null)).isEqualTo("llo");
    assertThat(fn.execute(new Object[]{"hello", 10}, null)).isEqualTo("hello");
    assertThat(fn.execute(new Object[]{"hello", 0}, null)).isEqualTo("");
  }

  @Test
  void rightNullReturnsNull() {
    final RightFunction fn = new RightFunction();
    assertThat(fn.execute(new Object[]{null, 3}, null)).isNull();
  }

  @Test
  void rightWrongArgCount() {
    final RightFunction fn = new RightFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"hello"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ SubstringFunction tests ============

  @Test
  void substringTwoArgs() {
    final SubstringFunction fn = new SubstringFunction();
    assertThat(fn.getName()).isEqualTo("substring");

    assertThat(fn.execute(new Object[]{"hello world", 6}, null)).isEqualTo("world");
  }

  @Test
  void substringThreeArgs() {
    final SubstringFunction fn = new SubstringFunction();

    assertThat(fn.execute(new Object[]{"hello world", 0, 5}, null)).isEqualTo("hello");
    assertThat(fn.execute(new Object[]{"hello world", 6, 100}, null)).isEqualTo("world");
  }

  @Test
  void substringNullReturnsNull() {
    final SubstringFunction fn = new SubstringFunction();
    assertThat(fn.execute(new Object[]{null, 0}, null)).isNull();
  }

  @Test
  void substringOutOfRange() {
    final SubstringFunction fn = new SubstringFunction();

    // Start beyond string length returns empty
    assertThat(fn.execute(new Object[]{"hello", 100}, null)).isEqualTo("");
    // Negative start returns empty
    assertThat(fn.execute(new Object[]{"hello", -1}, null)).isEqualTo("");
  }

  @Test
  void substringWrongArgCount() {
    final SubstringFunction fn = new SubstringFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"hello"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ SplitFunction tests ============

  @Test
  void splitFunctionBasic() {
    final SplitFunction fn = new SplitFunction();
    assertThat(fn.getName()).isEqualTo("split");

    @SuppressWarnings("unchecked")
    final List<String> result = (List<String>) fn.execute(new Object[]{"a,b,c", ","}, null);
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void splitFunctionNullReturnsNull() {
    final SplitFunction fn = new SplitFunction();
    assertThat(fn.execute(new Object[]{null, ","}, null)).isNull();
  }

  @Test
  void splitFunctionSpecialChars() {
    final SplitFunction fn = new SplitFunction();

    // Regex special chars should be treated as literals
    @SuppressWarnings("unchecked")
    final List<String> result = (List<String>) fn.execute(new Object[]{"a.b.c", "."}, null);
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void splitFunctionNullDelimiterThrowsNpe() {
    final SplitFunction fn = new SplitFunction();

    // SplitFunction has no null guard on args[1] — throws NPE unlike TextSplit which handles it gracefully
    assertThatThrownBy(() -> fn.execute(new Object[]{"abc", null}, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void splitFunctionWrongArgCount() {
    final SplitFunction fn = new SplitFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"hello"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ ReplaceFunction tests ============

  @Test
  void replaceFunctionBasic() {
    final ReplaceFunction fn = new ReplaceFunction();
    assertThat(fn.getName()).isEqualTo("replace");

    assertThat(fn.execute(new Object[]{"hello world", "world", "there"}, null)).isEqualTo("hello there");
    assertThat(fn.execute(new Object[]{"aaa", "a", "bb"}, null)).isEqualTo("bbbbbb");
  }

  @Test
  void replaceFunctionNullReturnsNull() {
    final ReplaceFunction fn = new ReplaceFunction();
    assertThat(fn.execute(new Object[]{null, "a", "b"}, null)).isNull();
  }

  @Test
  void replaceFunctionWrongArgCount() {
    final ReplaceFunction fn = new ReplaceFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"hello", "l"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ CharLengthFunction tests ============

  @Test
  void charLengthBasic() {
    final CharLengthFunction fn = new CharLengthFunction();
    assertThat(fn.getName()).isEqualTo("char_length");

    assertThat(fn.execute(new Object[]{"hello"}, null)).isEqualTo(5L);
    assertThat(fn.execute(new Object[]{""}, null)).isEqualTo(0L);
  }

  @Test
  void charLengthNullReturnsNull() {
    final CharLengthFunction fn = new CharLengthFunction();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void charLengthWrongArgCount() {
    final CharLengthFunction fn = new CharLengthFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"a", "b"}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ NormalizeFunction tests ============

  @Test
  void normalizeDefaultNFC() {
    final NormalizeFunction fn = new NormalizeFunction();
    assertThat(fn.getName()).isEqualTo("normalize");

    // e + combining acute accent (NFD) -> single é character (NFC)
    final String nfdInput = "e\u0301"; // decomposed é
    final String result = (String) fn.execute(new Object[]{nfdInput}, null);
    assertThat(result).isEqualTo("\u00e9"); // composed é
  }

  @Test
  void normalizeWithForm() {
    final NormalizeFunction fn = new NormalizeFunction();

    // NFC -> NFD decomposition
    final String nfcInput = "\u00e9"; // composed é
    final String result = (String) fn.execute(new Object[]{nfcInput, "NFD"}, null);
    assertThat(result).isEqualTo("e\u0301"); // decomposed
  }

  @Test
  void normalizeNullReturnsNull() {
    final NormalizeFunction fn = new NormalizeFunction();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void normalizeInvalidForm() {
    final NormalizeFunction fn = new NormalizeFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"hello", "INVALID"}, null))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("unsupported normalization form");
  }

  @Test
  void normalizeWrongArgCount() {
    final NormalizeFunction fn = new NormalizeFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{}, null))
        .isInstanceOf(CommandExecutionException.class);
  }

  // ============ FormatFunction tests ============

  @Test
  void formatFunctionTemporalNoPattern() {
    final FormatFunction fn = new FormatFunction();
    assertThat(fn.getName()).isEqualTo("format");

    final CypherDate date = new CypherDate(LocalDate.of(2026, 3, 3));
    // Without pattern, returns toString()
    final String result = (String) fn.execute(new Object[]{date}, null);
    assertThat(result).isEqualTo("2026-03-03");
  }

  @Test
  void formatFunctionTemporalWithPattern() {
    final FormatFunction fn = new FormatFunction();

    final CypherDate date = new CypherDate(LocalDate.of(2026, 3, 3));
    final String result = (String) fn.execute(new Object[]{date, "dd/MM/yyyy"}, null);
    assertThat(result).isEqualTo("03/03/2026");
  }

  @Test
  void formatFunctionNullReturnsNull() {
    final FormatFunction fn = new FormatFunction();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void formatFunctionNullPattern() {
    final FormatFunction fn = new FormatFunction();

    final CypherDate date = new CypherDate(LocalDate.of(2026, 1, 15));
    // Null pattern returns toString()
    final String result = (String) fn.execute(new Object[]{date, null}, null);
    assertThat(result).isEqualTo("2026-01-15");
  }

  @Test
  void formatFunctionDurationWithPatternThrows() {
    final FormatFunction fn = new FormatFunction();

    final CypherDuration duration = CypherDuration.parse("P1Y2M3D");
    assertThatThrownBy(() -> fn.execute(new Object[]{duration, "yyyy"}, null))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Duration");
  }

  @Test
  void formatFunctionNonTemporalWithPatternThrows() {
    final FormatFunction fn = new FormatFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{"not-a-temporal", "yyyy"}, null))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("temporal");
  }

  @Test
  void formatFunctionWrongArgCount() {
    final FormatFunction fn = new FormatFunction();

    assertThatThrownBy(() -> fn.execute(new Object[]{}, null))
        .isInstanceOf(CommandExecutionException.class);
  }
}
