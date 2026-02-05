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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.query.opencypher.functions.text.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for OpenCypher text functions.
 */
class OpenCypherTextFunctionsTest {

  // ============ TextIndexOf tests ============

  @Test
  void textIndexOfBasic() {
    final TextIndexOf fn = new TextIndexOf();
    assertThat(fn.getName()).isEqualTo("text.indexOf");

    assertThat(fn.execute(new Object[]{"hello world", "world"}, null)).isEqualTo(6L);
    assertThat(fn.execute(new Object[]{"hello world", "hello"}, null)).isEqualTo(0L);
    assertThat(fn.execute(new Object[]{"hello world", "x"}, null)).isEqualTo(-1L);
  }

  @Test
  void textIndexOfWithStartPosition() {
    final TextIndexOf fn = new TextIndexOf();

    assertThat(fn.execute(new Object[]{"hello hello", "hello", 1}, null)).isEqualTo(6L);
    assertThat(fn.execute(new Object[]{"hello hello", "hello", 0}, null)).isEqualTo(0L);
    assertThat(fn.execute(new Object[]{"hello hello", "hello", 10}, null)).isEqualTo(-1L);
  }

  @Test
  void textIndexOfNullHandling() {
    final TextIndexOf fn = new TextIndexOf();

    assertThat(fn.execute(new Object[]{null, "test"}, null)).isNull();
    assertThat(fn.execute(new Object[]{"test", null}, null)).isNull();
  }

  // ============ TextSplit tests ============

  @Test
  void textSplitBasic() {
    final TextSplit fn = new TextSplit();
    assertThat(fn.getName()).isEqualTo("text.split");

    @SuppressWarnings("unchecked")
    final List<String> result = (List<String>) fn.execute(new Object[]{"a,b,c", ","}, null);
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void textSplitEmptyDelimiter() {
    final TextSplit fn = new TextSplit();

    @SuppressWarnings("unchecked")
    final List<String> result = (List<String>) fn.execute(new Object[]{"abc", ""}, null);
    assertThat(result).containsExactly("a", "b", "c");
  }

  @Test
  void textSplitNullString() {
    final TextSplit fn = new TextSplit();
    assertThat(fn.execute(new Object[]{null, ","}, null)).isNull();
  }

  @Test
  void textSplitSpecialCharDelimiter() {
    final TextSplit fn = new TextSplit();

    // Regex special characters should be treated as literals
    @SuppressWarnings("unchecked")
    final List<String> result = (List<String>) fn.execute(new Object[]{"a.b.c", "."}, null);
    assertThat(result).containsExactly("a", "b", "c");
  }

  // ============ TextJoin tests ============

  @Test
  void textJoinBasic() {
    final TextJoin fn = new TextJoin();
    assertThat(fn.getName()).isEqualTo("text.join");

    final Object result = fn.execute(new Object[]{Arrays.asList("a", "b", "c"), ","}, null);
    assertThat(result).isEqualTo("a,b,c");
  }

  @Test
  void textJoinWithNullElements() {
    final TextJoin fn = new TextJoin();

    final Object result = fn.execute(new Object[]{Arrays.asList("a", null, "c"), "-"}, null);
    assertThat(result).isEqualTo("a--c");
  }

  @Test
  void textJoinNullList() {
    final TextJoin fn = new TextJoin();
    assertThat(fn.execute(new Object[]{null, ","}, null)).isNull();
  }

  @Test
  void textJoinNullDelimiter() {
    final TextJoin fn = new TextJoin();

    final Object result = fn.execute(new Object[]{Arrays.asList("a", "b", "c"), null}, null);
    assertThat(result).isEqualTo("abc");
  }

  @Test
  void textJoinInvalidFirstArg() {
    final TextJoin fn = new TextJoin();

    assertThatThrownBy(() -> fn.execute(new Object[]{"not a list", ","}, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a list");
  }

  // ============ TextCapitalize tests ============

  @Test
  void textCapitalizeBasic() {
    final TextCapitalize fn = new TextCapitalize();
    assertThat(fn.getName()).isEqualTo("text.capitalize");

    assertThat(fn.execute(new Object[]{"hello"}, null)).isEqualTo("Hello");
    assertThat(fn.execute(new Object[]{"HELLO"}, null)).isEqualTo("HELLO");
    assertThat(fn.execute(new Object[]{"hello world"}, null)).isEqualTo("Hello world");
  }

  @Test
  void textCapitalizeEdgeCases() {
    final TextCapitalize fn = new TextCapitalize();

    assertThat(fn.execute(new Object[]{null}, null)).isNull();
    assertThat(fn.execute(new Object[]{""}, null)).isEqualTo("");
    assertThat(fn.execute(new Object[]{"a"}, null)).isEqualTo("A");
  }

  // ============ TextLevenshteinDistance tests ============

  @Test
  void textLevenshteinDistanceBasic() {
    final TextLevenshteinDistance fn = new TextLevenshteinDistance();
    assertThat(fn.getName()).isEqualTo("text.levenshteinDistance");

    assertThat(fn.execute(new Object[]{"kitten", "sitting"}, null)).isEqualTo(3L);
    assertThat(fn.execute(new Object[]{"hello", "hello"}, null)).isEqualTo(0L);
    assertThat(fn.execute(new Object[]{"", "abc"}, null)).isEqualTo(3L);
    assertThat(fn.execute(new Object[]{"abc", ""}, null)).isEqualTo(3L);
  }

  @Test
  void textLevenshteinDistanceNullHandling() {
    final TextLevenshteinDistance fn = new TextLevenshteinDistance();

    assertThat(fn.execute(new Object[]{null, "test"}, null)).isNull();
    assertThat(fn.execute(new Object[]{"test", null}, null)).isNull();
  }

  @Test
  void textLevenshteinDistanceStaticMethod() {
    // Test the static method directly
    assertThat(TextLevenshteinDistance.levenshteinDistance("cat", "hat")).isEqualTo(1);
    assertThat(TextLevenshteinDistance.levenshteinDistance("book", "back")).isEqualTo(2);
    assertThat(TextLevenshteinDistance.levenshteinDistance("", "")).isEqualTo(0);
  }

  // ============ TextJaroWinklerDistance tests ============

  @Test
  void textJaroWinklerDistanceBasic() {
    final TextJaroWinklerDistance fn = new TextJaroWinklerDistance();
    assertThat(fn.getName()).isEqualTo("text.jaroWinklerDistance");

    // Identical strings should return 1.0
    assertThat((Double) fn.execute(new Object[]{"hello", "hello"}, null)).isCloseTo(1.0, within(0.001));

    // Completely different strings should return 0.0
    assertThat((Double) fn.execute(new Object[]{"abc", "xyz"}, null)).isCloseTo(0.0, within(0.001));
  }

  @Test
  void textJaroWinklerDistanceNullHandling() {
    final TextJaroWinklerDistance fn = new TextJaroWinklerDistance();

    assertThat(fn.execute(new Object[]{null, "test"}, null)).isNull();
    assertThat(fn.execute(new Object[]{"test", null}, null)).isNull();
  }

  // ============ TextCamelCase tests ============

  @Test
  void textCamelCaseBasic() {
    final TextCamelCase fn = new TextCamelCase();
    assertThat(fn.getName()).isEqualTo("text.camelCase");

    assertThat(fn.execute(new Object[]{"hello world"}, null)).isEqualTo("helloWorld");
    assertThat(fn.execute(new Object[]{"HELLO_WORLD"}, null)).isEqualTo("helloWorld");
    assertThat(fn.execute(new Object[]{"hello-world"}, null)).isEqualTo("helloWorld");
  }

  @Test
  void textCamelCaseEdgeCases() {
    final TextCamelCase fn = new TextCamelCase();

    assertThat(fn.execute(new Object[]{null}, null)).isNull();
    assertThat(fn.execute(new Object[]{""}, null)).isEqualTo("");
    assertThat(fn.execute(new Object[]{"a"}, null)).isEqualTo("a");
  }

  // ============ TextFormat tests ============

  @Test
  void textFormatBasic() {
    final TextFormat fn = new TextFormat();
    assertThat(fn.getName()).isEqualTo("text.format");

    // TextFormat takes varargs, not a list
    assertThat(fn.execute(new Object[]{"Hello %s!", "World"}, null)).isEqualTo("Hello World!");
    assertThat(fn.execute(new Object[]{"Value: %d", 42}, null)).isEqualTo("Value: 42");
  }

  @Test
  void textFormatNullFormat() {
    final TextFormat fn = new TextFormat();
    assertThat(fn.execute(new Object[]{null, "test"}, null)).isNull();
  }

  // ============ TextByteCount tests ============

  @Test
  void textByteCountBasic() {
    final TextByteCount fn = new TextByteCount();
    assertThat(fn.getName()).isEqualTo("text.byteCount");

    assertThat(fn.execute(new Object[]{"hello"}, null)).isEqualTo(5L);
    assertThat(fn.execute(new Object[]{""}, null)).isEqualTo(0L);
  }

  @Test
  void textByteCountUnicode() {
    final TextByteCount fn = new TextByteCount();

    // Multi-byte UTF-8 character
    assertThat((Long) fn.execute(new Object[]{"\u00e9"}, null)).isGreaterThan(1L); // é
  }

  @Test
  void textByteCountNull() {
    final TextByteCount fn = new TextByteCount();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ TextCharAt tests ============

  @Test
  void textCharAtBasic() {
    final TextCharAt fn = new TextCharAt();
    assertThat(fn.getName()).isEqualTo("text.charAt");

    assertThat(fn.execute(new Object[]{"hello", 0}, null)).isEqualTo("h");
    assertThat(fn.execute(new Object[]{"hello", 4}, null)).isEqualTo("o");
  }

  @Test
  void textCharAtNull() {
    final TextCharAt fn = new TextCharAt();
    assertThat(fn.execute(new Object[]{null, 0}, null)).isNull();
  }

  // ============ TextCode tests ============

  @Test
  void textCodeBasic() {
    final TextCode fn = new TextCode();
    assertThat(fn.getName()).isEqualTo("text.code");

    assertThat(fn.execute(new Object[]{"A"}, null)).isEqualTo(65L);
    assertThat(fn.execute(new Object[]{"a"}, null)).isEqualTo(97L);
  }

  @Test
  void textCodeNull() {
    final TextCode fn = new TextCode();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ TextHexValue tests ============

  @Test
  void textHexValueFromNumber() {
    final TextHexValue fn = new TextHexValue();
    assertThat(fn.getName()).isEqualTo("text.hexValue");

    // TextHexValue converts numbers to hex string
    assertThat(fn.execute(new Object[]{255}, null)).isEqualTo("ff");
    assertThat(fn.execute(new Object[]{16}, null)).isEqualTo("10");
    assertThat(fn.execute(new Object[]{0}, null)).isEqualTo("0");
  }

  @Test
  void textHexValueFromString() {
    final TextHexValue fn = new TextHexValue();

    // For strings, each character is converted to 4-digit hex (UTF-16)
    assertThat(fn.execute(new Object[]{"A"}, null)).isEqualTo("0041");
    assertThat(fn.execute(new Object[]{"AB"}, null)).isEqualTo("00410042");
  }

  @Test
  void textHexValueNull() {
    final TextHexValue fn = new TextHexValue();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ TextHammingDistance tests ============

  @Test
  void textHammingDistanceBasic() {
    final TextHammingDistance fn = new TextHammingDistance();
    assertThat(fn.getName()).isEqualTo("text.hammingDistance");

    assertThat(fn.execute(new Object[]{"karolin", "kathrin"}, null)).isEqualTo(3L);
    assertThat(fn.execute(new Object[]{"hello", "hello"}, null)).isEqualTo(0L);
  }

  @Test
  void textHammingDistanceNullHandling() {
    final TextHammingDistance fn = new TextHammingDistance();

    assertThat(fn.execute(new Object[]{null, "test"}, null)).isNull();
    assertThat(fn.execute(new Object[]{"test", null}, null)).isNull();
  }

  // ============ TextDecapitalize tests ============

  @Test
  void textDecapitalizeBasic() {
    final TextDecapitalize fn = new TextDecapitalize();
    assertThat(fn.getName()).isEqualTo("text.decapitalize");

    assertThat(fn.execute(new Object[]{"Hello"}, null)).isEqualTo("hello");
    assertThat(fn.execute(new Object[]{"HELLO"}, null)).isEqualTo("hELLO");
  }

  @Test
  void textDecapitalizeEdgeCases() {
    final TextDecapitalize fn = new TextDecapitalize();

    assertThat(fn.execute(new Object[]{null}, null)).isNull();
    assertThat(fn.execute(new Object[]{""}, null)).isEqualTo("");
  }

  // ============ TextCapitalizeAll tests ============

  @Test
  void textCapitalizeAllBasic() {
    final TextCapitalizeAll fn = new TextCapitalizeAll();
    assertThat(fn.getName()).isEqualTo("text.capitalizeAll");

    assertThat(fn.execute(new Object[]{"hello world"}, null)).isEqualTo("Hello World");
  }

  @Test
  void textCapitalizeAllNull() {
    final TextCapitalizeAll fn = new TextCapitalizeAll();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ TextDecapitalizeAll tests ============

  @Test
  void textDecapitalizeAllBasic() {
    final TextDecapitalizeAll fn = new TextDecapitalizeAll();
    assertThat(fn.getName()).isEqualTo("text.decapitalizeAll");

    assertThat(fn.execute(new Object[]{"Hello World"}, null)).isEqualTo("hello world");
  }

  @Test
  void textDecapitalizeAllNull() {
    final TextDecapitalizeAll fn = new TextDecapitalizeAll();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ TextRandom tests ============

  @Test
  void textRandomBasic() {
    final TextRandom fn = new TextRandom();
    assertThat(fn.getName()).isEqualTo("text.random");

    final String result = (String) fn.execute(new Object[]{10}, null);
    assertThat(result).hasSize(10);
  }

  @Test
  void textRandomWithCharset() {
    final TextRandom fn = new TextRandom();

    final String result = (String) fn.execute(new Object[]{5, "abc"}, null);
    assertThat(result).hasSize(5);
    assertThat(result).matches("[abc]+");
  }

  // ============ TextLpad tests ============

  @Test
  void textLpadBasic() {
    final TextLpad fn = new TextLpad();
    assertThat(fn.getName()).isEqualTo("text.lpad");

    assertThat(fn.execute(new Object[]{"test", 8, "0"}, null)).isEqualTo("0000test");
    assertThat(fn.execute(new Object[]{"test", 4, "0"}, null)).isEqualTo("test");
  }

  @Test
  void textLpadNull() {
    final TextLpad fn = new TextLpad();
    assertThat(fn.execute(new Object[]{null, 8, "0"}, null)).isNull();
  }

  // ============ Function metadata tests ============

  @Test
  void textFunctionMetadata() {
    final TextIndexOf fn = new TextIndexOf();

    assertThat(fn.getMinArgs()).isEqualTo(2);
    assertThat(fn.getMaxArgs()).isEqualTo(3);
    assertThat(fn.getDescription()).isNotEmpty();
  }

  @Test
  void textLevenshteinDistanceMetadata() {
    final TextLevenshteinDistance fn = new TextLevenshteinDistance();

    assertThat(fn.getMinArgs()).isEqualTo(2);
    assertThat(fn.getMaxArgs()).isEqualTo(2);
    assertThat(fn.getDescription()).contains("Levenshtein");
  }
}
