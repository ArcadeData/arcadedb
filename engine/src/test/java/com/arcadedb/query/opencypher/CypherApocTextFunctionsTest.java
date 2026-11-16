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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code apoc.text.*} functions answer what APOC 5.26 answers for the same arguments.
 */
class CypherApocTextFunctionsTest {

  /**
   * Issue #12104: replace and split take a regular expression, charAt answers the code point and code the character, the
   * Jaro-Winkler distance is a distance, format takes its parameters as one list, and snakeCase, slug, join, hexValue,
   * sorensenDiceSimilarity, camelCase, upperCamelCase, capitalizeAll and decapitalizeAll compute what APOC computes. Every expected
   * value is the answer of Neo4j 5.26 with APOC 5.26.33, except the ones a test names as a deliberate difference: a character
   * outside the BMP stays one character (issue #10485), and an input APOC fails on with an internal exception, or answers NaN for,
   * has an answer.
   */
  @Nested
  class Issue12104ApocTextSemantics extends TestHelper {
    private static final String EMOJI = "\uD83D\uDE00";

    @Test
    void replaceTakesARegularExpression() {
      assertThat(value("apoc.text.replace('hello world', 'o.', '_')")).isEqualTo("hell_w_ld");
      assertThat(value("apoc.text.replace('a1b22c', '[0-9]+', '#')")).isEqualTo("a#b#c");
      assertThat(value("apoc.text.replace($s, $r, 'X')", "s", "  padded  ", "r", "\\s+")).isEqualTo("XpaddedX");
      assertThat(value("apoc.text.replace('hello world', '.', 'X')")).isEqualTo("XXXXXXXXXXX");
      assertThat(value("text.replace('a1b22c', '[0-9]+', '#')")).isEqualTo("a#b#c");
    }

    @Test
    void replaceExpandsGroupReferences() {
      assertThat(value("apoc.text.replace('hello', '(l+)', '<$1>')")).isEqualTo("he<ll>o");
      assertThat(value("apoc.text.replace('abc', '(?<x>b)', '${x}${x}')")).isEqualTo("abbc");
      assertThat(value("apoc.text.replace('abc', 'b', $r)", "r", "\\$")).isEqualTo("a$c");
    }

    @Test
    void replaceWithTheEmptyRegexSurroundsEveryCharacter() {
      assertThat(value("apoc.text.replace('abc', '', '-')")).isEqualTo("-a-b-c-");
      assertThat(value("apoc.text.replace('', '', '-')")).isEqualTo("-");
      // deliberate difference: APOC puts the replacement between the two halves of the emoji too
      assertThat(value("apoc.text.replace($s, '', '-')", "s", "a" + EMOJI + "b")).isEqualTo("-a-" + EMOJI + "-b-");
      assertThat(value("apoc.text.replace($s, '.', '-')", "s", "a" + EMOJI + "b")).isEqualTo("---");
    }

    @Test
    void replaceAnswersNullForANullArgument() {
      assertThat(value("apoc.text.replace(null, 'a', 'b')")).isNull();
      assertThat(value("apoc.text.replace('hello world', null, 'X')")).isNull();
      assertThat(value("apoc.text.replace('hello world', '\\\\s+', null)")).isNull();
    }

    @Test
    void replaceRejectsAnInvalidPatternOrReplacement() {
      assertThatThrownBy(() -> value("apoc.text.replace('hello world', '[', 'X')")).hasStackTraceContaining("Invalid regex pattern");
      assertThatThrownBy(() -> value("apoc.text.replace('hello world', 'l+', '$1')")).hasStackTraceContaining("No group 1");
      assertThatThrownBy(() -> value("apoc.text.replace('abc', 'b', '$')")).hasStackTraceContaining("cannot apply the replacement");
    }

    @Test
    void splitTakesARegularExpression() {
      assertThat(value("apoc.text.split('a1b22c', '[0-9]+')")).isEqualTo(List.of("a", "b", "c"));
      assertThat(value("apoc.text.split('a, b,c', $r)", "r", ",\\s*")).isEqualTo(List.of("a", "b", "c"));
      assertThat(value("apoc.text.split('  padded  ', $r)", "r", "\\s+")).isEqualTo(List.of("", "padded"));
      assertThat(value("apoc.text.split('hello world', 'l+')")).isEqualTo(List.of("he", "o wor", "d"));
      assertThat(value("apoc.text.split('hello world', '.')")).isEqualTo(List.of());
      assertThat(value("text.split('a1b2c', '[0-9]')")).isEqualTo(List.of("a", "b", "c"));
    }

    @Test
    void splitDropsTrailingEmptyPiecesUnlessTheLimitSaysOtherwise() {
      assertThat(value("apoc.text.split('a,b,,c,,', ',')")).isEqualTo(List.of("a", "b", "", "c"));
      assertThat(value("apoc.text.split('a,b,,c,,', ',', -1)")).isEqualTo(List.of("a", "b", "", "c", "", ""));
      assertThat(value("apoc.text.split('a,b,c', ',', 2)")).isEqualTo(List.of("a", "b,c"));
      assertThat(value("apoc.text.split(',a,', ',')")).isEqualTo(List.of("", "a"));
      assertThat(value("apoc.text.split(',a,', ',', -1)")).isEqualTo(List.of("", "a", ""));
      assertThat(value("apoc.text.split(',,', ',')")).isEqualTo(List.of());
      assertThat(value("apoc.text.split('hello world', ' ', 1)")).isEqualTo(List.of("hello world"));
      assertThat(value("apoc.text.split('hello world', '.', 3)")).isEqualTo(List.of("", "", "llo world"));
      assertThat(value("apoc.text.split('hello world', ' ', 9223372036854775807)")).isEqualTo(List.of("hello", "world"));
      assertThat(value("apoc.text.split('hello world', ' ', -9223372036854775808)")).isEqualTo(List.of("hello", "world"));
    }

    @Test
    void splitByTheEmptyRegexSplitsIntoCharacters() {
      assertThat(value("apoc.text.split('abc', '')")).isEqualTo(List.of("a", "b", "c"));
      assertThat(value("apoc.text.split('abc', '', -1)")).isEqualTo(List.of("a", "b", "c", ""));
      assertThat(value("apoc.text.split('abc', '', 2)")).isEqualTo(List.of("a", "bc"));
      assertThat(value("apoc.text.split('abc', '', 5)")).isEqualTo(List.of("a", "b", "c", ""));
      assertThat(value("apoc.text.split('', '')")).isEqualTo(List.of(""));
      assertThat(value("apoc.text.split('', ',', -1)")).isEqualTo(List.of(""));
      // deliberate difference: APOC splits the emoji into its two halves
      assertThat(value("apoc.text.split($s, '')", "s", "a" + EMOJI + "b")).isEqualTo(List.of("a", EMOJI, "b"));
    }

    @Test
    void splitAnswersNullForANullArgument() {
      assertThat(value("apoc.text.split(null, ',')")).isNull();
      assertThat(value("apoc.text.split('hello world', null)")).isNull();
      assertThat(value("apoc.text.split('', null)")).isNull();
      assertThat(value("apoc.text.split('hello world', ',', null)")).isNull();
      assertThatThrownBy(() -> value("apoc.text.split('hello world', '[')")).hasStackTraceContaining("Invalid regex pattern");
    }

    @Test
    void charAtAnswersTheCodePoint() {
      assertThat(value("apoc.text.charAt('hello', 1)")).isEqualTo(101L);
      assertThat(value("apoc.text.charAt('hello world', 0)")).isEqualTo(104L);
      assertThat(value("apoc.text.charAt('ÄÖü', 1)")).isEqualTo(214L);
      assertThat(value("apoc.text.charAt('hello', 5)")).isNull();
      assertThat(value("apoc.text.charAt('hello', -1)")).isNull();
      assertThat(value("apoc.text.charAt('', 0)")).isNull();
      assertThat(value("apoc.text.charAt(null, 0)")).isNull();
      assertThat(value("apoc.text.charAt('hello world', null)")).isNull();
      // deliberate difference: APOC counts UTF-16 units and answers 55357, the first half of the emoji
      assertThat(value("apoc.text.charAt($s, 1)", "s", "a" + EMOJI + "b")).isEqualTo(0x1F600L);
      assertThat(value("apoc.text.charAt($s, 2)", "s", "a" + EMOJI + "b")).isEqualTo(98L);
    }

    @Test
    void codeAnswersTheCharacter() {
      assertThat(value("apoc.text.code(65)")).isEqualTo("A");
      assertThat(value("apoc.text.code(233)")).isEqualTo("\u00e9");
      assertThat(value("apoc.text.code(0)")).isEqualTo("\u0000");
      assertThat(value("apoc.text.code(65535)")).isEqualTo("\uffff");
      assertThat(value("apoc.text.code(-1)")).isNull();
      assertThat(value("apoc.text.code(1114112)")).isNull();
      assertThat(value("apoc.text.code(null)")).isNull();
      // deliberate difference: APOC answers null past U+FFFF; here code() reads back what charAt() answers
      assertThat(value("apoc.text.code(128512)")).isEqualTo(EMOJI);
      assertThat(value("apoc.text.code(apoc.text.charAt($s, 1))", "s", "a" + EMOJI + "b")).isEqualTo(EMOJI);
    }

    @Test
    void jaroWinklerDistanceIsADistance() {
      assertThat(value("apoc.text.jaroWinklerDistance('martha', 'marhta')")).isEqualTo(0.03888888888888886);
      assertThat(value("apoc.text.jaroWinklerDistance('abc', 'abc')")).isEqualTo(0.0);
      assertThat(value("apoc.text.jaroWinklerDistance('', '')")).isEqualTo(0.0);
      assertThat(value("apoc.text.jaroWinklerDistance('', 'a')")).isEqualTo(1.0);
      assertThat(value("apoc.text.jaroWinklerDistance('abc', '')")).isEqualTo(1.0);
      assertThat(value("apoc.text.jaroWinklerDistance('123', 'hello world')")).isEqualTo(1.0);
      assertThat(value("apoc.text.jaroWinklerDistance(null, 'fog')")).isNull();
    }

    @Test
    void jaroWinklerDistanceMatchesCommonsText() {
      assertThat(value("apoc.text.jaroWinklerDistance('dixon', 'dicksonx')")).isEqualTo(0.18666666666666676);
      assertThat(value("apoc.text.jaroWinklerDistance('dicksonx', 'dixon')")).isEqualTo(0.18666666666666676);
      assertThat(value("apoc.text.jaroWinklerDistance('abcd', 'dcba')")).isEqualTo(0.5);
      assertThat(value("apoc.text.jaroWinklerDistance('crate', 'trace')")).isEqualTo(0.2666666666666666);
      assertThat(value("apoc.text.jaroWinklerDistance('frog', 'fog')")).isEqualTo(0.07500000000000007);
      assertThat(value("apoc.text.jaroWinklerDistance('abcdefgh', 'badcfehg')")).isEqualTo(0.16666666666666663);
      assertThat(value("apoc.text.jaroWinklerDistance('Hello World fooBar', 'hello world')")).isEqualTo(0.22727272727272718);
      assertThat(value("apoc.text.jaroWinklerDistance('  padded  ', 'hello world')")).isEqualTo(0.5393939393939393);
      assertThat(value("apoc.text.jaroWinklerDistance('snake_case-and camelCase', 'a.b.c')")).isEqualTo(0.5055555555555555);
    }

    @Test
    void formatTakesItsParametersAsOneList() {
      assertThat(value("apoc.text.format('%s-%d', ['a', 3])")).isEqualTo("a-3");
      assertThat(value("apoc.text.format('%.2f', [3.14159])")).isEqualTo("3.14");
      assertThat(value("apoc.text.format('%b %s', [true, [1, 2]])")).isEqualTo("true [1, 2]");
      assertThat(value("apoc.text.format('x', [])")).isEqualTo("x");
      assertThat(value("apoc.text.format('%s', null)")).isEqualTo("%s");
      assertThat(value("apoc.text.format(null, ['a'])")).isNull();
      assertThatThrownBy(() -> value("apoc.text.format('%s %s', ['a'])")).hasStackTraceContaining("MissingFormatArgumentException");
    }

    @Test
    void formatUsesTheLanguage() {
      assertThat(value("apoc.text.format('%,d', [1234567], 'de')")).isEqualTo("1.234.567");
      assertThat(value("apoc.text.format('%,.2f', [1234567.891], 'fr')")).isEqualTo("1\u202f234\u202f567,89");
      assertThat(value("apoc.text.format('%,d', [1234567])")).isEqualTo("1,234,567");
      assertThat(value("apoc.text.format('%s', ['a'], '')")).isEqualTo("a");
      // deliberate difference: APOC fails with a NullPointerException on a null language
      assertThat(value("apoc.text.format('%,d', [1234567], null)")).isEqualTo("1,234,567");
    }

    @Test
    void snakeCaseJoinsLowerCaseWordsWithADash() {
      final Map<String, String> expected = new HashMap<>();
      expected.put("", "");
      expected.put(" ", "");
      expected.put("fooBarBaz", "foo-bar-baz");
      expected.put("FooBar", "foo-bar");
      expected.put("FOO_BAR", "foo-bar");
      expected.put("FOO", "foo");
      expected.put("FOO BAR", "f-o-o-b-a-r");
      expected.put("FOO-BAR", "f-o-o--b-a-r");
      expected.put("HTTPServer", "h-t-t-p-server");
      expected.put("foo_bar", "foo-_bar");
      expected.put("foo__bar", "foo-_-_bar");
      expected.put("foo-bar", "foo-bar");
      expected.put("foo--bar", "foo--bar");
      expected.put("foo---bar", "foo---bar");
      expected.put("a  b", "a-b");
      expected.put("  padded  ", "padded");
      expected.put("Foo1Bar2", "foo1-bar2");
      expected.put("123", "123");
      expected.put("ABC123", "abc123");
      expected.put("_x_", "_x-_");
      expected.put("-a-", "-a-");
      expected.put("a.b.c", "a-.b-.c");
      expected.put("snake_case-and camelCase", "snake-_case-and-camel-case");
      expected.put("\u00c4\u00d6\u00fc", "\u00e4-\u00f6\u00fc");
      expected.put("\u00c9COLE_NORMALE", "\u00e9cole-normale");
      expected.put("x\ty", "x-y");
      expected.put("\u03a3\u0391\u03a3", "\u03c3\u03b1\u03c2");
      // deliberate difference: APOC starts a word at the second half of the emoji too ("a-?-?b")
      expected.put("a" + EMOJI + "b", "a-" + EMOJI + "b");
      for (final Map.Entry<String, String> entry : expected.entrySet())
        assertThat(value("apoc.text.snakeCase($s)", "s", entry.getKey())).as("snakeCase('%s')", entry.getKey())
            .isEqualTo(entry.getValue());
    }

    @Test
    void slugKeepsLettersAndCase() {
      assertThat(value("apoc.text.slug('Hello World')")).isEqualTo("Hello-World");
      assertThat(value("apoc.text.slug('Hello World!  Foo')")).isEqualTo("Hello-World-Foo");
      assertThat(value("apoc.text.slug('  Hello  ')")).isEqualTo("Hello");
      assertThat(value("apoc.text.slug('')")).isEqualTo("");
      assertThat(value("apoc.text.slug('  ')")).isEqualTo("");
      assertThat(value("apoc.text.slug('a_b-c d')")).isEqualTo("a_b-c-d");
      assertThat(value("apoc.text.slug('snake_case-and camelCase', '.')")).isEqualTo("snake_case.and.camelCase");
      assertThat(value("apoc.text.slug($s)", "s", "\u00c4\u00d6\u00fc \u00f1 \u65e5\u672c\u8a9e " + EMOJI)).isEqualTo(
          "\u00c4\u00d6\u00fc-\u00f1-\u65e5\u672c\u8a9e-");
      assertThat(value("apoc.text.slug($s)", "s", "a" + EMOJI + "b")).isEqualTo("a-b");
      assertThat(value("apoc.text.slug($s)", "s", "a123b \u0663")).isEqualTo("a123b-");
      assertThat(value("apoc.text.slug($s)", "s", "Hello\tWorld\n")).isEqualTo("Hello-World");
      assertThat(value("apoc.text.slug(' -a- ', '_')")).isEqualTo("_a_");
      assertThat(value("apoc.text.slug('Hello World', '')")).isEqualTo("HelloWorld");
      assertThat(value("apoc.text.slug('Hello World', '||')")).isEqualTo("Hello||World");
      assertThat(value("apoc.text.slug('Hello World', null)")).isNull();
      assertThat(value("apoc.text.slug(null)")).isNull();
      // deliberate difference: APOC reads the delimiter as a replacement pattern and fails on '$1'
      assertThat(value("apoc.text.slug('Hello World', '$1')")).isEqualTo("Hello$1World");
    }

    @Test
    void joinWritesANullElementAsNull() {
      assertThat(value("apoc.text.join(['a', null, 'b'], ',')")).isEqualTo("a,null,b");
      assertThat(value("apoc.text.join([null], ',')")).isEqualTo("null");
      assertThat(value("apoc.text.join([], ',')")).isEqualTo("");
      assertThat(value("apoc.text.join(['a'], '')")).isEqualTo("a");
      assertThat(value("apoc.text.join(['a', 'b'], null)")).isNull();
      assertThat(value("apoc.text.join(null, ',')")).isNull();
    }

    @Test
    void hexValueIsUpperCaseAndZeroPadded() {
      assertThat(value("apoc.text.hexValue(255)")).isEqualTo("00FF");
      assertThat(value("apoc.text.hexValue(1)")).isEqualTo("0001");
      assertThat(value("apoc.text.hexValue(0)")).isEqualTo("0000");
      assertThat(value("apoc.text.hexValue(65535)")).isEqualTo("FFFF");
      assertThat(value("apoc.text.hexValue(65536)")).isEqualTo("00010000");
      assertThat(value("apoc.text.hexValue(4294967295)")).isEqualTo("FFFFFFFF");
      assertThat(value("apoc.text.hexValue(4294967296)")).isEqualTo("0000000100000000");
      assertThat(value("apoc.text.hexValue(9223372036854775807)")).isEqualTo("7FFFFFFFFFFFFFFF");
      assertThat(value("apoc.text.hexValue(-1)")).isEqualTo("FFFFFFFF");
      assertThat(value("apoc.text.hexValue(-65536)")).isEqualTo("FFFF0000");
      assertThat(value("apoc.text.hexValue(-2147483648)")).isEqualTo("80000000");
      assertThat(value("apoc.text.hexValue(null)")).isNull();
      // deliberate difference: APOC writes the low int of a number below the int range ("7FFFFFFF", "0000")
      assertThat(value("apoc.text.hexValue(-2147483649)")).isEqualTo("FFFFFFFF7FFFFFFF");
      assertThat(value("apoc.text.hexValue(-9223372036854775808)")).isEqualTo("8000000000000000");
    }

    @Test
    void sorensenDiceSimilarityCountsBigramsAsAMultiset() {
      assertThat(value("apoc.text.sorensenDiceSimilarity('aa', 'aaaa')")).isEqualTo(0.5);
      assertThat(value("apoc.text.sorensenDiceSimilarity('aaa', 'aaaa')")).isEqualTo(0.8);
      assertThat(value("apoc.text.sorensenDiceSimilarity('night', 'nacht')")).isEqualTo(0.25);
      assertThat(value("apoc.text.sorensenDiceSimilarity('Hello World fooBar', 'hello world')")).isEqualTo(0.7619047619047619);
      assertThat(value("apoc.text.sorensenDiceSimilarity('snake_case-and camelCase', 'hello world')")).isEqualTo(
          0.06896551724137931);
      assertThat(value("apoc.text.sorensenDiceSimilarity('ab cd', 'abcd')")).isEqualTo(0.8);
      assertThat(value("apoc.text.sorensenDiceSimilarity('ab', 'AB')")).isEqualTo(1.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity(' ab ', 'ab')")).isEqualTo(1.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity('a', 'a')")).isEqualTo(1.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity('', '')")).isEqualTo(1.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity('a', 'ab')")).isEqualTo(0.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity('', 'ab')")).isEqualTo(0.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity(null, 'ab')")).isNull();
      // deliberate difference: APOC answers NaN (0 / 0) when neither string has a bigram
      assertThat(value("apoc.text.sorensenDiceSimilarity('x', 'y')")).isEqualTo(0.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity('a b', 'c d')")).isEqualTo(0.0);
      // deliberate difference: APOC's bigrams are UTF-16 units and give 0.6666666666666666
      assertThat(value("apoc.text.sorensenDiceSimilarity($a, $b)", "a", "a" + EMOJI + "b", "b", "a" + EMOJI + "c")).isEqualTo(0.5);
    }

    @Test
    void sorensenDiceSimilarityUpperCasesInTheLanguage() {
      assertThat(value("apoc.text.sorensenDiceSimilarity('straße', 'STRASSE')")).isEqualTo(1.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity('İstanbul', 'istanbul')")).isEqualTo(0.8571428571428571);
      assertThat(value("apoc.text.sorensenDiceSimilarity('İstanbul', 'istanbul', 'tr')")).isEqualTo(1.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity('istanbul', 'ISTANBUL', 'tr')")).isEqualTo(0.8571428571428571);
      assertThat(value("apoc.text.sorensenDiceSimilarity('hello world', 'hello world', 'de')")).isEqualTo(1.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity('ab', 'ac', 'zz-ZZ')")).isEqualTo(0.0);
      assertThat(value("apoc.text.sorensenDiceSimilarity('ab', 'ab', null)")).isNull();
    }

    @Test
    void camelCaseIgnoresTheBlanksAroundTheWords() {
      final Map<String, String> expected = new HashMap<>();
      expected.put("  padded  ", "padded");
      expected.put("_a_", "a");
      expected.put("hello world", "helloWorld");
      expected.put("Hello World fooBar", "helloWorldFoobar");
      expected.put("FOO_BAR", "fooBar");
      expected.put("foo-bar_baz qux", "fooBarBazQux");
      expected.put("1abc def", "1abcDef");
      expected.put("\u00c4\u00d6\u00fc \u00f1 \u65e5\u672c\u8a9e", "\u00e4\u00f6\u00fc\u00d1\u65e5\u672c\u8a9e");
      expected.put("a" + EMOJI + "b", "aB");
      expected.put("ab\u0663cd", "abCd");
      expected.put("\u03a3\u0391\u03a3 \u03a3\u0391\u03a3", "\u03c3\u03b1\u03c2\u03a3\u03b1\u03c2");
      expected.put("\u01c4emal", "\u01c6emal");
      expected.put("x \u01c6emal", "x\u01c5emal");
      expected.put("x \u00df", "x\u00df");
      // deliberate difference: APOC fails with a StringIndexOutOfBoundsException when there is no word
      expected.put("", "");
      expected.put(" ", "");
      for (final Map.Entry<String, String> entry : expected.entrySet())
        assertThat(value("apoc.text.camelCase($s)", "s", entry.getKey())).as("camelCase('%s')", entry.getKey())
            .isEqualTo(entry.getValue());
    }

    @Test
    void upperCamelCaseUpperCasesTheFirstCharacterOfCamelCase() {
      assertThat(value("apoc.text.upperCamelCase('  padded  ')")).isEqualTo("Padded");
      assertThat(value("apoc.text.upperCamelCase('hello world')")).isEqualTo("HelloWorld");
      assertThat(value("apoc.text.upperCamelCase('\u00df x')")).isEqualTo("SSX");
      assertThat(value("apoc.text.upperCamelCase('\u01c6emal')")).isEqualTo("\u01c4emal");
      assertThat(value("apoc.text.upperCamelCase('1abc')")).isEqualTo("1abc");
      // deliberate difference: APOC fails with a StringIndexOutOfBoundsException
      assertThat(value("apoc.text.upperCamelCase('')")).isEqualTo("");
    }

    @Test
    void capitalizeAllSplitsAtSpacesAndTrims() {
      assertThat(value("apoc.text.capitalizeAll('  padded  ')")).isEqualTo("Padded");
      assertThat(value("apoc.text.capitalizeAll('hello world')")).isEqualTo("Hello World");
      assertThat(value("apoc.text.capitalizeAll('a  b')")).isEqualTo("A  B");
      assertThat(value("apoc.text.capitalizeAll($s)", "s", "a\tb c")).isEqualTo("A\tb C");
      assertThat(value("apoc.text.capitalizeAll($s)", "s", "\ta b")).isEqualTo("a B");
      assertThat(value("apoc.text.capitalizeAll($s)", "s", "a b\n")).isEqualTo("A B");
      assertThat(value("apoc.text.capitalizeAll('\u01c6emal x')")).isEqualTo("\u01c5emal X");
      assertThat(value("apoc.text.capitalizeAll('\u00df x')")).isEqualTo("\u00df X");
      assertThat(value("apoc.text.capitalizeAll('')")).isEqualTo("");
      assertThat(value("apoc.text.capitalizeAll(' ')")).isEqualTo("");
    }

    @Test
    void decapitalizeAllSplitsAtSpacesAndTrims() {
      assertThat(value("apoc.text.decapitalizeAll('  Padded  ')")).isEqualTo("padded");
      assertThat(value("apoc.text.decapitalizeAll('A  B')")).isEqualTo("a  b");
      assertThat(value("apoc.text.decapitalizeAll($s)", "s", "A\tB C")).isEqualTo("a\tB c");
      assertThat(value("apoc.text.decapitalizeAll('')")).isEqualTo("");
    }

    @Test
    void sqlCallsAnswerTheSame() {
      try (final ResultSet rs = database.query("sql",
          "SELECT text.split('a1b22c', '[0-9]+') AS s, text.charAt('hello', 1) AS c, text.code(65) AS k, "
              + "text.jaroWinklerDistance('abc', 'abc') AS j, text.format('%s-%d', ['a', 3]) AS f")) {
        final var row = rs.next();
        assertThat(row.<List<String>>getProperty("s")).isEqualTo(List.of("a", "b", "c"));
        assertThat(row.<Object>getProperty("c")).isEqualTo(101L);
        assertThat(row.<Object>getProperty("k")).isEqualTo("A");
        assertThat(row.<Object>getProperty("j")).isEqualTo(0.0);
        assertThat(row.<Object>getProperty("f")).isEqualTo("a-3");
      }
    }

    private Object value(final String expression, final Object... parameters) {
      final Map<String, Object> params = new HashMap<>();
      for (int i = 0; i < parameters.length; i += 2)
        params.put((String) parameters[i], parameters[i + 1]);
      try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS r", params)) {
        final Object value = rs.next().getProperty("r");
        return value instanceof Object[] array ? Arrays.asList(array) : value;
      }
    }
  }
}
