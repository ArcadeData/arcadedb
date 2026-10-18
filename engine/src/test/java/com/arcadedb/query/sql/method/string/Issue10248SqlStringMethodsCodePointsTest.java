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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10248: the SQL string methods counted and cut UTF-16 code units, so for a string holding U+1F600 (outside the BMP) followed
 * by {@code a}, {@code length()} was 3, {@code left(1)} / {@code substring(0, 1)} returned the lone high surrogate (which then
 * compared equal to {@code '?'}), and {@code indexOf('a')} was 2. They count and cut Unicode characters now, as openCypher does
 * since #8998.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10248SqlStringMethodsCodePointsTest extends TestHelper {
  private static final String EMOJI = "😀";
  private static final String VALUE = EMOJI + "a";

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.transaction(() -> database.newVertex("T").set("s", VALUE).save());
  }

  @Test
  void methodsCountAndCutCharacters() {
    assertThat(select("s.length()")).isEqualTo(2);
    assertThat(select("s.left(1)")).isEqualTo(EMOJI);
    assertThat(select("s.right(1)")).isEqualTo("a");
    assertThat(select("s.right(2)")).isEqualTo(VALUE);
    assertThat(select("s.substring(0, 1)")).isEqualTo(EMOJI);
    assertThat(select("s.substring(1)")).isEqualTo("a");
    assertThat(select("s.substring(1, 2)")).isEqualTo("a");
    assertThat(select("s.substring(0, 2)")).isEqualTo(VALUE);
    assertThat(select("s.substring(2)")).isEqualTo("");
    assertThat(select("s.indexOf('a')")).isEqualTo(1);
    assertThat(select("s.indexOf('a', 1)")).isEqualTo(1);
    assertThat(select("s.indexOf('a', 2)")).isEqualTo(-1);
    assertThat(select("s.lastIndexOf('a')")).isEqualTo(1);
    assertThat(select("s.charAt(0)")).isEqualTo(EMOJI);
    assertThat(select("s.charAt(1)")).isEqualTo("a");
    assertThat(select("s.charAt(2)")).isNull();
  }

  @Test
  void aHalfOfAPairIsNeverReturnedSoItDoesNotMatchTheReplacementCharacter() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS v FROM T WHERE s.substring(0, 1) = '?'")) {
      assertThat(rs.next().<Long>getProperty("v")).isZero();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS v FROM T WHERE s.left(1) = '?'")) {
      assertThat(rs.next().<Long>getProperty("v")).isZero();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS v FROM T WHERE s.left(1) = '" + EMOJI + "'")) {
      assertThat(rs.next().<Long>getProperty("v")).isEqualTo(1L);
    }
  }

  @Test
  void agreesWithOpenCypher() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) RETURN size(n.s) AS len, left(n.s, 1) AS l, right(n.s, 1) AS r, substring(n.s, 1) AS sub")) {
      final Result row = rs.next();
      assertThat(select("s.length()")).isEqualTo(row.<Number>getProperty("len").intValue());
      assertThat(select("s.left(1)")).isEqualTo(row.<String>getProperty("l"));
      assertThat(select("s.right(1)")).isEqualTo(row.<String>getProperty("r"));
      assertThat(select("s.substring(1)")).isEqualTo(row.<String>getProperty("sub"));
    }
  }

  @Test
  void aSurrogateThatIsNotPartOfAPairStaysAUnit() {
    // a lone surrogate is not valid text but is a value a String can hold; it counts as one unit, never swallowing its neighbour
    final String lone = "x\uD83Dy";
    assertThat(literal(lone, "length()")).isEqualTo(3);
    assertThat(literal(lone, "substring(1, 2)")).isEqualTo("\uD83D");
    assertThat(literal(lone, "indexOf('y')")).isEqualTo(2);
  }

  @Test
  void searchesNeverMatchInsideAPair() {
    // the low surrogate of the emoji is not a character on its own: it must not be found in the middle of the pair
    final String needle = "\uDE00";
    assertThat(literal(VALUE + needle, "indexOf('" + needle + "')")).isEqualTo(2);
    assertThat(literal(VALUE + needle, "lastIndexOf('" + needle + "')")).isEqualTo(2);
    assertThat(literal(VALUE, "indexOf('" + needle + "')")).isEqualTo(-1);
    assertThat(literal(VALUE, "lastIndexOf('" + needle + "')")).isEqualTo(-1);

    // nor the high surrogate alone, which would end inside the pair, or a needle that ends with it
    final String high = "\uD83D";
    assertThat(literal(VALUE, "indexOf('" + high + "')")).isEqualTo(-1);
    assertThat(literal(VALUE, "lastIndexOf('" + high + "')")).isEqualTo(-1);
    assertThat(literal("a" + EMOJI, "indexOf('a" + high + "')")).isEqualTo(-1);
    assertThat(literal("a" + EMOJI, "lastIndexOf('a" + high + "')")).isEqualTo(-1);
    assertThat(literal("a" + EMOJI + "a" + high, "indexOf('a" + high + "')")).isEqualTo(2);
    assertThat(literal("a" + EMOJI + "a" + high, "lastIndexOf('a" + high + "')")).isEqualTo(2);
  }

  @Test
  void charactersInTheBasicPlaneKeepTheirBehavior() {
    assertThat(literal("foobar", "length()")).isEqualTo(6);
    assertThat(literal("foobar", "left(3)")).isEqualTo("foo");
    assertThat(literal("foobar", "left(30)")).isEqualTo("foobar");
    assertThat(literal("foobar", "left(-1)")).isEqualTo("");
    assertThat(literal("foobar", "right(3)")).isEqualTo("bar");
    assertThat(literal("foobar", "right(30)")).isEqualTo("foobar");
    assertThat(literal("foobar", "right(-1)")).isEqualTo("");
    assertThat(literal("foobar", "substring(1, 3)")).isEqualTo("oo");
    assertThat(literal("foobar", "substring(-4, 3)")).isEqualTo("foo");
    assertThat(literal("foobar", "substring(4, 3)")).isEqualTo("");
    assertThat(literal("foobar", "substring(3)")).isEqualTo("bar");
    assertThat(literal("foobar", "substring(6)")).isEqualTo("");
    assertThat(literal("foobar", "indexOf('o')")).isEqualTo(1);
    assertThat(literal("foobar", "indexOf('o', 2)")).isEqualTo(2);
    assertThat(literal("foobar", "indexOf('o', -5)")).isEqualTo(1);
    assertThat(literal("foobar", "indexOf('z')")).isEqualTo(-1);
    assertThat(literal("foobar", "indexOf('')")).isEqualTo(0);
    assertThat(literal("foobar", "indexOf('', 3)")).isEqualTo(3);
    assertThat(literal("foobar", "indexOf('', 30)")).isEqualTo(6);
    assertThat(literal("foobar", "lastIndexOf('o')")).isEqualTo(2);
    assertThat(literal("foobar", "lastIndexOf('o', 1)")).isEqualTo(1);
    assertThat(literal("foobar", "lastIndexOf('o', -1)")).isEqualTo(-1);
    assertThat(literal("foobar", "lastIndexOf('o', 30)")).isEqualTo(2);
    assertThat(literal("foobar", "lastIndexOf('')")).isEqualTo(6);
    assertThat(literal("foobar", "lastIndexOf('', 3)")).isEqualTo(3);
    assertThat(literal("foobar", "lastIndexOf('', 30)")).isEqualTo(6);
    assertThat(literal("foobar", "lastIndexOf('', -1)")).isEqualTo(-1);
    assertThat(literal("foobar", "lastIndexOf('o', 2)")).isEqualTo(2);
    assertThat(literal("foobar", "lastIndexOf('o', 1000000)")).isEqualTo(2);
    assertThat(literal("foobar", "lastIndexOf('o', -1000000)")).isEqualTo(-1);
    assertThat(literal("foobar", "indexOf('o', 1000000)")).isEqualTo(-1);
    assertThat(literal("foobar", "charAt(3)")).isEqualTo("b");
    assertThat(literal("foobar", "charAt(6)")).isNull();
    assertThat(literal("foobar", "charAt(-1)")).isNull();
  }

  @Test
  void aNullSearchStringAnswersNull() {
    try (final ResultSet rs = database.query("sql",
        "SELECT 'foobar'.indexOf(null) AS i, 'foobar'.lastIndexOf(null) AS l")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("i")).isNull();
      assertThat(row.<Object>getProperty("l")).isNull();
    }
  }

  @Test
  void aPositionRightAfterAPairIsNotInsideIt() {
    // the unit before the position is the high half of a pair whose low half is at the position: the position is past the pair
    final String text = "a" + EMOJI + "b";
    assertThat(literal(text, "indexOf('b', 2)")).isEqualTo(2);
    assertThat(literal(text, "indexOf('b', 1)")).isEqualTo(2);
    assertThat(literal(text, "lastIndexOf('b', 2)")).isEqualTo(2);
    assertThat(literal(text, "lastIndexOf('" + EMOJI + "', 1)")).isEqualTo(1);
    assertThat(literal(text, "lastIndexOf('a', 1)")).isEqualTo(0);
    assertThat(literal(text, "charAt(2)")).isEqualTo("b");
    assertThat(literal(text, "charAt(1)")).isEqualTo(EMOJI);
  }

  @Test
  void anEmptySearchStringIsFoundAtTheBoundariesOnlyAndNeverInsideAPair() {
    final String text = "a" + EMOJI;
    assertThat(literal(text, "indexOf('')")).isEqualTo(0);
    assertThat(literal(text, "indexOf('', 1)")).isEqualTo(1);
    assertThat(literal(text, "indexOf('', 2)")).isEqualTo(2);
    assertThat(literal(text, "indexOf('', 5)")).isEqualTo(2);
    assertThat(literal(text, "lastIndexOf('')")).isEqualTo(2);
    assertThat(literal(text, "lastIndexOf('', 1)")).isEqualTo(1);
    assertThat(literal(text, "lastIndexOf('', 5)")).isEqualTo(2);
    assertThat(literal(text, "lastIndexOf('', -1)")).isEqualTo(-1);
  }

  @Test
  void textAfterSeveralPairsIsIndexedByCharacter() {
    final String text = "a" + EMOJI + "b" + EMOJI + EMOJI + "c";
    assertThat(literal(text, "length()")).isEqualTo(6);
    assertThat(literal(text, "left(3)")).isEqualTo("a" + EMOJI + "b");
    assertThat(literal(text, "right(3)")).isEqualTo(EMOJI + EMOJI + "c");
    assertThat(literal(text, "substring(2, 5)")).isEqualTo("b" + EMOJI + EMOJI);
    assertThat(literal(text, "indexOf('c')")).isEqualTo(5);
    assertThat(literal(text, "indexOf('" + EMOJI + "', 2)")).isEqualTo(3);
    assertThat(literal(text, "lastIndexOf('" + EMOJI + "')")).isEqualTo(4);
    assertThat(literal(text, "lastIndexOf('" + EMOJI + "', 3)")).isEqualTo(3);
    assertThat(literal(text, "lastIndexOf('" + EMOJI + "', 2)")).isEqualTo(1);
    assertThat(literal(text, "charAt(4)")).isEqualTo(EMOJI);
  }

  private Object select(final String expression) {
    try (final ResultSet rs = database.query("sql", "SELECT " + expression + " AS v FROM T")) {
      return rs.next().getProperty("v");
    }
  }

  private Object literal(final String value, final String call) {
    try (final ResultSet rs = database.query("sql", "SELECT :v." + call + " AS v", Map.of("v", value))) {
      return rs.next().getProperty("v");
    }
  }
}
