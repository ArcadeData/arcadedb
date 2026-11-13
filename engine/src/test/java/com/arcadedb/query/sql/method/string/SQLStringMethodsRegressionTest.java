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
import com.arcadedb.function.text.SplitFunction;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL string methods: NULL inputs, Unicode code points, split() and replace(). */
class SQLStringMethodsRegressionTest {
  /** Regression for #10193: s.replace() and s.length() on a NULL value answer NULL, not the first argument or 0. */
  @Nested
  class NullInputTest extends TestHelper {

    @Test
    void updateWithReplaceAndLengthOnNullKeepsNull() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.id LONG");
      database.command("sql", "CREATE PROPERTY T.c INTEGER");
      database.command("sql", "CREATE PROPERTY T.s STRING");
      database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 1"));

      database.transaction(() -> database.command("sql", "UPDATE T SET s = s.replace('a', 'b')"));
      database.transaction(() -> database.command("sql", "UPDATE T SET c = s.length()"));

      try (final ResultSet rs = database.query("sql", "SELECT s, c FROM T WHERE id = 1")) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("s")).isNull();
        assertThat(row.<Object>getProperty("c")).isNull();
      }
    }

    @Test
    void aNullArgumentAnswersNull() {
      try (final ResultSet rs = database.query("sql",
          "SELECT 'abc'.replace(null, 'b') AS r1, 'abc'.replace('a', null) AS r2, 'abc'.replace('a', 'b') AS r3, "
              + "'abc'.length() AS l1, ''.length() AS l2")) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("r1")).isNull();
        assertThat(row.<Object>getProperty("r2")).isNull();
        assertThat(row.<String>getProperty("r3")).isEqualTo("bbc");
        assertThat(row.<Integer>getProperty("l1")).isEqualTo(3);
        assertThat(row.<Integer>getProperty("l2")).isZero();
      }
    }

    /** NULL compares as unknown: a NULL string is neither empty nor non-empty by its length. */
    @Test
    void aNullLengthMatchesNoComparison() {
      database.command("sql", "CREATE DOCUMENT TYPE W");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO W SET id = 1");
        database.command("sql", "INSERT INTO W SET id = 2, s = ''");
        database.command("sql", "INSERT INTO W SET id = 3, s = 'abc'");
      });

      assertThat(ids("SELECT id FROM W WHERE s.length() = 0")).containsExactly(2);
      assertThat(ids("SELECT id FROM W WHERE s.length() > 0")).containsExactly(3);
      assertThat(ids("SELECT id FROM W WHERE s.length() IS NULL")).containsExactly(1);
      assertThat(ids("SELECT id FROM W WHERE ifnull(s.length(), 0) = 0 ORDER BY id")).containsExactly(1, 2);
    }

    private List<Integer> ids(final String sql) {
      final List<Integer> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext())
          ids.add(rs.next().<Number>getProperty("id").intValue());
      }
      return ids;
    }

    @Test
    void methodsOnAMissingPropertyAreNull() {
      database.command("sql", "CREATE DOCUMENT TYPE U");
      database.transaction(() -> database.command("sql", "INSERT INTO U SET id = 1"));

      try (final ResultSet rs = database.query("sql", "SELECT missing.replace('a', 'b') AS r, missing.length() AS l FROM U")) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("r")).isNull();
        assertThat(row.<Object>getProperty("l")).isNull();
      }
    }
  }

  /** Regression for #10248: the SQL string methods count and cut Unicode characters, not UTF-16 code units. */
  @Nested
  class CodePointsTest extends TestHelper {
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

  /** Regression for #10485: split() and replace() never cut surrogate pairs and split() keeps trailing empty pieces, as openCypher does. */
  @Nested
  class SplitReplaceCodePointsTest extends TestHelper {
    private static final String P = new String(Character.toChars(0x1F600));
    private static final String Q = new String(Character.toChars(0x1F601));

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE VERTEX TYPE T");
      database.transaction(() -> database.newVertex("T").set("s", "a" + P + "b").save());
    }

    @Test
    void splitWithAnEmptyDelimiterReturnsTheCharacters() {
      assertThat(sql("SELECT :s.split('') AS v", "s", "a" + P)).isEqualTo(List.of("a", P));
      assertThat(sql("SELECT s.split('') AS v FROM T")).isEqualTo(List.of("a", P, "b"));
      assertThat(sql("SELECT text.split(:s, '') AS v", "s", "a" + P)).isEqualTo(List.of("a", P));
    }

    @Test
    void splitKeepsTheEmptyPiecesAtTheEnd() {
      assertThat(sql("SELECT 'a,'.split(',') AS v")).isEqualTo(List.of("a", ""));
      assertThat(sql("SELECT ','.split(',') AS v")).isEqualTo(List.of("", ""));
      assertThat(sql("SELECT 'a,,b,,'.split(',') AS v")).isEqualTo(List.of("a", "", "b", "", ""));
      assertThat(sql("SELECT ',a'.split(',') AS v")).isEqualTo(List.of("", "a"));
      assertThat(sql("SELECT ''.split(',') AS v")).isEqualTo(List.of(""));
      assertThat(sql("SELECT 'abc'.split(',') AS v")).isEqualTo(List.of("abc"));
    }

    @Test
    void splitByAnEmojiKeepsTheEmptyTrailingPieceLikeOpenCypher() {
      assertThat(sql("SELECT :s.split(:c) AS v", "s", "a" + P, "c", P)).isEqualTo(List.of("a", ""));
      assertThat(sql("SELECT text.split(:s, :c) AS v", "s", "a" + P, "c", P)).isEqualTo(List.of("a", ""));
      assertThat(cypher("RETURN split($s, $c) AS v", "s", "a" + P, "c", P)).isEqualTo(List.of("a", ""));
    }

    @Test
    void aLoneSurrogateDelimiterNeverMatchesInsideAPair() {
      assertThat(sql("SELECT :s.split(:c) AS v", "s", "a" + P + "b", "c", "\uDE00")).isEqualTo(List.of("a" + P + "b"));
      assertThat(sql("SELECT :s.split(:c) AS v", "s", "a" + P + "b", "c", "\uD83D")).isEqualTo(List.of("a" + P + "b"));
      assertThat(sql("SELECT :s.split(:c) AS v", "s", "a" + P + "\uDE00b", "c", "\uDE00")).isEqualTo(List.of("a" + P, "b"));
    }

    /** A lone high surrogate right before a pair: the occurrence that starts inside the pair is skipped, the one before it is not. */
    @Test
    void aLoneSurrogateRightBeforeAPairMatchesOnlyItself() {
      final String s = "\uD83D" + P;
      assertThat(sql("SELECT :s.split(:c) AS v", "s", s, "c", "\uD83D")).isEqualTo(List.of("", P));
      assertThat(sql("SELECT :s.replace(:c, '-') AS v", "s", s, "c", "\uD83D")).isEqualTo("-" + P);
      assertThat(sql("SELECT :s.split(:c) AS v", "s", "\uD83D\uD83D" + P, "c", "\uD83D\uD83D")).isEqualTo(List.of("", P));
    }

    @Test
    void splitTreatsTheDelimiterAsALiteralAndStaysTheListItWas() {
      assertThat(sql("SELECT 'a.b.c'.split('.') AS v")).isEqualTo(List.of("a", "b", "c"));
      assertThat(sql("SELECT 'a|b'.split('|') AS v")).isEqualTo(List.of("a", "b"));
      assertThat(sql("SELECT 'a,b'.split(',') AS v")).isInstanceOf(List.class);
    }

    @Test
    void replaceWithAnEmptySearchInsertsBetweenCharactersOnly() {
      assertThat(sql("SELECT :s.replace('', '-') AS v", "s", "a" + P)).isEqualTo("-a-" + P + "-");
      assertThat(sql("SELECT s.replace('', '-') AS v FROM T")).isEqualTo("-a-" + P + "-b-");
      assertThat(sql("SELECT :s.replace('', '-') AS v", "s", "")).isEqualTo("-");
      assertThat(sql("SELECT 'abc'.replace('', '-') AS v")).isEqualTo("-a-b-c-");
    }

    @Test
    void replaceNeverMatchesInsideAPair() {
      assertThat(sql("SELECT :s.replace(:c, '-') AS v", "s", "x" + P, "c", "\uDE00")).isEqualTo("x" + P);
      assertThat(sql("SELECT :s.replace(:c, '-') AS v", "s", "x" + P, "c", "\uD83D")).isEqualTo("x" + P);
      assertThat(sql("SELECT :s.replace(:c, '-') AS v", "s", "x" + P + "\uDE00", "c", "\uDE00")).isEqualTo("x" + P + "-");
      assertThat(sql("SELECT :s.replace(:c, '-') AS v", "s", P + Q + P, "c", Q)).isEqualTo(P + "-" + P);
    }

    @Test
    void replaceOfBasicPlaneTextKeepsItsBehavior() {
      assertThat(sql("SELECT 'hello world'.replace('o', '0') AS v")).isEqualTo("hell0 w0rld");
      assertThat(sql("SELECT 'aaa'.replace('a', 'bb') AS v")).isEqualTo("bbbbbb");
      assertThat(sql("SELECT 'aaa'.replace('aa', 'b') AS v")).isEqualTo("ba");
      assertThat(sql("SELECT 'abc'.replace('x', 'y') AS v")).isEqualTo("abc");
      assertThat(sql("SELECT 'abc'.replace('b', '') AS v")).isEqualTo("ac");
    }

    /** The two languages, and the function and the method of each, answer one way. */
    @Test
    void sqlAndOpenCypherAgree() {
      for (final String s : new String[] { "", "a", "a,", ",a", "a" + P, P + P, "a" + P + "b" + Q + "c" }) {
        for (final String d : new String[] { "", ",", P, "b" }) {
          final Object cypher = cypher("RETURN split($s, $d) AS v", "s", s, "d", d);
          assertThat(sql("SELECT :s.split(:d) AS v", "s", s, "d", d)).as("method split '%s' by '%s'", s, d).isEqualTo(cypher);
          assertThat(sql("SELECT text.split(:s, :d) AS v", "s", s, "d", d)).as("text.split '%s' by '%s'", s, d).isEqualTo(cypher);
          assertThat(new SplitFunction().execute(new Object[] { s, d }, null)).as("split() '%s' by '%s'", s, d).isEqualTo(cypher);
          assertThat(sql("SELECT :s.replace(:d, '-') AS v", "s", s, "d", d)).as("method replace '%s' by '%s'", s, d)
              .isEqualTo(cypher("RETURN replace($s, $d, '-') AS v", "s", s, "d", d));
          assertThat(sql("SELECT text.replace(:s, :d, '-') AS v", "s", s, "d", d)).as("text.replace '%s' by '%s'", s, d)
              .isEqualTo(cypher("RETURN replace($s, $d, '-') AS v", "s", s, "d", d));
        }
      }
    }

    private Object sql(final String query, final Object... keyValues) {
      try (final ResultSet rs = database.query("sql", query, params(keyValues))) {
        return rs.next().getProperty("v");
      }
    }

    private Object cypher(final String query, final Object... keyValues) {
      try (final ResultSet rs = database.query("opencypher", query, params(keyValues))) {
        return rs.next().getProperty("v");
      }
    }

    private static Map<String, Object> params(final Object... keyValues) {
      final Map<String, Object> map = new HashMap<>();
      for (int i = 0; i < keyValues.length; i += 2)
        map.put((String) keyValues[i], keyValues[i + 1]);
      return map;
    }
  }
}
