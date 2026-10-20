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

import com.arcadedb.TestHelper;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10378: {@code text.charAt()}, {@code text.indexOf()}, {@code text.lpad()}, {@code text.rpad()} and the SQL
 * {@code size()} method still counted UTF-16 code units after #10248 moved their neighbours to Unicode characters, so
 * {@code text.charAt('a😀b', 1)} answered half a surrogate pair, {@code text.indexOf('a😀b', 'b')} answered 3 for a string whose
 * {@code size()} is 3, and {@code text.lpad} measured an emoji as two characters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10378TextFunctionsCodePointsTest extends TestHelper {
  private static final String EMOJI = "😀";
  private static final String VALUE = "a" + EMOJI + "b";

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> database.newDocument("T").set("s", VALUE).save());
  }

  @Test
  void charAtReturnsTheWholeCharacter() {
    assertThat(both("text.charAt('" + VALUE + "', 0)")).isEqualTo("a");
    assertThat(both("text.charAt('" + VALUE + "', 1)")).isEqualTo(EMOJI);
    assertThat(both("text.charAt('" + VALUE + "', 2)")).isEqualTo("b");
    assertThat(both("text.charAt('" + VALUE + "', 3)")).isNull();
    assertThat(both("text.charAt('" + VALUE + "', -1)")).isNull();
  }

  @Test
  void charAtAgreesWithTheCharAtMethod() {
    try (final ResultSet rs = database.query("sql", "SELECT text.charAt(s, 1) AS f, s.charAt(1) AS m FROM T")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("f")).isEqualTo(EMOJI).isEqualTo(row.<String>getProperty("m"));
    }
  }

  @Test
  void indexOfIsAPositionInTheStringTheEngineReportsAndNeverPastItsSize() {
    assertThat(both("text.indexOf('" + VALUE + "', 'b')")).isEqualTo(2L);
    assertThat(both("text.indexOf('" + VALUE + "', '" + EMOJI + "')")).isEqualTo(1L);
    assertThat(both("text.indexOf('" + VALUE + "', 'b', 2)")).isEqualTo(2L);
    assertThat(both("text.indexOf('" + VALUE + "', 'b', 3)")).isEqualTo(-1L);
    assertThat(both("text.indexOf('" + VALUE + "', 'z')")).isEqualTo(-1L);
    assertThat(both("text.indexOf('" + VALUE + "', 'a', -4)")).isEqualTo(0L);
  }

  @Test
  void indexOfNeverMatchesInsideAPair() {
    assertThat(both("text.indexOf('" + VALUE + "', '\uDE00')")).isEqualTo(-1L);
    assertThat(both("text.indexOf('" + VALUE + "', '\uD83D')")).isEqualTo(-1L);
  }

  @Test
  void indexOfAgreesWithTheIndexOfMethod() {
    try (final ResultSet rs = database.query("sql", "SELECT text.indexOf(s, 'b') AS f, s.indexOf('b') AS m FROM T")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("f").longValue()).isEqualTo(2L).isEqualTo(row.<Number>getProperty("m").longValue());
    }
  }

  @Test
  void padsMeasureCharactersNotUtf16Units() {
    // "a😀b" is three characters: a width of 3 needs no padding, a width of 4 needs one pad character
    assertThat(both("text.lpad('" + VALUE + "', 3, '.')")).isEqualTo(VALUE);
    assertThat(both("text.rpad('" + VALUE + "', 3, '.')")).isEqualTo(VALUE);
    assertThat(both("text.lpad('" + VALUE + "', 4, '.')")).isEqualTo("." + VALUE);
    assertThat(both("text.rpad('" + VALUE + "', 4, '.')")).isEqualTo(VALUE + ".");
    assertThat(both("text.lpad('" + VALUE + "', 6, '.')")).isEqualTo("..." + VALUE);
    assertThat(both("text.rpad('" + VALUE + "', 6, '.')")).isEqualTo(VALUE + "...");
  }

  @Test
  void aPadCharacterOutsideTheBasicPlaneIsWholeAndCounted() {
    assertThat(both("text.lpad('a', 3, '" + EMOJI + "')")).isEqualTo(EMOJI + EMOJI + "a");
    assertThat(both("text.rpad('a', 3, '" + EMOJI + "')")).isEqualTo("a" + EMOJI + EMOJI);
    // a string of emoji is as wide as it has emoji
    assertThat(both("text.lpad('" + EMOJI + EMOJI + "', 2, '.')")).isEqualTo(EMOJI + EMOJI);
    assertThat(both("text.lpad('" + EMOJI + EMOJI + "', 3, '.')")).isEqualTo("." + EMOJI + EMOJI);
  }

  @Test
  void onlyTheFirstCharacterOfAPadStringIsUsed() {
    assertThat(both("text.lpad('a', 3, 'xy')")).isEqualTo("xxa");
    assertThat(both("text.rpad('a', 3, 'xy')")).isEqualTo("axx");
    // the first character of the pad is the emoji, whole, and not its high half, nor the character after it
    assertThat(both("text.lpad('a', 3, '" + EMOJI + "x')")).isEqualTo(EMOJI + EMOJI + "a");
    assertThat(both("text.rpad('a', 3, '" + EMOJI + "x')")).isEqualTo("a" + EMOJI + EMOJI);
  }

  @Test
  void padsOfPlainTextKeepTheirBehavior() {
    assertThat(both("text.lpad('abc', 5, '*')")).isEqualTo("**abc");
    assertThat(both("text.rpad('abc', 5, '*')")).isEqualTo("abc**");
    assertThat(both("text.lpad('abc', 2, '*')")).isEqualTo("abc");
    assertThat(both("text.lpad('abc', 5, '')")).isEqualTo("  abc");
    assertThat(both("text.rpad('abc', 5, '')")).isEqualTo("abc  ");
  }

  @Test
  void sizeOfAStringCountsCharactersLikeLength() {
    try (final ResultSet rs = database.query("sql", "SELECT s.size() AS sz, s.length() AS len FROM T")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("sz").intValue()).isEqualTo(3).isEqualTo(row.<Number>getProperty("len").intValue());
    }
  }

  @Test
  void sizeOfACollectionIsUnchanged() {
    try (final ResultSet rs = database.query("sql", "SELECT [1,2,3].size() AS a, {'a':1}.size() AS b")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(3);
      assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(1);
    }
  }

  /** The functions of the registry are enumerated, so a text function added later cannot keep counting units unnoticed. */
  @Test
  void noTextFunctionHandsBackHalfOfAPair() {
    final Collection<StatelessFunction> functions = CypherFunctionRegistry.getAllFunctions();
    final List<String> checked = new ArrayList<>();
    for (final StatelessFunction function : functions) {
      if (!function.getName().startsWith("text."))
        continue;
      for (final Object[] args : argumentsFor(function)) {
        final Object result;
        try {
          result = function.execute(args, null);
        } catch (final RuntimeException e) {
          // the combination is not one the function takes (a pattern that does not compile, an argument of another type)
          continue;
        }
        checked.add(function.getName());
        assertNoLoneSurrogate(function.getName(), result);
      }
    }
    assertThat(checked).contains("text.charAt", "text.lpad", "text.rpad", "text.indexOf");
  }

  private static List<Object[]> argumentsFor(final StatelessFunction function) {
    final List<Object[]> out = new ArrayList<>();
    final int min = function.getMinArgs();
    final int max = Math.min(function.getMaxArgs(), 4);
    // the string under test first, then the usual companions of a string function: an index, a width, a one character pad
    final Object[][] companions = { { 1 }, { 1, 2 }, { 4, "." }, { "b" }, { "b", 1 }, { "b", "c" }, { 2, EMOJI } };
    for (int count = Math.max(min, 1); count <= max; count++) {
      if (count == 1)
        out.add(new Object[] { VALUE });
      for (final Object[] companion : companions)
        if (companion.length + 1 == count) {
          final Object[] args = new Object[count];
          args[0] = VALUE;
          System.arraycopy(companion, 0, args, 1, companion.length);
          out.add(args);
        }
    }
    return out;
  }

  private static void assertNoLoneSurrogate(final String function, final Object result) {
    if (result instanceof String s) {
      for (int i = 0; i < s.length(); i++) {
        final char c = s.charAt(i);
        if (Character.isHighSurrogate(c) && i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
          i++;
          continue;
        }
        assertThat(Character.isSurrogate(c)).as("%s returned an unpaired surrogate in '%s'", function, s).isFalse();
      }
    } else if (result instanceof Collection<?> collection) {
      for (final Object element : collection)
        assertNoLoneSurrogate(function, element);
    }
  }

  /** Runs the expression from SQL and from openCypher, asserts they agree, and returns the answer. */
  private Object both(final String expression) {
    final Object sql;
    try (final ResultSet rs = database.query("sql", "SELECT " + expression + " AS v")) {
      sql = rs.next().getProperty("v");
    }
    final Object cypher;
    try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS v", Map.of())) {
      cypher = rs.next().getProperty("v");
    }
    assertThat(cypher).as("openCypher and SQL answers for %s", expression).isEqualTo(sql);
    return sql;
  }
}
