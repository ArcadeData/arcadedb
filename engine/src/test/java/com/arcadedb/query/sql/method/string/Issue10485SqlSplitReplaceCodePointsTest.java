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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10485: SQL {@code split()} and {@code replace()} cut surrogate pairs when the delimiter or the search string was empty, and
 * SQL {@code split()} dropped the trailing empty pieces openCypher {@code split()} keeps for the same input. {@code 'a😀'.split('')}
 * answered two lone surrogates where openCypher answers the two characters, {@code 'a😀b'.replace('', '-')} put the replacement
 * between the two halves of the emoji, and {@code 'a,'.split(',')} answered {@code ["a"]} where openCypher answers {@code ["a", ""]}.
 * The four split implementations and the three replace ones now share one code, so they answer alike.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10485SqlSplitReplaceCodePointsTest extends TestHelper {
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
