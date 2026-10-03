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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.parser.ContainsKeyOperator;
import com.arcadedb.query.sql.parser.ContainsValueOperator;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.GeOperator;
import com.arcadedb.query.sql.parser.GtOperator;
import com.arcadedb.query.sql.parser.ILikeOperator;
import com.arcadedb.query.sql.parser.InOperator;
import com.arcadedb.query.sql.parser.LeOperator;
import com.arcadedb.query.sql.parser.LikeOperator;
import com.arcadedb.query.sql.parser.LtOperator;
import com.arcadedb.query.sql.parser.NeOperator;
import com.arcadedb.query.sql.parser.NeqOperator;
import com.arcadedb.query.sql.parser.NullSafeEqualsCompareOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8979: the SQL comparison operators answered false or true for a null operand instead of unknown, so a
 * NOT around them (or {@code <>} itself) turned a missing value into a match, disagreeing with {@code NOT IN}, openCypher and
 * the documented CASE behavior.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8979NullComparisonsThreeValuedTest extends TestHelper {

  @BeforeEach
  void load() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.k INTEGER");
    database.transaction(() -> {
      database.newVertex("T").set("name", "one", "k", 1).save();
      database.newVertex("T").set("name", "five", "k", 5).save();
      database.newVertex("T").set("name", "null", "k", null).save();
      database.newVertex("T").set("name", "missing").save();
    });
  }

  private List<String> names(final String where) {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE " + where + " ORDER BY name")) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("name"));
    }
    return got;
  }

  @Test
  void negatedComparisonsLeaveOutTheRecordsWithoutAValue() {
    for (final String where : new String[] { "k <> 5", "k != 5", "NOT (k = 5)", "k NOT IN [5]", "NOT (k IN [5])", "NOT (k > 2)",
        "NOT (k BETWEEN 4 AND 6)", "NOT (k >= 2)", "NOT (k > 1 AND k > 2)" })
      assertThat(names(where)).as(where).containsExactly("one");
  }

  @Test
  void comparisonWithNullMatchesNothing() {
    for (final String where : new String[] { "k = null", "k <> null", "k != null", "k < null", "k >= null", "k >= nothing",
        "k <= nothing", "k > nothing", "k < nothing", "k = nothing", "k <> nothing" })
      assertThat(names(where)).as(where).isEmpty();
  }

  @Test
  void explicitNullTestsStillWork() {
    assertThat(names("k IS NULL")).containsExactly("missing", "null");
    assertThat(names("k IS NOT NULL")).containsExactly("five", "one");
    assertThat(names("k <=> null")).containsExactly("missing", "null");
    assertThat(names("NOT (k IS NULL)")).containsExactly("five", "one");
    assertThat(names("NOT (k < 0)")).containsExactly("five", "one");
  }

  @Test
  void booleanLogicCarriesTheUnknown() {
    assertThat(names("k <> 5 OR k = 5")).containsExactly("five", "one");
    assertThat(names("k = 5 OR k IS NULL")).containsExactly("five", "missing", "null");
    assertThat(names("NOT (k = 5 AND name = 'x')")).containsExactly("five", "missing", "null", "one");
  }

  @Test
  void caseAnswersNoForAMissingValue() {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name, CASE WHEN k <> 5 THEN 'yes' ELSE 'no' END AS ne FROM T ORDER BY name")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        got.add(r.getProperty("name") + ":" + r.getProperty("ne"));
      }
    }
    assertThat(got).containsExactly("five:no", "missing:no", "null:no", "one:yes");
  }

  @Test
  void indexedAndOrderedPathsAgreeWithTheScan() {
    database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE");
    assertThat(names("k <> 5")).containsExactly("one");
    assertThat(names("NOT (k > 2)")).containsExactly("one");
    try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE k <> 5 ORDER BY k")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("one");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void unknownIsNotMetInModifierFilterAndScriptControlFlow() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.transaction(() -> database.command("sql", "INSERT INTO D SET items = [{x: 1}, {x: 9}, {y: 2}, {x: null}]"));
    try (final ResultSet rs = database.query("sql", "SELECT items[x > 5] AS hit FROM D")) {
      final List<?> hit = rs.next().getProperty("hit");
      assertThat(hit).hasSize(1);
    }

    try (final ResultSet rs = database.command("sqlscript", "LET $v = null;\nIF ($v > 5) {\n  RETURN 'yes';\n}\nRETURN 'no';")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo("no");
    }

    try (final ResultSet rs = database.command("sqlscript",
        "LET $v = null;\nLET $n = 0;\nWHILE ($v < 10) {\n  LET $n = $n + 1;\n}\nRETURN $n;")) {
      assertThat(rs.next().<Number>getProperty("value").intValue()).isZero();
    }
  }

  @Test
  void onlyTheValueComparisonsAreUnknownOnNull() {
    assertThat(new EqualsCompareOperator().isUnknownOnNull()).isTrue();
    assertThat(new NeOperator().isUnknownOnNull()).isTrue();
    assertThat(new NeqOperator().isUnknownOnNull()).isTrue();
    assertThat(new LtOperator().isUnknownOnNull()).isTrue();
    assertThat(new LeOperator().isUnknownOnNull()).isTrue();
    assertThat(new GtOperator().isUnknownOnNull()).isTrue();
    assertThat(new GeOperator().isUnknownOnNull()).isTrue();
    assertThat(new NullSafeEqualsCompareOperator().isUnknownOnNull()).isFalse();
    assertThat(new LikeOperator().isUnknownOnNull()).isFalse();
    assertThat(new ILikeOperator().isUnknownOnNull()).isFalse();
    assertThat(new InOperator().isUnknownOnNull()).isFalse();
    assertThat(new ContainsKeyOperator().isUnknownOnNull()).isFalse();
    assertThat(new ContainsValueOperator().isUnknownOnNull()).isFalse();
  }

  @Test
  void multiValueFilterEvaluatesEachItem() {
    try (final ResultSet rs = database.query("sql", "SELECT [{x: 1}, {x: 9}, {x: 12}][x > 5].size() AS n")) {
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(2);
    }
  }

  @Test
  void betweenFollowsKleeneLogicWithANullBound() {
    // 1 >= 5 is false, so the whole BETWEEN is false even though the upper bound is unknown; 7 >= 5 is true and 7 <= null is unknown
    assertThat(names("name = 'one' AND NOT (k BETWEEN 5 AND null)")).containsExactly("one");
    assertThat(names("name = 'five' AND NOT (k BETWEEN 5 AND null)")).isEmpty();
    assertThat(names("k BETWEEN null AND 6")).isEmpty();
    assertThat(names("k BETWEEN 0 AND 6")).containsExactly("five", "one");
  }

  @Test
  void rightBinaryFilterDropsNullElements() {
    try (final ResultSet rs = database.query("sql", "SELECT [1, null, 5, 7][<> 5].size() AS n")) {
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(2);
    }
  }
}
