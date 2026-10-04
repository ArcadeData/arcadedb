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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #8998 (string functions count code points), #8996 (simple CASE with list/map literal WHEN values)
 * and #8994 (a parenthesized true/false literal in WHERE is not a node pattern).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSemanticFixesBatch8998Test extends TestHelper {
  private static final String GRIN = new String(Character.toChars(0x1F600));

  private List<Object> column(final String query, final String column, final Object... params) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty(column));
      }
    }
    return out;
  }

  // ---- #8998

  @Test
  void stringFunctionsCountCodePoints() {
    final String s = GRIN + "a";
    assertThat(column("RETURN size($s) AS v", "v", "s", s)).containsExactly(2L);
    assertThat(column("RETURN char_length($s) AS v", "v", "s", s)).containsExactly(2L);
    assertThat(column("RETURN left($s, 1) AS v", "v", "s", s)).containsExactly(GRIN);
    assertThat(column("RETURN left($s, 5) AS v", "v", "s", s)).containsExactly(s);
    assertThat(column("RETURN left($s, 0) AS v", "v", "s", s)).containsExactly("");
    assertThat(column("RETURN right($s, 1) AS v", "v", "s", s)).containsExactly("a");
    assertThat(column("RETURN right($s, 2) AS v", "v", "s", s)).containsExactly(s);
    assertThat(column("RETURN right($s, 0) AS v", "v", "s", s)).containsExactly("");
    assertThat(column("RETURN substring($s, 1) AS v", "v", "s", s)).containsExactly("a");
    assertThat(column("RETURN substring($s, 0, 1) AS v", "v", "s", s)).containsExactly(GRIN);
    assertThat(column("RETURN substring($s, 2) AS v", "v", "s", s)).containsExactly("");
    assertThat(column("RETURN substring($s, 0, 10) AS v", "v", "s", s)).containsExactly(s);
    assertThat(column("RETURN substring($s, 1, 2147483647) AS v", "v", "s", s)).containsExactly("a");
    assertThat(column("RETURN reverse($s) AS v", "v", "s", s)).containsExactly("a" + GRIN);
  }

  @Test
  void stringFunctionsUnchangedOnBmpText() {
    assertThat(column("RETURN size('hello') AS v", "v")).containsExactly(5L);
    assertThat(column("RETURN left('hello', 2) AS v", "v")).containsExactly("he");
    assertThat(column("RETURN right('hello', 2) AS v", "v")).containsExactly("lo");
    assertThat(column("RETURN substring('hello', 1, 3) AS v", "v")).containsExactly("ell");
  }

  @Test
  void substringAndNegativeLengthsOnSeveralSupplementaryCharacters() {
    final String s = GRIN + GRIN + "b" + GRIN + "c";
    assertThat(column("RETURN substring($s, 1, 3) AS v", "v", "s", s)).containsExactly(GRIN + "b" + GRIN);
    assertThat(column("RETURN substring($s, 3) AS v", "v", "s", s)).containsExactly(GRIN + "c");
    assertThat(column("RETURN left($s, 3) AS v", "v", "s", s)).containsExactly(GRIN + GRIN + "b");
    assertThat(column("RETURN right($s, 3) AS v", "v", "s", s)).containsExactly("b" + GRIN + "c");
    assertThatThrownBy(() -> column("RETURN left($s, -1) AS v", "v", "s", s)).isInstanceOf(CommandSemanticException.class);
    assertThatThrownBy(() -> column("RETURN right($s, -1) AS v", "v", "s", s)).isInstanceOf(CommandSemanticException.class);
  }

  // ---- #8996

  @Test
  void simpleCaseMatchesListAndMapLiterals() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:N {id: 1, tags: []}), (:N {id: 2, tags: ['a']}), (:N {id: 3, tags: ['a', 'b']})"));
    assertThat(column("MATCH (n:N) RETURN CASE n.tags WHEN [] THEN 'empty' WHEN ['a'] THEN 'just a' ELSE 'other' END AS v ORDER BY n.id",
        "v")).containsExactly("empty", "just a", "other");
    assertThat(column("RETURN CASE [1, 2] WHEN [1, 2] THEN 'match' ELSE 'no match' END AS v", "v")).containsExactly("match");
    assertThat(column("RETURN CASE {k: 1} WHEN {k: 1} THEN 'match' ELSE 'no match' END AS v", "v")).containsExactly("match");
    assertThat(column("RETURN CASE [1, 2] WHEN [1, 3] THEN 'match' ELSE 'no match' END AS v", "v")).containsExactly("no match");
  }

  @Test
  void simpleCaseSupportsSeveralWhenValues() {
    assertThat(column("UNWIND [1, 2, 3, 4] AS x RETURN CASE x WHEN 1, 2 THEN 'low' WHEN 3 THEN 'three' ELSE 'other' END AS v", "v"))
        .containsExactly("low", "low", "three", "other");
  }

  // ---- #8994

  @Test
  void parenthesizedBooleanLiteralInWhere() {
    database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1}), (:N {id: 2}), (:N {id: 3})"));
    assertThat(column("WITH 1 AS x WHERE (true) RETURN x", "x")).containsExactly(1L);
    assertThat(column("WITH 1 AS x WHERE NOT (true) RETURN x", "x")).isEmpty();
    assertThat(column("WITH 1 AS x WHERE NOT (false) RETURN x", "x")).containsExactly(1L);
    assertThat(column("MATCH (n:N) WITH n WHERE n.id > 1 AND (true) RETURN n.id AS id ORDER BY id", "id")).containsExactly(2, 3);
    assertThat(column("MATCH (n:N) WHERE n.id > 1 AND (true) RETURN n.id AS id ORDER BY id", "id")).containsExactly(2, 3);
    assertThat(column("MATCH (n:N) WHERE (n.id = 1) OR (false) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1);
    assertThat(column("MATCH (n:N) WHERE (null) RETURN n.id AS id", "id")).isEmpty();
    assertThat(column("MATCH (n:N) WHERE (TRUE) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 2, 3);
    assertThat(column("MATCH (n:N) WHERE (Null) RETURN n.id AS id", "id")).isEmpty();
  }

  @Test
  void parenthesizedVariableStartingWithKeywordIsStillAPattern() {
    assertThatThrownBy(() -> column("MATCH (trueish) WHERE (trueish) RETURN trueish", "trueish"))
        .isInstanceOf(CommandParsingException.class);
  }
}
