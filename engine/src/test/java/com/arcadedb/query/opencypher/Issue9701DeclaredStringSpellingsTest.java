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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #9701: the declared-STRING coercion of #9695 ({@code n.v = 1} finds the {@code '1'} a write of
 * {@code 1} stored in a property declared STRING) applied only to the spelling {@code n.v}. Every other spelling of the same
 * read - {@code n['v']}, {@code properties(n).v}, {@code properties(n)['v']}, a chained {@code a.link.v} - and the simple
 * {@code CASE n.v WHEN 1} kept strict typing, so the same test answered differently depending on how it was written.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9701DeclaredStringSpellingsTest extends TestHelper {

  @BeforeEach
  void declare() {
    database.command("sql", "CREATE VERTEX TYPE S");
    database.command("sql", "CREATE PROPERTY S.v STRING");
    database.command("sql", "CREATE PROPERTY S.link LINK");
    database.command("sql", "CREATE VERTEX TYPE U");
    command("CREATE (a:S {name: 'a', v: 1})");
    command("MATCH (a:S {name: 'a'}) CREATE (b:S {name: 'b', v: 2}) SET b.link = a");
    // schemaless: the Cypher typing of = stands
    command("CREATE (:U {v: '1'})");
  }

  @ParameterizedTest
  @ValueSource(strings = { "n.v = 1", "n['v'] = 1", "properties(n).v = 1", "properties(n)['v'] = 1", "1 = n['v']",
      "n.v IN [1, 5]", "n['v'] IN [1, 5]", "properties(n).v IN [1]", "n['v'] <> 2", "NOT (n['v'] <> 1)" })
  void everySpellingOfTheReadFindsTheStoredText(final String predicate) {
    assertThat(names("MATCH (n:S) WHERE " + predicate + " RETURN n.name AS name ORDER BY name")).containsExactly("a");
  }

  @Test
  void aChainedReadThroughALinkIsCoerced() {
    assertThat(names("MATCH (b:S) WHERE b.link.v = 1 RETURN b.name AS name")).containsExactly("b");
    assertThat(names("MATCH (b:S) WHERE b.link['v'] = 1 RETURN b.name AS name")).containsExactly("b");
  }

  @Test
  void theSimpleCaseFormIsCoerced() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:S {name: 'a'}) RETURN CASE n.v WHEN 1 THEN 'one' ELSE 'other' END AS c, "
            + "CASE n['v'] WHEN 1 THEN 'one' ELSE 'other' END AS d, CASE WHEN n.v = 1 THEN 'one' ELSE 'other' END AS e")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("c")).isEqualTo("one");
      assertThat(r.<String>getProperty("d")).isEqualTo("one");
      assertThat(r.<String>getProperty("e")).isEqualTo("one");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "n.v = 1", "n['v'] = 1", "properties(n).v = 1", "properties(n)['v'] = 1", "n['v'] IN [1]" })
  void anUndeclaredPropertyKeepsStrictTyping(final String predicate) {
    assertThat(names("MATCH (n:U) WHERE " + predicate + " RETURN n.v AS name")).isEmpty();
  }

  @Test
  void aProjectedValueKeepsStrictTyping() {
    // the projection carries no record, so nothing says what the text stands for
    assertThat(names("MATCH (n:S {name: 'a'}) WITH n.name AS name, n.v AS v WHERE v = 1 RETURN name")).isEmpty();
  }

  @Test
  void aMapLiteralIsNotARecord() {
    assertThat(names("WITH {v: '1'} AS m WHERE m['v'] = 1 OR m.v = 1 RETURN 'x' AS name")).isEmpty();
  }

  private void command(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private List<String> names(final String cypher) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        names.add(String.valueOf(rs.next().<Object>getProperty("name")));
    }
    return names;
  }
}
