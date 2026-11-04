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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.grammar.SQLLexer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10788: the documented {@code parallel} option of {@code astar()} could not be written unquoted, {@code {parallel: true}}
 * was a syntax error, because PARALLEL is a keyword of the lexer and a map key had to be an identifier. 71 keywords were in the same
 * place, among them {@code timeout}, {@code unique}, {@code strategy}, {@code batch} and {@code index}. A word between the brace or
 * comma of a map literal and its colon can only be a key, so every one is.
 * <p>
 * The census walks the lexer's vocabulary, not a list: a keyword added to the lexer is covered by this test without touching it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MapKeyKeywordsTest extends TestHelper {

  @Test
  void everyKeywordOfTheLexerIsAMapKeyAsSpelled() {
    final List<String> refused = new ArrayList<>();
    int keywords = 0;
    for (int tokenType = 1; tokenType <= SQLLexer.VOCABULARY.getMaxTokenType(); tokenType++) {
      final String symbolic = SQLLexer.VOCABULARY.getSymbolicName(tokenType);
      if (symbolic == null)
        continue;
      final String literal = SQLLexer.VOCABULARY.getLiteralName(tokenType);
      final String word = literal != null ? literal.replace("'", "") : symbolic.replace("_KW", "");
      if (!word.matches("[A-Za-z_]+"))
        continue;
      ++keywords;

      for (final String spelling : new String[] { word.toLowerCase(), word.toUpperCase() })
        try (final ResultSet rs = database.query("sql", "SELECT {" + spelling + ": 1, other: 2} AS m")) {
          final Map<String, Object> map = rs.next().getProperty("m");
          if (!map.containsKey(spelling) && !(spelling.equalsIgnoreCase("maxDepth") && map.containsKey("maxDepth")))
            refused.add(symbolic + " read as " + map.keySet());
        } catch (final Exception e) {
          refused.add(symbolic + " (" + spelling + "): " + e.getMessage());
        }
    }

    assertThat(keywords).as("the census reads the keywords from the lexer").isGreaterThan(200);
    assertThat(refused).as("keywords refused as a map key").isEmpty();
  }

  @Test
  void aKeywordKeyIsAKeyAfterTheFirstEntryTooAndInAnOptionBag() {
    try (final ResultSet rs = database.query("sql", "SELECT {a: 1, select: 2, index: 3, timeout: 4, parallel: true} AS m")) {
      final Map<String, Object> map = rs.next().getProperty("m");
      assertThat(map).containsEntry("a", 1).containsEntry("select", 2).containsEntry("index", 3).containsEntry("timeout", 4)
          .containsEntry("parallel", true);
    }
    // the shape of the "index:" token (SELECT FROM index:...) without a space is a key too
    try (final ResultSet rs = database.query("sql", "SELECT {index:3} AS m")) {
      assertThat(rs.next().<Map<String, Object>>getProperty("m")).containsEntry("index", 3);
    }
  }

  /** "bucket:1", "bucket:x" and "schema:x" without a space are one target token for the lexer: in a map they are a key and a value. */
  @Test
  void aKeyTheLexerJoinsWithItsValueIsStillAKeyAndAValue() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> database.newDocument("T").set("x", 7).save());
    try (final ResultSet rs = database.query("sql", "SELECT {bucket:1, schema:x, other: 2} AS m, {bucket:x} AS n FROM T")) {
      final Result row = rs.next();
      assertThat(row.<Map<String, Object>>getProperty("m")).containsEntry("bucket", 1).containsEntry("schema", 7).containsEntry("other", 2);
      assertThat(row.<Map<String, Object>>getProperty("n")).containsEntry("bucket", 7);
    }
  }
}
