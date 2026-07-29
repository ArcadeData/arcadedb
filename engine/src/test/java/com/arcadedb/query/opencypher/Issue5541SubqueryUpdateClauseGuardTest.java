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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.opencypher.ast.CorrelatedSubqueryRewriter;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #5541.
 * <p>
 * {@code COUNT { }}, {@code EXISTS { }} and {@code COLLECT { }} must reject a body that contains an
 * update clause. The guard used to be a plain {@code toUpperCase().contains("SET ")} scan, so any
 * body whose <em>text</em> mentioned an update keyword was rejected - including a keyword sitting
 * inside a string literal, where it is ordinary user data rather than a clause. A read-only filter
 * such as {@code WHERE n.name = 'SET x'} therefore failed to parse.
 */
class Issue5541SubqueryUpdateClauseGuardTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5541-subquery-update-clause-guard").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (:T {name:'SET x'}), (:T {name:'CREATE z'}), (:T {name:'DELETE me'}), (:T {name:'plain'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private Object scalar(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("v");
    }
  }

  @Test
  void countSubqueryWithUpdateKeywordInsideStringLiteral() {
    assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'SET x' RETURN n } AS v")).isEqualTo(1L);
    assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'CREATE z' RETURN n } AS v")).isEqualTo(1L);
    assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'DELETE me' RETURN n } AS v")).isEqualTo(1L);
  }

  @Test
  void countSubqueryWithUpdateKeywordAsSubstringOfLiteral() {
    // The literal need not start with the keyword for the naive scan to trip
    assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'a SET b' RETURN n } AS v")).isEqualTo(0L);
  }

  @Test
  void existsSubqueryWithUpdateKeywordInsideStringLiteral() {
    assertThat(scalar("RETURN EXISTS { MATCH (n:T) WHERE n.name = 'SET x' RETURN n } AS v")).isEqualTo(true);
    assertThat(scalar("RETURN EXISTS { MATCH (n:T) WHERE n.name = 'MERGE nothing' RETURN n } AS v")).isEqualTo(false);
  }

  @Test
  void collectSubqueryWithUpdateKeywordInsideStringLiteral() {
    assertThat(scalar("RETURN COLLECT { MATCH (n:T) WHERE n.name = 'SET x' RETURN n.name } AS v"))
        .isEqualTo(List.of("SET x"));
  }

  @Test
  void doubleQuotedAndBacktickQuotedTextIsAlsoData() {
    assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = \"REMOVE this\" RETURN n } AS v")).isEqualTo(0L);
    assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.`name` = 'SET x' RETURN n } AS v")).isEqualTo(1L);
  }

  @Test
  void genuineUpdateClausesAreStillRejected() {
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:T) RETURN COUNT { MATCH (m:T) SET m.x = 1 RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (n:T) RETURN EXISTS { MATCH (m:T) SET m.x = 1 } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThatThrownBy(
        () -> database.query("opencypher", "MATCH (n:T) RETURN COLLECT { MATCH (m:T) SET m.x = 1 RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { MATCH (m:T) DETACH DELETE m RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { CREATE (m:T) RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { MERGE (m:T {name:'q'}) RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { MATCH (m:T) REMOVE m:T RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
  }

  /**
   * INSERT is a synonym of CREATE in this engine, so a body opening with it is a write. It used to
   * slip past the guard and reach the executor, which absorbed it into the expression's neutral
   * value - a silent 0 rather than an error. No row was ever written, but the answer was wrong.
   */
  @Test
  void insertIsRejectedAsAnUpdateClause() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { INSERT (m:T {name:'x'}) RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN EXISTS { INSERT (m:T {name:'x'}) RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThatThrownBy(() -> database.query("opencypher", "RETURN COLLECT { INSERT (m:T {name:'x'}) RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("INSERT (m:T)")).isTrue();

    // ... but INSERT inside a literal is still just data
    assertThat(scalar("RETURN COUNT { MATCH (n:T) WHERE n.name = 'INSERT here' RETURN n } AS v")).isEqualTo(0L);
    assertThat(scalar("MATCH (n:T) RETURN count(n) AS v")).isEqualTo(4L);
  }

  /**
   * A map key spelled like an update clause is data, not a clause. The grammar accepts
   * {@code MATCH (n:T {set : 1}) RETURN n} standalone, so the guard must accept it too.
   */
  @Test
  void mapKeyNamedLikeAnUpdateClauseIsNotAClause() {
    assertThat(scalar("RETURN COUNT { MATCH (n:T {set : 1}) RETURN n } AS v")).isEqualTo(0L);
    assertThat(scalar("RETURN COUNT { MATCH (n:T {create : 1}) RETURN n } AS v")).isEqualTo(0L);
    assertThat(scalar("RETURN EXISTS { MATCH (n:T {set : 1}) RETURN n } AS v")).isEqualTo(false);

    // The lookahead must not swallow a genuine clause that merely has extra whitespace
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n)   SET   n.x = 1")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) REMOVE n:T")).isTrue();
  }

  /**
   * None of the update keywords is reserved in this grammar - {@code RETURN 1 AS set} parses - so an
   * alias spelled like one must not be read as a clause. A scan that accepts any word boundary flags
   * it; the guard requires the keyword to be followed by whitespace or {@code (}, as a clause is.
   */
  @Test
  void anAliasSpelledLikeAnUpdateClauseIsNotAClause() {
    for (final String keyword : new String[] { "set", "create", "delete", "merge", "remove", "insert" }) {
      assertThat(scalar("RETURN COUNT { MATCH (n:T) RETURN 1 AS " + keyword + " } AS v"))
          .as("alias named %s", keyword).isEqualTo(4L);
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) RETURN 1 AS " + keyword))
          .as("alias named %s", keyword).isFalse();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) RETURN 1 AS " + keyword + ", 2 AS b"))
          .as("alias named %s before a comma", keyword).isFalse();
    }

    // A space-less write is still a write, which the original trailing-space scan missed
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("CREATE(n:T)")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) DELETE n")).isTrue();
  }

  /**
   * The scan uppercases the body to compare against ASCII keywords, so it must not depend on the
   * default locale. In a Turkish locale {@code "insert".toUpperCase()} yields a dotted capital I,
   * which does not match {@code INSERT} and let a lowercase write through.
   */
  @Test
  void keywordMatchingIsIndependentOfTheDefaultLocale() {
    final Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("insert (n:T)")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("match (n) set n.x = 1")).isTrue();
      assertThat(CorrelatedSubqueryRewriter.startsWithClauseKeyword("unwind [1] AS y RETURN y")).isTrue();
    } finally {
      Locale.setDefault(original);
    }
  }

  @Test
  void backtickIdentifiersEscapeByDoublingNotByBackslash() {
    // A doubled backtick closes and reopens the quote, so the SET after it is still inside data
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.`a``b SET c` = 1 RETURN n")).isFalse();
    // A backslash is not an escape inside a backtick identifier, so it must not swallow the closer
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.`a\\` = 1 SET n.x = 2")).isTrue();
  }

  @Test
  void rejectedUpdateSubqueryLeavesTheDataUntouched() {
    assertThatThrownBy(() -> database.query("opencypher", "RETURN COUNT { CREATE (m:T {name:'leaked'}) RETURN m } AS v"))
        .hasMessageContaining("InvalidClauseComposition");
    assertThat(scalar("MATCH (n:T) RETURN count(n) AS v")).isEqualTo(4L);
  }

  @Test
  void updateClauseTestSkipsLiteralsAndRespectsWordBoundaries() {
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.name = 'SET x' RETURN n")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.name = \"CREATE z\" RETURN n")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.`DELETE` = 1 RETURN n")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.name = 'it\\'s SET' RETURN n")).isFalse();

    // Not clause keywords: property access, labels/types, and longer identifiers
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) RETURN n.createdAt")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) RETURN n.set")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n)-[r:SET_BY]->(m) RETURN r")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n {create: 1}) RETURN n")).isFalse();
    // ... including when whitespace separates the key from its colon
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n {set : 1}) RETURN n")).isFalse();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n {create  :  1}) RETURN n")).isFalse();

    // Genuine update clauses
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) SET n.x = 1")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) WHERE n.name = 'SET x' SET n.x = 1")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("CREATE (n:T)")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) DETACH DELETE n")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MERGE (n:T {x: 1})")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("MATCH (n) REMOVE n:T")).isTrue();
    assertThat(CorrelatedSubqueryRewriter.containsUpdateClause("")).isFalse();
  }

  @Test
  void correlatedSubqueryWithUpdateKeywordInsideStringLiteral() {
    assertThat(scalar("MATCH (n:T {name:'plain'}) RETURN COUNT { MATCH (m:T) WHERE m.name = 'SET x' RETURN m } AS v"))
        .isEqualTo(1L);
  }
}
