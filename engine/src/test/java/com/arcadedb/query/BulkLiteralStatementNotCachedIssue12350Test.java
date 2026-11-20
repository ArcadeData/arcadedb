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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser.ParsedQuery;
import com.arcadedb.query.opencypher.query.CypherStatementCache;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.StatementCache;
import org.junit.jupiter.api.Test;

import java.util.StringJoiner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12350: a text with more literals than can be extracted into parameters is keyed as written, so only an exact resend can
 * share its cache entry. It was cached anyway, with its whole parse tree: one node chain per literal, 17 to 34 times the text,
 * for each of the 300 entries a cache holds. A bulk write with its data written in pinned gigabytes of heap. Such a text is now
 * parsed for its execution and not cached, whichever lookup asks for it (with the literal extraction on or off, as written, or
 * with a name of the generated namespace in it); a text with fewer literals still is. The last one is remembered, so a statement
 * analyzed before it is executed is still parsed once.
 */
class BulkLiteralStatementNotCachedIssue12350Test extends TestHelper {
  /** More literals than {@code LiteralParameterizer.MAX_LITERALS} (4096). */
  private static final int ROWS   = 10;
  private static final int VALUES = 500;

  @Test
  void aSqlInsertWithItsDataWrittenInIsNotCached() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    final StringJoiner rows = new StringJoiner(", ", "[", "]");
    for (int r = 0; r < ROWS; r++)
      rows.add("{\"k\": " + r + ", \"e\": " + numbers(r) + "}");
    final String insert = "INSERT INTO T CONTENT " + rows;

    database.transaction(() -> database.command("sql", insert).close());

    // count(@rid) reads the records: count(*) answers from a counter, which is no proof that the rows were written
    assertThat(count("SELECT count(@rid) AS n FROM T")).isEqualTo(ROWS);
    assertThat(db().getStatementCache().contains(insert)).as("a text with more literals than can be extracted").isFalse();

    // the control: a text without literals is still cached as written, through the same lookup
    final String small = "SELECT count(@rid) AS n FROM T";
    assertThat(count(small)).isEqualTo(ROWS);
    assertThat(db().getStatementCache().contains(small)).as("a text the SQL cache keeps").isTrue();
  }

  @Test
  void aCypherUnwindWithItsDataWrittenInIsNotCached() {
    database.command("sql", "CREATE VERTEX TYPE V");
    final StringJoiner rows = new StringJoiner(", ", "[", "]");
    for (int r = 0; r < ROWS; r++)
      rows.add("{k: " + r + ", e: " + numbers(r) + "}");
    final String unwind = "UNWIND " + rows + " AS row CREATE (n:V) SET n = row";

    database.transaction(() -> database.command("opencypher", unwind).close());

    assertThat(count("SELECT count(@rid) AS n FROM V")).isEqualTo(ROWS);
    assertThat(db().getCypherStatementCache().contains(unwind)).as("a text with more literals than can be extracted").isFalse();

    final String small = "MATCH (n:V) RETURN count(n) AS n";
    try (final ResultSet rs = database.query("opencypher", small)) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo((long) ROWS);
    }
    assertThat(db().getCypherStatementCache().contains(small)).as("a text the openCypher cache keeps").isTrue();
  }

  @Test
  void aTextAtTheLimitIsCachedAndOneLiteralMoreIsNot() {
    // exactly 4096 literals (LiteralParameterizer.MAX_LITERALS): still a statement the SQL cache keeps, as a generated key or as
    // written. The shape's first text runs uncached (see SQLLiteralParameterizationIssue8307Test), so it takes two lookups.
    final String atTheLimit = "SELECT count(*) AS n FROM T WHERE k IN " + list(4096, 0);
    sql().getParameterized(atTheLimit);
    assertThat(sql().contains(sql().getParameterized(atTheLimit).cacheKey())).as("a text at the limit").isTrue();

    final String overTheLimit = "SELECT count(*) AS n FROM T WHERE k IN " + list(4097, 0);
    sql().getParameterized(overTheLimit);
    final Lookup<Statement> over = sql().getParameterized(overTheLimit);
    assertThat(over.parameters()).as("a text over the limit extracts nothing").isNull();
    assertThat(sql().contains(over.cacheKey())).as("a text one literal over the limit").isFalse();

    final String cypherAtTheLimit = "UNWIND " + list(4096, 0) + " AS x RETURN x";
    cypher().getParameterized(cypherAtTheLimit);
    assertThat(cypher().contains(cypher().getParameterized(cypherAtTheLimit).cacheKey())).as("a Cypher text at the limit").isTrue();

    final String cypherOverTheLimit = "UNWIND " + list(4097, 0) + " AS x RETURN x";
    cypher().getParameterized(cypherOverTheLimit);
    assertThat(cypher().contains(cypher().getParameterized(cypherOverTheLimit).cacheKey())).as("a Cypher text over the limit")
        .isFalse();
  }

  @Test
  void aBulkTextIsNotCachedWhateverPathLooksItUp() {
    final String bulk = "SELECT count(*) AS n FROM T WHERE k IN " + list(5000, 0);
    final String cypherBulk = "UNWIND " + list(5000, 0) + " AS x RETURN x";

    // the lookup that never extracts a literal: the statement cache's own `get`, and the Cypher EXPLAIN/PROFILE one
    sql().get(bulk);
    assertThat(sql().contains(bulk)).as("SQL, as written").isFalse();
    cypher().getAsWritten(cypherBulk);
    assertThat(cypher().contains(cypherBulk)).as("Cypher, as written").isFalse();
    cypher().getParsed(cypherBulk);
    assertThat(cypher().contains(cypherBulk)).as("Cypher parsed, as written").isFalse();

    // the setting off: the caches key on the exact text, which a text with this many literals cannot afford
    setParameterization(false);
    try {
      sql().getParameterized(bulk);
      assertThat(sql().contains(bulk)).as("SQL, parameterization off").isFalse();
      cypher().getParameterized(cypherBulk);
      assertThat(cypher().contains(cypherBulk)).as("Cypher, parameterization off").isFalse();

      final String small = "SELECT count(*) AS n FROM T WHERE k IN [1, 2, 3]";
      sql().getParameterized(small);
      assertThat(sql().contains(small)).as("a small text is still cached as written").isTrue();
    } finally {
      setParameterization(true);
    }
  }

  @Test
  void aBulkTextThatMentionsTheGeneratedNamespaceIsNotCached() {
    // a text with a name of the generated namespace is never parameterized and goes down the as-written path: the data of a bulk
    // write can carry such a string, and must not be able to buy its way back into the cache with it
    final String sqlBulk = "INSERT INTO T CONTENT {\"s\": \"__lit_i0\", \"e\": " + list(5000, 0) + "}";
    sql().getParameterized(sqlBulk);
    sql().getParameterized(sqlBulk);
    assertThat(sql().contains(sqlBulk)).as("SQL").isFalse();

    final String cypherBulk = "UNWIND " + list(5000, 0) + " AS x RETURN x, '__lit_i0' AS s";
    cypher().getParameterized(cypherBulk);
    cypher().getParameterized(cypherBulk);
    assertThat(cypher().contains(cypherBulk)).as("Cypher").isFalse();

    final String small = "SELECT count(*) AS n FROM T WHERE s = '__lit_i0'";
    sql().getParameterized(small);
    assertThat(sql().contains(small)).as("a small text of the same kind is cached as written").isTrue();
  }

  @Test
  void aBulkTextAnalyzedThenExecutedIsParsedOnce() {
    // classifying a statement before executing it is free for every text the cache keeps; a bulk text is not kept, so the last one
    // is remembered for the second lookup.
    // The remembered entry is held softly, and the heap may reclaim it whenever nothing else reaches it. Each test below keeps the
    // Lookup it got in a local and reads it AFTER the repeated lookup, so it is reachable throughout and the test cannot depend on
    // the heap's mood.
    final String bulk = "SELECT count(*) AS n FROM T WHERE k IN " + list(5000, 0);
    final Lookup<Statement> first = sql().getParameterized(bulk);
    assertThat(sql().getParameterized(bulk).statement()).as("SQL, parameterized lookup").isSameAs(first.statement());
    assertThat(sql().get(bulk)).as("SQL, lookup as written").isSameAs(first.statement());
    assertThat(sql().contains(bulk)).as("remembered is not cached").isFalse();

    final String cypherBulk = "UNWIND " + list(5000, 0) + " AS x RETURN x";
    final Lookup<ParsedQuery> cypherFirst = cypher().getParameterized(cypherBulk);
    assertThat(cypher().getParameterized(cypherBulk).statement()).as("Cypher, parameterized lookup").isSameAs(cypherFirst.statement());
    assertThat(cypher().getAsWritten(cypherBulk).statement()).as("Cypher, lookup as written").isSameAs(cypherFirst.statement());
    // a trailing semicolon is dropped before the lookup, so the text is a new String, and the same statement all the same
    assertThat(cypher().getParameterized(cypherBulk + ";").statement()).as("Cypher, text with a semicolon")
        .isSameAs(cypherFirst.statement());

    // only the last bulk text is remembered: the heap never holds more than one of them
    final String other = "SELECT count(*) AS n FROM T WHERE k IN " + list(5000, 1);
    final Lookup<Statement> otherFirst = sql().getParameterized(other);
    assertThat(sql().getParameterized(other).statement()).isSameAs(otherFirst.statement());
    assertThat(sql().getParameterized(bulk).statement()).as("the first text was replaced by the second").isNotSameAs(first.statement());

    // and a cleared cache forgets it, as it forgets every statement
    final Lookup<Statement> again = sql().getParameterized(bulk);
    sql().clear();
    assertThat(sql().getParameterized(bulk).statement()).as("after clear()").isNotSameAs(again.statement());
  }

  @Test
  void aBulkStatementThatFailsToParseIsNotRemembered() {
    final String broken = "SELECT FROM T WHERE k IN " + list(5000, 0) + " )";
    assertThatThrownBy(() -> sql().getParameterized(broken)).isInstanceOf(CommandSQLParsingException.class);
    assertThat(sql().contains(broken)).isFalse();
    assertThatThrownBy(() -> sql().getParameterized(broken)).isInstanceOf(CommandSQLParsingException.class);
  }

  /** {@code count} distinct numbers, each one literal, as a list; the offset keeps two lists of one size apart. */
  private static String list(final int count, final int offset) {
    final StringJoiner values = new StringJoiner(", ", "[", "]");
    for (int i = 0; i < count; i++)
      values.add(String.valueOf(offset * count + i + 0.5));
    return values.toString();
  }

  private StatementCache sql() {
    return db().getStatementCache();
  }

  private CypherStatementCache cypher() {
    return db().getCypherStatementCache();
  }

  private void setParameterization(final boolean enabled) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION, enabled);
    sql().clear();
    cypher().clear();
    db().getExecutionPlanCache().invalidate();
    db().getCypherPlanCache().invalidate();
  }

  private static String numbers(final int row) {
    final StringJoiner values = new StringJoiner(", ", "[", "]");
    for (int i = 0; i < VALUES; i++)
      values.add(String.valueOf(row * VALUES + i + 0.5));
    return values.toString();
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private DatabaseInternal db() {
    return (DatabaseInternal) database;
  }
}
