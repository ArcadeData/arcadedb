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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostCommandHandler#appliesRowCap(String, String, int, Database)}, the predicate that decides whether
 * the response cap bounds the rows of a command in the engine: a SELECT or a MATCH that states no LIMIT of its own, or a script ending
 * with one. A command that already carries a LIMIT has stated the caller's own expectation, and that expectation, not the cap, decides
 * how many rows are serialized (issue #5711).
 * <p>
 * Since issue #12016 the cap is no longer written into the text of the statement, so the predicate reads the parsed statement: every
 * shape that needed a rule of its own while a LIMIT was appended to the text (a TIMEOUT or PARALLEL clause after the LIMIT, a FACETS
 * clause, a word in a literal or a subquery) is read as the parser reads it. The command passed in is already trimmed, as in the
 * handler.
 */
class PostCommandHandlerAutoLimitTest {

  private static final int LIMIT = 20_000;

  private static boolean capped(final String command) {
    return PostCommandHandler.appliesRowCap(command, "sql", LIMIT, null);
  }

  private static boolean cappedScript(final String script) {
    return PostCommandHandler.appliesRowCap(script, "sqlScript", LIMIT, null);
  }

  @Test
  void capsAPlainSelect() {
    assertThat(capped("select from V")).isTrue();
  }

  @Test
  void capsAMatch() {
    assertThat(capped("match {type: V, as: v} return v")).isTrue();
  }

  @Test
  void prefixCheckIsCaseInsensitive() {
    assertThat(capped("SELECT from V")).isTrue();
  }

  @Test
  void doesNotCapWhenExplicitLimitAlreadyPresent() {
    assertThat(capped("select from V limit 5")).isFalse();
    assertThat(capped("select from V LIMIT 5")).isFalse();
    assertThat(capped("select from V limit :n")).isFalse();
  }

  /** The semicolon was a reason to leave the text alone while a LIMIT was appended to it: a bound handed to the engine has none. */
  @Test
  void capsASelectEndingWithASemicolon() {
    assertThat(capped("select from V;")).isTrue();
  }

  @Test
  void doesNotCapNonSelectOrMatch() {
    assertThat(capped("insert into V set name = 'a'")).isFalse();
    assertThat(capped("explain select from V")).isFalse();
  }

  @Test
  void doesNotCapForNonSqlLanguage() {
    assertThat(PostCommandHandler.appliesRowCap("MATCH (n) RETURN n", "cypher", LIMIT, null)).isFalse();
  }

  @Test
  void doesNotCapWhenTheCapIsDisabled() {
    assertThat(PostCommandHandler.appliesRowCap("select from V", "sql", -1, null)).isFalse();
    // A non-positive cap means unlimited, as it does in the serializer
    assertThat(PostCommandHandler.appliesRowCap("select from V", "sql", 0, null)).isFalse();
  }

  @Test
  void aWordThatIsNotTheClauseIsNotALimit() {
    // "limitless" must not be mistaken for an existing LIMIT clause
    assertThat(capped("select limitless from V")).isTrue();
    assertThat(capped("select from V where note = 'a limit b'")).isTrue();
    assertThat(capped("select from V where `limit` = 5")).isTrue();
  }

  @Test
  void aLimitOfASubqueryIsNotTheLimitOfTheStatement() {
    assertThat(capped("select from (select from V limit 3)\nwhere x > 1")).isTrue();
    assertThat(capped("SELECT FROM V WHERE tag = 'facets ' AND x IN (SELECT y FROM Z LIMIT 5) ORDER BY ts")).isTrue();
  }

  @Test
  void aSqlScriptIsCapped() {
    assertThat(cappedScript("select from V")).isTrue();
  }

  /** Issue #8429: the cap bounds the script's LAST statement, whatever that is. */
  @Test
  void doesNotCapASqlScriptEndingWithAWrite() {
    assertThat(cappedScript("select count(*) from V; delete from V")).isFalse();
    assertThat(PostCommandHandler.appliesRowCap("SELECT count(*) FROM V; UPDATE V SET x = 1", "sqlscript", LIMIT, null)).isFalse();
    assertThat(cappedScript("select count(*) from V; insert into V set i = -1")).isFalse();
  }

  @Test
  void stillCapsASqlScriptEndingWithAQuery() {
    // The last statement is also the one whose rows the script returns: bounding it is the point of the cap
    assertThat(cappedScript("select from V; select from V")).isTrue();
    assertThat(cappedScript("select from V; match {type: V, as: v} return v")).isTrue();
    assertThat(cappedScript("let a = select from V; select from V where x = 1")).isTrue();
  }

  @Test
  void doesNotCapAScriptWhoseLastStatementHasALimit() {
    assertThat(cappedScript("select from V; select from V limit 3")).isFalse();
  }

  @Test
  void doesNotCapAScriptWhoseStatementsAreSeparatedOnlyByALineBreak() {
    // The script grammar makes the semicolon optional: no ';' in the text does not mean one statement
    assertThat(cappedScript("select count(*) from V\ndelete from V")).isFalse();
  }

  @Test
  void doesNotCapAnUnparsableSqlScript() {
    // Left to the execution, so the error the caller gets quotes the caller's own text
    assertThat(cappedScript("select from V where (")).isFalse();
  }

  @Test
  void aScriptThatHoldsNoQueryIsNotParsed() {
    // A bulk script of writes is the engine's to parse, once: the handler does not read it for a word it cannot hold
    assertThat(cappedScript("insert into V set i = 1; insert into V set i = 2")).isFalse();
  }

  @Test
  void capsASingleStatementSqlScriptSpreadOverLines() {
    assertThat(cappedScript("select from V\nwhere x > 1")).isTrue();
  }

  @Test
  void aSemicolonInsideAStringLiteralDoesNotMakeASecondStatement() {
    assertThat(cappedScript("select from V where name = 'a;b'")).isTrue();
  }

  @Test
  void capsAFacetsQueryWithoutALimit() {
    assertThat(capped("SELECT FROM Asset FACETS brand TERMS TOP 5")).isTrue();
    assertThat(capped("SELECT FROM P WHERE note = 'a limit here' FACETS brand TERMS")).isTrue();
  }

  @Test
  void doesNotCapAFacetsQueryWithALimit() {
    assertThat(capped("SELECT FROM Asset FACETS brand TERMS LIMIT 3")).isFalse();
    // A LIMIT before FACETS is the caller's own mistake: the parser reports it, the statement is not read as one to cap
    assertThat(capped("SELECT FROM Asset LIMIT 3 FACETS brand TERMS")).isFalse();
  }

  /** Issue #11804: a trailing TIMEOUT or PARALLEL clause is no part of the LIMIT decision, wherever the LIMIT stands before it. */
  @Test
  void aTrailingTimeoutOrParallelClauseDecidesNothing() {
    assertThat(capped("SELECT FROM V TIMEOUT 5000")).isTrue();
    assertThat(capped("SELECT FROM V TIMEOUT 5000 EXCEPTION")).isTrue();
    assertThat(capped("SELECT FROM V PARALLEL")).isTrue();
    assertThat(capped("SELECT FROM V TIMEOUT 5000 EXCEPTION PARALLEL")).isTrue();
    assertThat(capped("MATCH {type: V, as: v} RETURN v TIMEOUT 5000")).isTrue();

    assertThat(capped("SELECT FROM V LIMIT 2 TIMEOUT 5000 EXCEPTION")).isFalse();
    assertThat(capped("SELECT FROM V SKIP 1 LIMIT 2 timeout 5000 return")).isFalse();
    assertThat(capped("SELECT FROM V LIMIT 2 /* cap */ TIMEOUT 5000")).isFalse();
    assertThat(capped("SELECT FROM V LIMIT 2 TIMEOUT 5000EXCEPTION")).isFalse();
    assertThat(capped("SELECT FROM V LIMIT 2 PARALLEL")).isFalse();
    assertThat(capped("SELECT FROM V LIMIT 2 TIMEOUT 5000 EXCEPTION PARALLEL")).isFalse();
    assertThat(capped("MATCH {type: V, as: v} RETURN v LIMIT 2 TIMEOUT 5000 EXCEPTION")).isFalse();
  }

  @Test
  void aTextThatDoesNotParseIsLeftToTheExecution() {
    assertThat(capped("SELECT FROM V WHERE a = 'unterminated TIMEOUT 5000")).isFalse();
    assertThat(capped("select from V where (")).isFalse();
  }

  @Test
  void probeLimitIsOneRowAboveTheCapAndSaturates() {
    // What the engine is told the caller reads at most: the extra row is never serialized, it only tells a result ending at the cap
    // from a truncated one.
    assertThat(PostCommandHandler.truncationProbeLimit(LIMIT)).isEqualTo(LIMIT + 1);
    assertThat(PostCommandHandler.truncationProbeLimit(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    assertThat(PostCommandHandler.truncationProbeLimit(0)).isEqualTo(0);
    assertThat(PostCommandHandler.truncationProbeLimit(-1)).isEqualTo(-1);
  }
}
