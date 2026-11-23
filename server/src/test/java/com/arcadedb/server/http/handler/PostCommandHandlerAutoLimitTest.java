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
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostCommandHandler#requiresAutomaticLimit(String, String, int, Database)}, the predicate that
 * decides whether the handler pushes a trailing {@code LIMIT} into the command: it must behave exactly as the
 * previous inline implementation while avoiding a full-command {@code toLowerCase} copy per request, because a
 * command that already carries a LIMIT has stated the caller's own expectation and must not be rewritten
 * (issue #5711). The command passed in is already trimmed, as in the handler.
 */
class PostCommandHandlerAutoLimitTest {

  private static final int LIMIT = 20_000;

  @Test
  void appendsLimitToPlainSelect() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V", "sql", LIMIT, null)).isTrue();
  }

  @Test
  void appendsLimitToMatch() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("match {type: V, as: v} return v", "sql", LIMIT, null)).isTrue();
  }

  @Test
  void prefixCheckIsCaseInsensitive() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT from V", "sql", LIMIT, null)).isTrue();
  }

  @Test
  void doesNotAppendWhenExplicitLowercaseLimitAlreadyPresent() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V limit 5", "sql", LIMIT, null)).isFalse();
  }

  @Test
  void doesNotAppendWhenExplicitUppercaseLimitAlreadyPresent() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V LIMIT 5", "sql", LIMIT, null)).isFalse();
  }

  @Test
  void doesNotAppendWhenCommandEndsWithSemicolon() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V;", "sql", LIMIT, null)).isFalse();
  }

  @Test
  void doesNotAppendToNonSelectOrMatch() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("insert into V set name = 'a'", "sql", LIMIT, null)).isFalse();
  }

  @Test
  void doesNotAppendForNonSqlLanguage() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("MATCH (n) RETURN n", "cypher", LIMIT, null)).isFalse();
  }

  @Test
  void doesNotAppendWhenLimitDisabled() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V", "sql", -1, null)).isFalse();
  }

  @Test
  void doesNotAppendWhenLimitIsZero() {
    // A non-positive cap means unlimited, as it does in the serializer: appending 'limit 0' would return no row.
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V", "sql", 0, null)).isFalse();
  }

  @Test
  void appendsWhenLimitSubstringIsNotAStandaloneClause() {
    // "limitless" must not be mistaken for an existing LIMIT clause.
    assertThat(PostCommandHandler.requiresAutomaticLimit("select limitless from V", "sql", LIMIT, null)).isTrue();
  }

  @Test
  void appendsWhenLimitOnlyOnEarlierLineButNotOnLastLine() {
    // A subquery LIMIT on an earlier line must still let the outer query receive a trailing LIMIT.
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from (select from V limit 3)\nwhere x > 1", "sql", LIMIT, null)).isTrue();
  }

  @Test
  void handlesSqlScriptLanguage() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V", "sqlScript", LIMIT, null)).isTrue();
  }

  @Test
  void doesNotAppendToASqlScriptEndingWithAWrite() {
    // Issue #8429: the LIMIT lands on the script's LAST statement, whatever that is.
    assertThat(PostCommandHandler.requiresAutomaticLimit("select count(*) from V; delete from V", "sqlScript", LIMIT, null)).isFalse();
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT count(*) FROM V; UPDATE V SET x = 1", "sqlscript", LIMIT, null)).isFalse();
    assertThat(PostCommandHandler.requiresAutomaticLimit("select count(*) from V; insert into V set i = -1", "sqlScript", LIMIT, null)).isFalse();
  }

  @Test
  void stillAppendsToASqlScriptEndingWithAQuery() {
    // The last statement is also the one whose rows the script returns: bounding it is the push-down's purpose.
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V; select from V", "sqlScript", LIMIT, null)).isTrue();
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V; match {type: V, as: v} return v", "sqlScript", LIMIT, null)).isTrue();
  }

  @Test
  void doesNotAppendToAScriptWhoseStatementsAreSeparatedOnlyByALineBreak() {
    // The script grammar makes the semicolon optional: no ';' in the text does not mean one statement.
    assertThat(PostCommandHandler.requiresAutomaticLimit("select count(*) from V\ndelete from V", "sqlScript", LIMIT, null)).isFalse();
  }

  @Test
  void doesNotAppendToAnUnparsableSqlScript() {
    // Left as written, so the error the caller gets quotes the caller's own text.
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V where (", "sqlScript", LIMIT, null)).isFalse();
  }

  @Test
  void stillAppendsToASingleStatementSqlScriptSpreadOverLines() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V\nwhere x > 1", "sqlScript", LIMIT, null)).isTrue();
  }

  @Test
  void aSemicolonInsideAStringLiteralDoesNotMakeASecondStatement() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("select from V where name = 'a;b'", "sqlScript", LIMIT, null)).isTrue();
  }

  @Test
  void appendsLimitToAFacetsQueryWithoutOne() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM Asset FACETS brand TERMS TOP 5", "sql", LIMIT, null)).isTrue();
  }

  @Test
  void doesNotAppendWhenTheLimitPrecedesFacets() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM Asset FACETS brand TERMS LIMIT 3", "sql", LIMIT, null)).isFalse();
    // A LIMIT before FACETS is the caller's own mistake: left as written, the parser says FACETS must come first, instead of
    // reporting the second LIMIT this handler would have appended
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM Asset LIMIT 3 FACETS brand TERMS", "sql", LIMIT, null)).isFalse();
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM Asset LIMIT 3\nFACETS brand TERMS", "sql", LIMIT, null)).isFalse();
  }

  /** The words alone decide nothing: a query without a FACETS clause that mentions both is auto-limited as it always was. */
  @Test
  void aQueryMentioningFacetsAndLimitWithoutTheClauseIsStillAutoLimited() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT name, facets FROM P WHERE note = 'no limit here' ORDER BY name", "sql",
        LIMIT, null)).isTrue();
    assertThat(PostCommandHandler.requiresAutomaticLimit(
        "SELECT FROM V WHERE tag = 'facets ' AND x IN (SELECT y FROM Z LIMIT 5) ORDER BY ts", "sql", LIMIT, null)).isTrue();
  }

  /** A FACETS query without a LIMIT of its own gets one, even when its text mentions the word: FACETS precedes the appended LIMIT. */
  @Test
  void aFacetsQueryWithoutItsOwnLimitIsAutoLimitedEvenWhenTheTextSaysLimit() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM P WHERE note = 'a limit here' FACETS brand TERMS", "sql", LIMIT,
        null)).isTrue();
  }

  /** Issue #11804: a trailing TIMEOUT clause is not part of the LIMIT decision, and the appended LIMIT goes before it. */
  @Test
  void aTrailingTimeoutClauseKeepsTheLimitBeforeIt() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V TIMEOUT 5000", "sql", LIMIT, null)).isTrue();
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V TIMEOUT 5000 EXCEPTION", "sql", LIMIT, null)).isTrue();
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V LIMIT 2 TIMEOUT 5000 EXCEPTION", "sql", LIMIT, null)).isFalse();
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V SKIP 1 LIMIT 2 timeout 5000 return", "sql", LIMIT, null))
        .isFalse();

    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V", 21)).isEqualTo("SELECT FROM V limit 21");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT 5000", 21)).isEqualTo("SELECT FROM V limit 21 TIMEOUT 5000");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V ORDER BY a\ntimeout 5000 EXCEPTION", 21))
        .isEqualTo("SELECT FROM V ORDER BY a\nlimit 21 timeout 5000 EXCEPTION");
    // A literal is not a clause: only a TIMEOUT the text ends with moves the LIMIT
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V WHERE note = 'timeout 5000'", 21))
        .isEqualTo("SELECT FROM V WHERE note = 'timeout 5000' limit 21");
  }

  /** Comments between the words of a trailing TIMEOUT clause are skipped as the lexer skips them, and a LIMIT stays out of them. */
  @Test
  void aTrailingTimeoutClauseIsFoundAcrossComments() {
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT /* budget */ 5000", 21))
        .isEqualTo("SELECT FROM V limit 21 TIMEOUT /* budget */ 5000");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT 5000 -- ms\nRETURN", 21))
        .isEqualTo("SELECT FROM V limit 21 TIMEOUT 5000 -- ms\nRETURN");
    // A line comment before the clause: the LIMIT goes after the line break that ends it, not into the comment text
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V -- note\nTIMEOUT 5000", 21))
        .isEqualTo("SELECT FROM V -- note\nlimit 21 TIMEOUT 5000");
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V LIMIT 2 /* cap */ TIMEOUT 5000", "sql", LIMIT, null))
        .isFalse();
    // Words in a comment or a quoted identifier are not a clause
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V /* TIMEOUT 5000 */", 21))
        .isEqualTo("SELECT FROM V /* TIMEOUT 5000 */ limit 21");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT `timeout 5000` FROM V", 21))
        .isEqualTo("SELECT `timeout 5000` FROM V limit 21");
  }

  /** The value of a trailing TIMEOUT is any number literal the SQL lexer reads: a type suffix, hexadecimal, a fraction, an exponent. */
  @Test
  void aTrailingTimeoutTakesEveryNumberLiteral() {
    for (final String millis : List.of("5000L", "5000l", "0x1388", "0X1388L", "0", "5e3", "5E+3", "5000.0", "5000.5d", ".5e4"))
      assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT " + millis + " RETURN", 21)).as(millis)
          .isEqualTo("SELECT FROM V limit 21 TIMEOUT " + millis + " RETURN");
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V LIMIT 2 TIMEOUT 5000L", "sql", LIMIT, null)).isFalse();
    // A word that is no number literal is no TIMEOUT value
    for (final String notANumber : List.of("5000abc", "abc", "0x", "08", "5e"))
      assertThat(PostCommandHandler.trailingClauses("SELECT FROM V TIMEOUT " + notANumber)).as(notANumber).isNull();
  }

  /** A number literal ends where the lexer ends it: a keyword written right after it is the next token, not part of the number. */
  @Test
  void aTrailingTimeoutActionWrittenRightAfterTheNumberIsFound() {
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT 5000RETURN", 21))
        .isEqualTo("SELECT FROM V limit 21 TIMEOUT 5000RETURN");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT 5000EXCEPTION", 21))
        .isEqualTo("SELECT FROM V limit 21 TIMEOUT 5000EXCEPTION");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT 5000Lreturn", 21))
        .isEqualTo("SELECT FROM V limit 21 TIMEOUT 5000Lreturn");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT 5e3EXCEPTION", 21))
        .isEqualTo("SELECT FROM V limit 21 TIMEOUT 5e3EXCEPTION");
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V LIMIT 2 TIMEOUT 5000EXCEPTION", "sql", LIMIT, null)).isFalse();
    // A word that is no keyword after the number is still no clause
    assertThat(PostCommandHandler.trailingClauses("SELECT FROM V TIMEOUT 5000abc")).isNull();
    assertThat(PostCommandHandler.trailingClauses("SELECT FROM V TIMEOUT 5000returns")).isNull();
  }

  /**
   * The grammar ends a SELECT with {@code timeout? PARALLEL?}: a LIMIT appended after a trailing PARALLEL (the engine reads the
   * keyword and ignores it) is as much a syntax error as one appended after the TIMEOUT.
   */
  @Test
  void aTrailingParallelKeepsTheLimitBeforeIt() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V PARALLEL", "sql", LIMIT, null)).isTrue();
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V TIMEOUT 5000 EXCEPTION PARALLEL", "sql", LIMIT, null)).isTrue();
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V LIMIT 2 PARALLEL", "sql", LIMIT, null)).isFalse();
    assertThat(PostCommandHandler.requiresAutomaticLimit("SELECT FROM V LIMIT 2 TIMEOUT 5000 EXCEPTION PARALLEL", "sql", LIMIT, null))
        .isFalse();

    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V PARALLEL", 21)).isEqualTo("SELECT FROM V limit 21 PARALLEL");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V ORDER BY a parallel", 21))
        .isEqualTo("SELECT FROM V ORDER BY a limit 21 parallel");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT 5000 PARALLEL", 21))
        .isEqualTo("SELECT FROM V limit 21 TIMEOUT 5000 PARALLEL");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V TIMEOUT 5000 EXCEPTION /* ms */ PARALLEL", 21))
        .isEqualTo("SELECT FROM V limit 21 TIMEOUT 5000 EXCEPTION /* ms */ PARALLEL");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V -- note\nPARALLEL", 21))
        .isEqualTo("SELECT FROM V -- note\nlimit 21 PARALLEL");
    // The word is a keyword of the statement only where the statement ends with it: as a map key, in a literal, in a name, it is not
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V WHERE o = {parallel: true}", 21))
        .isEqualTo("SELECT FROM V WHERE o = {parallel: true} limit 21");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V WHERE note = 'PARALLEL'", 21))
        .isEqualTo("SELECT FROM V WHERE note = 'PARALLEL' limit 21");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V WHERE a = parallels", 21))
        .isEqualTo("SELECT FROM V WHERE a = parallels limit 21");
    // Nothing before it: no statement to put a LIMIT in
    assertThat(PostCommandHandler.trailingClauses("PARALLEL")).isNull();
  }

  /** The TIMEOUT clause of a MATCH is its last clause too (matchStatement ends with {@code skip? limit? timeout?}). */
  @Test
  void aTrailingTimeoutClauseOfAMatchKeepsTheLimitBeforeIt() {
    assertThat(PostCommandHandler.requiresAutomaticLimit("MATCH {type: V, as: v} RETURN v TIMEOUT 5000", "sql", LIMIT, null)).isTrue();
    assertThat(PostCommandHandler.requiresAutomaticLimit("MATCH {type: V, as: v} RETURN v LIMIT 2 TIMEOUT 5000 EXCEPTION", "sql", LIMIT,
        null)).isFalse();
    assertThat(PostCommandHandler.withAutomaticLimit("MATCH {type: V, as: v} RETURN v TIMEOUT 5000", 21))
        .isEqualTo("MATCH {type: V, as: v} RETURN v limit 21 TIMEOUT 5000");
    // RETURN is the keyword of the MATCH and the action of its TIMEOUT: only the one the text ends with is part of the clause
    assertThat(PostCommandHandler.withAutomaticLimit("MATCH {type: V, as: v} RETURN v TIMEOUT 5000 RETURN", 21))
        .isEqualTo("MATCH {type: V, as: v} RETURN v limit 21 TIMEOUT 5000 RETURN");
    assertThat(PostCommandHandler.withAutomaticLimit("MATCH {type: V, as: v} RETURN v", 21))
        .isEqualTo("MATCH {type: V, as: v} RETURN v limit 21");
  }

  /** The text is read as the parser reads it: a word that only contains {@code timeout}, or a statement the lexer refuses, is no clause. */
  @Test
  void aTextThatOnlyMentionsTimeoutIsLeftAlone() {
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V WHERE mytimeout = 5", 21))
        .isEqualTo("SELECT FROM V WHERE mytimeout = 5 limit 21");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V WHERE timeouts > 5000", 21))
        .isEqualTo("SELECT FROM V WHERE timeouts > 5000 limit 21");
    // A subquery ends with its own TIMEOUT, the statement does not
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM (SELECT FROM V TIMEOUT 5000)", 21))
        .isEqualTo("SELECT FROM (SELECT FROM V TIMEOUT 5000) limit 21");
    // An unterminated literal is no SQL: the text goes to the parser as it came, which reports it with its own message
    assertThat(PostCommandHandler.trailingClauses("SELECT FROM V WHERE a = 'unterminated TIMEOUT 5000")).isNull();
  }

  /** The offsets are positions in the text: a character outside the BMP (two chars, one code point) before the clause shifts none. */
  @Test
  void aSupplementaryCharacterBeforeTheClauseDoesNotShiftTheLimit() {
    final String emoji = new String(Character.toChars(0x1F600));
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V WHERE note = '" + emoji + emoji + "' TIMEOUT 5000", 21))
        .isEqualTo("SELECT FROM V WHERE note = '" + emoji + emoji + "' limit 21 TIMEOUT 5000");
    assertThat(PostCommandHandler.withAutomaticLimit("SELECT FROM V WHERE note = '" + emoji + "' LIMIT 3 TIMEOUT 5000", 21))
        .isEqualTo("SELECT FROM V WHERE note = '" + emoji + "' LIMIT 3 limit 21 TIMEOUT 5000");
  }

  /**
   * A statement with a huge IN list is read in one lexer pass and pays nothing per number: the clause comes out right and the time
   * stays bounded (the scan used to try seven number forms on every number of the text).
   */
  @Test
  void aLongStatementIsReadInOnePass() {
    final StringBuilder in = new StringBuilder("SELECT FROM V WHERE a IN [");
    for (int i = 0; i < 200_000; i++)
      in.append(i % 3 == 0 ? Integer.toString(i) : i % 3 == 1 ? i + ".5" : "0x" + Integer.toHexString(i)).append(',');
    in.append("1] TIMEOUT 5000 RETURN");
    final String command = in.toString();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final String limited = PostCommandHandler.withAutomaticLimit(command, 21);
    stopwatch.assertGaveUpWithin(20_000, "one pass over the text from a rescan for every number of it");

    assertThat(limited).endsWith("1] limit 21 TIMEOUT 5000 RETURN");
    assertThat(limited).hasSize(command.length() + " limit 21".length());
  }

  /** A long literal that mentions the word is still read in one pass, and its content is no clause. */
  @Test
  void aLongLiteralMentioningTheWordIsNoClause() {
    final String command = "SELECT FROM V WHERE note = 'timeout 5000" + " ".repeat(2_000_000) + "x'";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThat(PostCommandHandler.trailingClauses(command)).isNull();
    assertThat(PostCommandHandler.withAutomaticLimit(command + " TIMEOUT 5000", 21)).endsWith("x' limit 21 TIMEOUT 5000");
    stopwatch.assertGaveUpWithin(20_000, "one pass over the text from a rescan from each of its characters");
  }

  @Test
  void probeLimitIsOneRowAboveTheCapAndSaturates() {
    // What the handler actually pushes down: the extra row is never serialized, it only tells a result ending
    // at the cap from a truncated one.
    assertThat(PostCommandHandler.truncationProbeLimit(LIMIT)).isEqualTo(LIMIT + 1);
    assertThat(PostCommandHandler.truncationProbeLimit(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    assertThat(PostCommandHandler.truncationProbeLimit(0)).isEqualTo(0);
    assertThat(PostCommandHandler.truncationProbeLimit(-1)).isEqualTo(-1);
  }
}
