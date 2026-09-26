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
  void probeLimitIsOneRowAboveTheCapAndSaturates() {
    // What the handler actually pushes down: the extra row is never serialized, it only tells a result ending
    // at the cap from a truncated one.
    assertThat(PostCommandHandler.truncationProbeLimit(LIMIT)).isEqualTo(LIMIT + 1);
    assertThat(PostCommandHandler.truncationProbeLimit(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    assertThat(PostCommandHandler.truncationProbeLimit(0)).isEqualTo(0);
    assertThat(PostCommandHandler.truncationProbeLimit(-1)).isEqualTo(-1);
  }
}
