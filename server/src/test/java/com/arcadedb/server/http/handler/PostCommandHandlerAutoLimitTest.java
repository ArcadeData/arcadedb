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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostCommandHandler#appendAutomaticLimit(String, String, int)}: the automatic
 * trailing-LIMIT heuristic must behave exactly as the previous inline implementation while avoiding a
 * full-command {@code toLowerCase} copy per request. The command passed in is already trimmed, as in the
 * handler.
 */
class PostCommandHandlerAutoLimitTest {

  private static final int LIMIT = 20_000;

  @Test
  void appendsLimitToPlainSelect() {
    assertThat(PostCommandHandler.appendAutomaticLimit("select from V", "sql", LIMIT))
        .isEqualTo("select from V limit " + LIMIT);
  }

  @Test
  void appendsLimitToMatch() {
    assertThat(PostCommandHandler.appendAutomaticLimit("match {type: V, as: v} return v", "sql", LIMIT))
        .isEqualTo("match {type: V, as: v} return v limit " + LIMIT);
  }

  @Test
  void prefixCheckIsCaseInsensitive() {
    assertThat(PostCommandHandler.appendAutomaticLimit("SELECT from V", "sql", LIMIT))
        .isEqualTo("SELECT from V limit " + LIMIT);
  }

  @Test
  void doesNotAppendWhenExplicitLowercaseLimitAlreadyPresent() {
    final String cmd = "select from V limit 5";
    assertThat(PostCommandHandler.appendAutomaticLimit(cmd, "sql", LIMIT)).isEqualTo(cmd);
  }

  @Test
  void doesNotAppendWhenExplicitUppercaseLimitAlreadyPresent() {
    final String cmd = "select from V LIMIT 5";
    assertThat(PostCommandHandler.appendAutomaticLimit(cmd, "sql", LIMIT)).isEqualTo(cmd);
  }

  @Test
  void doesNotAppendWhenCommandEndsWithSemicolon() {
    final String cmd = "select from V;";
    assertThat(PostCommandHandler.appendAutomaticLimit(cmd, "sql", LIMIT)).isEqualTo(cmd);
  }

  @Test
  void doesNotAppendToNonSelectOrMatch() {
    final String cmd = "insert into V set name = 'a'";
    assertThat(PostCommandHandler.appendAutomaticLimit(cmd, "sql", LIMIT)).isEqualTo(cmd);
  }

  @Test
  void doesNotAppendForNonSqlLanguage() {
    final String cmd = "MATCH (n) RETURN n";
    assertThat(PostCommandHandler.appendAutomaticLimit(cmd, "cypher", LIMIT)).isEqualTo(cmd);
  }

  @Test
  void doesNotAppendWhenLimitDisabled() {
    final String cmd = "select from V";
    assertThat(PostCommandHandler.appendAutomaticLimit(cmd, "sql", -1)).isEqualTo(cmd);
  }

  @Test
  void appendsWhenLimitSubstringIsNotAStandaloneClause() {
    // "limitless" must not be mistaken for an existing LIMIT clause.
    final String cmd = "select limitless from V";
    assertThat(PostCommandHandler.appendAutomaticLimit(cmd, "sql", LIMIT))
        .isEqualTo("select limitless from V limit " + LIMIT);
  }

  @Test
  void appendsWhenLimitOnlyOnEarlierLineButNotOnLastLine() {
    // A subquery LIMIT on an earlier line must still let the outer query receive a trailing LIMIT.
    final String cmd = "select from (select from V limit 3)\nwhere x > 1";
    assertThat(PostCommandHandler.appendAutomaticLimit(cmd, "sql", LIMIT))
        .isEqualTo(cmd + " limit " + LIMIT);
  }

  @Test
  void handlesSqlScriptLanguage() {
    assertThat(PostCommandHandler.appendAutomaticLimit("select from V", "sqlScript", LIMIT))
        .isEqualTo("select from V limit " + LIMIT);
  }
}
