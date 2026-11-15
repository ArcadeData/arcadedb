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
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11879: a MATCH whose {@code where:} has a MATCHES with a side that names no pattern alias threw a NullPointerException
 * while planning. {@code getMatchPatternInvolvedAliases()} answers null for an expression that involves no alias, and
 * {@code MatchesCondition} copied both sides' answers without a null check.
 */
class Issue11879MatchesConditionAliasesTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE VERTEX V SET name = 'abc', l = [1, 2]");
      database.command("sql", "CREATE VERTEX V SET name = 'xyz', l = [3]");
    });
  }

  @Test
  void aMatchesWhoseRightSideNamesNoAliasPlansAndFilters() {
    assertThat(names("MATCH {type: V, as: a, where: (name MATCHES 'a' + '.*')} RETURN a.name AS name")).containsExactly("abc");
    assertThat(names("MATCH {type: V, as: a, where: (l[-1] MATCHES -1 | 'x')} RETURN a.name AS name"))
        .isEqualTo(selectNames("SELECT name FROM V WHERE l[-1] MATCHES -1 | 'x'"));
  }

  @Test
  void aMatchesWhoseLeftSideNamesNoAliasPlansAndFilters() {
    assertThat(names("MATCH {type: V, as: a, where: ('ab' + 'c' MATCHES 'a.*' AND name = 'abc')} RETURN a.name AS name"))
        .containsExactly("abc");
  }

  private Set<String> names(final String match) {
    final Set<String> names = new HashSet<>();
    try (final ResultSet rs = database.query("sql", match)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  private Set<String> selectNames(final String select) {
    return names(select);
  }
}
