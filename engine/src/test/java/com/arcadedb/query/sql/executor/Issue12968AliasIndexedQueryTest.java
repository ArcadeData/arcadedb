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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12968: a statement naming a type by one of its aliases answered no rows whenever the planner used an index, because the
 * plan's type filter asked {@code isSubTypeOf(alias)}, which matched type names only. The alias must answer what the name answers.
 */
class Issue12968AliasIndexedQueryTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.k INTEGER");
    database.command("sql", "CREATE PROPERTY Person.s STRING");
    database.command("sql", "CREATE INDEX ON Person (k) UNIQUE");
    database.command("sql", "CREATE INDEX ON Person (s) FULL_TEXT");
    database.command("sql", "CREATE VERTEX TYPE Student EXTENDS Person");
    database.command("sql", "CREATE EDGE TYPE Knows");
    database.command("sql", "ALTER TYPE Person ALIASES Human");
    database.transaction(() -> {
      for (int i = 1; i <= 5; i++)
        database.command("sql", "INSERT INTO Person SET k = ?, s = ?, u = ?", i, "word" + i, i);
      database.command("sql", "INSERT INTO Student SET k = 6, s = 'word6', u = 6");
      database.command("sql", "CREATE EDGE Knows FROM (SELECT FROM Person WHERE k = 1) TO (SELECT FROM Person WHERE k = 2)");
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "SELECT FROM %s WHERE k = 3", "SELECT FROM %s WHERE k > 2", "SELECT FROM %s WHERE k IN [1, 6]",
      "SELECT FROM %s WHERE k BETWEEN 2 AND 4", "SELECT FROM %s WHERE k = 3 AND u = 3", "SELECT FROM %s WHERE k = 3 OR u = 4",
      "SELECT FROM %s ORDER BY k", "SELECT FROM %s ORDER BY k DESC LIMIT 2", "SELECT FROM %s WHERE s CONTAINSTEXT 'word3'",
      "SELECT FROM %s WHERE u = 3", "SELECT FROM %s WHERE k = 6" })
  void theAliasAnswersWhatTheTypeNameAnswers(final String query) {
    final List<RID> byName = rids(query.formatted("Person"));
    assertThat(byName).as(query).isNotEmpty();
    final List<RID> byAlias = rids(query.formatted("Human"));
    // the same records, and in the same order when the statement asks for one
    if (query.contains("ORDER BY"))
      assertThat(byAlias).as(query.formatted("Human")).containsExactlyElementsOf(byName);
    else
      assertThat(byAlias).as(query.formatted("Human")).containsExactlyInAnyOrderElementsOf(byName);
  }

  @Test
  void matchAndInstanceofThroughTheAlias() {
    assertThat(count("MATCH {type: Human, as: h, where: (k = 1)}-Knows->{type: Human, as: o} RETURN o")).isEqualTo(1);
    assertThat(count("SELECT FROM Person WHERE @this INSTANCEOF 'Human'")).isEqualTo(6);
    assertThat(count("SELECT FROM Student WHERE @this INSTANCEOF 'Human'")).isEqualTo(1);
  }

  @Test
  void updateAndDeleteThroughTheAliasReachTheIndexedRecords() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE Human SET z = 1 WHERE k = 3")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
      try (final ResultSet rs = database.command("sql", "DELETE FROM Human WHERE k = 5")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
    });
    assertThat(count("SELECT FROM Person WHERE z = 1")).isEqualTo(1);
    assertThat(count("SELECT FROM Person WHERE k = 5")).isZero();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.stream().count();
    }
  }

  private List<RID> rids(final String query) {
    final List<RID> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rids.add(rs.next().getIdentity().orElseThrow());
    }
    return rids;
  }
}
