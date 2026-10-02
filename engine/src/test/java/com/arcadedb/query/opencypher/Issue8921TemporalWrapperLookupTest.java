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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8921. A temporal value that went through a {@code WITH}, an {@code UNWIND} or a
 * {@code datetime()} / {@code date()} call is a {@code CypherTemporalValue} wrapper, which the two paths a MATCH uses
 * to find candidates (the index seek and the inline property filter) did not understand: the seek threw an NPE on an
 * indexed property and the inline filter answered no rows at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8921TemporalWrapperLookupTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue8921").create();
    database.getSchema().createVertexType("N");
    database.getSchema().createVertexType("I");
    database.command("sql", "CREATE PROPERTY N.d DATETIME");
    database.command("sql", "CREATE PROPERTY N.dt DATE");
    database.command("sql", "CREATE PROPERTY I.d DATETIME");
    database.command("sql", "CREATE PROPERTY I.dt DATE");
    database.command("sql", "CREATE INDEX ON I (d) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON I (dt) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "N", "I" })
        for (int i = 0; i < 3; i++)
          database.command("cypher", "CREATE (:" + type + " {d: $d, dt: $dt})",
              Map.of("d", LocalDateTime.of(2021, 6, 15, 12, 30, 0), "dt", LocalDate.of(2021, 6, 15)));
    });
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("cypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void both(final String template, final long expected) {
    assertThat(count(template.replace("%s", "N"))).as("unindexed: " + template).isEqualTo(expected);
    assertThat(count(template.replace("%s", "I"))).as("indexed: " + template).isEqualTo(expected);
  }

  @Test
  void withBoundDatetimeInlineForm() {
    both("MATCH (a:N) WITH a.d AS m MATCH (p:%s {d: m}) RETURN count(p) AS c", 9);
  }

  @Test
  void withBoundDatetimeWhereForm() {
    both("MATCH (a:N) WITH a.d AS m MATCH (p:%s) WHERE p.d = m RETURN count(p) AS c", 9);
  }

  @Test
  void withBoundDate() {
    both("MATCH (a:N) WITH a.dt AS m MATCH (p:%s {dt: m}) RETURN count(p) AS c", 9);
    both("MATCH (a:N) WITH a.dt AS m MATCH (p:%s) WHERE p.dt = m RETURN count(p) AS c", 9);
  }

  @Test
  void unwindOfLiterals() {
    both("UNWIND [datetime('2021-06-15T12:30:00')] AS m MATCH (p:%s) WHERE p.d = m RETURN count(p) AS c", 3);
    both("UNWIND [date('2021-06-15')] AS m MATCH (p:%s) WHERE p.dt = m RETURN count(p) AS c", 3);
    both("UNWIND [date('2021-06-15')] AS m MATCH (p:%s {dt: m}) RETURN count(p) AS c", 3);
  }

  @Test
  void literalConstructors() {
    both("MATCH (p:%s {d: datetime('2021-06-15T12:30:00')}) RETURN count(p) AS c", 3);
    both("MATCH (p:%s) WHERE p.d = datetime('2021-06-15T12:30:00') RETURN count(p) AS c", 3);
  }

  @Test
  void optionalMatchAfterWith() {
    both("MATCH (a:N) WITH a.d AS m OPTIONAL MATCH (p:%s {d: m}) RETURN count(p) AS c", 9);
  }
}
