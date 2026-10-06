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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9334: a zoned {@code datetime()} used as an inline property of a MERGE never matched an
 * existing node, because MERGE compared the storage text of the operand against the stored DATETIME, so it created a
 * duplicate where the identical MATCH found the node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9334MergeZonedDatetimeTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue9334");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    for (final String type : new String[] { "Z", "ZN", "U", "S" })
      database.getSchema().createVertexType(type);
    for (final String type : new String[] { "Z", "ZN" })
      database.command("sql", "CREATE PROPERTY " + type + ".d DATETIME");
    database.command("sql", "CREATE PROPERTY S.d STRING");
    database.command("sql", "CREATE INDEX ON Z (d) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON S (d) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "Z", "ZN" })
        database.command("cypher", "CREATE (:" + type + " {d: $d})", Map.of("d", LocalDateTime.of(2021, 6, 15, 12, 30, 0)));
      database.command("cypher", "CREATE (:U {d: datetime('2021-06-15T12:30:00Z')})");
      database.command("cypher", "CREATE (:S {d: datetime('2021-06-15T12:30:00Z')})");
    });
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("cypher", "MATCH (p:" + type + ") RETURN count(p) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void mergeAndAssertNoDuplicate(final String type) {
    database.transaction(() -> database.command("cypher", "MERGE (p:" + type + " {d: datetime('2021-06-15T12:30:00Z')})"));
    assertThat(count(type)).as(type + " after literal MERGE").isEqualTo(1L);
    database.transaction(() -> database.command("cypher", "MERGE (p:" + type + " {d: $d})",
        Map.of("d", ZonedDateTime.of(2021, 6, 15, 12, 30, 0, 0, ZoneOffset.UTC))));
    assertThat(count(type)).as(type + " after parameter MERGE").isEqualTo(1L);
    database.transaction(() -> database.command("cypher", "WITH datetime('2021-06-15T12:30:00Z') AS x MERGE (p:" + type + " {d: x})"));
    assertThat(count(type)).as(type + " after WITH MERGE").isEqualTo(1L);
  }

  @Test
  void mergeOnIndexedDatetimeMatchesTheExistingNode() {
    mergeAndAssertNoDuplicate("Z");
  }

  @Test
  void mergeOnUnindexedDatetimeMatchesTheExistingNode() {
    mergeAndAssertNoDuplicate("ZN");
  }

  @Test
  void mergeOnUndeclaredPropertyStillMatches() {
    mergeAndAssertNoDuplicate("U");
  }

  @Test
  void mergeOnIndexedStringPropertyStillMatches() {
    mergeAndAssertNoDuplicate("S");
  }

  @Test
  void aDifferentInstantStillCreatesANewNode() {
    database.transaction(() -> database.command("cypher", "MERGE (p:Z {d: datetime('2021-06-15T13:30:00Z')})"));
    assertThat(count("Z")).isEqualTo(2L);
  }

  @Test
  void mergeAgreesWithMatchOnAnOffsetOperand() {
    // 14:30+02:00 is the same instant as 12:30Z
    database.transaction(() -> database.command("cypher", "MERGE (p:Z {d: datetime('2021-06-15T14:30:00+02:00')})"));
    assertThat(count("Z")).isEqualTo(1L);
  }

  @Test
  void aStoredNonTemporalTextNeverMakesMergeFailOrMatch() {
    database.transaction(() -> database.command("cypher", "CREATE (:U {d: 'not a date'})"));
    database.transaction(() -> database.command("cypher", "MERGE (p:U {d: datetime('2021-06-15T12:30:00Z')})"));
    // the existing datetime node plus the text one plus nothing new: the text node is skipped, no error
    assertThat(count("U")).isEqualTo(2L);
  }

  @Test
  void naiveOperandControlStillMerges() {
    database.transaction(() -> database.command("cypher", "MERGE (p:Z {d: localdatetime('2021-06-15T12:30:00')})"));
    assertThat(count("Z")).isEqualTo(1L);
  }

  @Test
  void mergeOnIndexedNonTemporalKeysStillMatches() {
    database.getSchema().createVertexType("K");
    database.command("sql", "CREATE PROPERTY K.i INTEGER");
    database.command("sql", "CREATE PROPERTY K.s STRING");
    database.command("sql", "CREATE PROPERTY K.b BOOLEAN");
    database.command("sql", "CREATE INDEX ON K (i) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON K (s) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON K (b) NOTUNIQUE");
    database.transaction(() -> database.command("cypher", "CREATE (:K {i: 7, s: 'x', b: true})"));
    for (final String props : new String[] { "i: 7", "s: 'x'", "b: true" })
      database.transaction(() -> database.command("cypher", "MERGE (k:K {" + props + "})"));
    assertThat(count("K")).isEqualTo(1L);
  }
}
