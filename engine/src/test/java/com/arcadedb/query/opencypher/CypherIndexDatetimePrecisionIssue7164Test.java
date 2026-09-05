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
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7164. {@code CREATE INDEX} on an already-populated type declares the indexed
 * property so that the index key has a stable type (issue #4222). The declared type used to come from
 * {@code Type.getTypeByClass(value.getClass())}, which maps every temporal class to {@code DATETIME}, i.e.
 * millisecond precision, no matter what precision the stored values actually carry. Records written before
 * the index kept their microseconds - an undeclared temporal is serialized at the precision of its own value -
 * while every write after it was silently truncated to milliseconds.
 * <p>
 * Creating an index must never narrow the precision already present in the data, so once the inference sees a
 * temporal value it sweeps the type for the widest precision the rows actually hold, with milliseconds as the
 * floor (a value that happens to land on a whole second must not declare the property {@code DATETIME_SECOND}).
 * {@code CREATE CONSTRAINT} shares the same inference and is covered here too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherIndexDatetimePrecisionIssue7164Test {
  private static final LocalDateTime MICROS  = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 123_456_000);
  private static final LocalDateTime NANOS   = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 123_456_789);
  private static final LocalDateTime MILLIS  = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 123_000_000);
  private static final LocalDateTime SECONDS = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 0);

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue7164").create();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private void insert(final String label, final String uuid, final LocalDateTime ts) {
    database.command("cypher", "CREATE (n:" + label + " {uuid:$uuid, created_at:$ts})", Map.of("uuid", uuid, "ts", ts));
  }

  private LocalDateTime readBack(final String label, final String uuid) {
    final ResultSet rs = database.command("cypher",
        "MATCH (n:" + label + ") WHERE n.uuid = $uuid RETURN n.created_at AS ts", Map.of("uuid", uuid));
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    // Cypher hands temporal values back wrapped in its own value type.
    final Object ts = row.getProperty("ts");
    return ts instanceof CypherLocalDateTime cypherValue ? cypherValue.getValue() : (LocalDateTime) ts;
  }

  @Test
  void indexOnPopulatedTypePreservesMicrosecondPrecision() {
    insert("Ev", "before", MICROS);
    database.command("cypher", "CREATE INDEX ev_created IF NOT EXISTS FOR (n:Ev) ON (n.created_at)");
    insert("Ev", "after", MICROS);

    assertThat(database.getSchema().getType("Ev").getProperty("created_at").getType()).isEqualTo(Type.DATETIME_MICROS);
    assertThat(readBack("Ev", "before")).isEqualTo(MICROS);
    assertThat(readBack("Ev", "after")).isEqualTo(MICROS);
  }

  @Test
  void indexOnPopulatedTypePreservesNanosecondPrecision() {
    insert("Nano", "before", NANOS);
    database.command("cypher", "CREATE INDEX nano_created IF NOT EXISTS FOR (n:Nano) ON (n.created_at)");
    insert("Nano", "after", NANOS);

    assertThat(database.getSchema().getType("Nano").getProperty("created_at").getType()).isEqualTo(Type.DATETIME_NANOS);
    assertThat(readBack("Nano", "before")).isEqualTo(NANOS);
    assertThat(readBack("Nano", "after")).isEqualTo(NANOS);
  }

  @Test
  void widestPrecisionAmongSampledRecordsWins() {
    // The first record carries only milliseconds: sampling must not stop there and declare DATETIME, or the
    // microseconds already stored on the second record would be truncated on every subsequent write.
    insert("Mixed", "milli", MILLIS);
    insert("Mixed", "micro", MICROS);
    database.command("cypher", "CREATE INDEX mixed_created IF NOT EXISTS FOR (n:Mixed) ON (n.created_at)");
    insert("Mixed", "after", MICROS);

    assertThat(database.getSchema().getType("Mixed").getProperty("created_at").getType()).isEqualTo(Type.DATETIME_MICROS);
    assertThat(readBack("Mixed", "micro")).isEqualTo(MICROS);
    assertThat(readBack("Mixed", "after")).isEqualTo(MICROS);
  }

  @Test
  void microsecondsBeyondTheSampleCapStillWiden() {
    // The kind lookup samples at most 256 records. The precision sweep must not stop there, or a type whose
    // first 256 rows happen to land on a millisecond boundary would still be truncated.
    for (int i = 0; i < 300; i++)
      insert("Deep", "milli" + i, MILLIS);
    insert("Deep", "micro", MICROS);
    database.command("cypher", "CREATE INDEX deep_created IF NOT EXISTS FOR (n:Deep) ON (n.created_at)");
    insert("Deep", "after", MICROS);

    assertThat(database.getSchema().getType("Deep").getProperty("created_at").getType()).isEqualTo(Type.DATETIME_MICROS);
    assertThat(readBack("Deep", "after")).isEqualTo(MICROS);
  }

  @Test
  void aLaterNonTemporalValueDoesNotResetTheInferredPrecision() {
    // Heterogeneous properties are already an unsupported shape, but the sweep must degrade gracefully: a row whose
    // value is not a temporal reports no precision and is simply skipped, it must not reset the kind settled by the
    // first value nor drag the precision back down to milliseconds.
    insert("Het", "micro", MICROS);
    database.command("cypher", "CREATE (n:Het {uuid:'text', created_at:'not a date'})");
    insert("Het", "micro2", MICROS);
    database.command("cypher", "CREATE INDEX het_created IF NOT EXISTS FOR (n:Het) ON (n.created_at)");

    assertThat(database.getSchema().getType("Het").getProperty("created_at").getType()).isEqualTo(Type.DATETIME_MICROS);
    assertThat(readBack("Het", "micro")).isEqualTo(MICROS);
  }

  @Test
  void nonTemporalPropertyStillInfersFromTheFirstValue() {
    // Non-temporal inference (issue #4222) is unchanged: the first non-null value decides the type.
    database.command("cypher", "CREATE (n:Num {uuid:'a', val:1})");
    database.command("cypher", "CREATE INDEX num_val IF NOT EXISTS FOR (n:Num) ON (n.val)");

    assertThat(database.getSchema().getType("Num").getProperty("val").getType()).isEqualTo(Type.INTEGER);
  }

  @Test
  void wholeSecondValuesDoNotNarrowBelowMilliseconds() {
    // A value that lands on a whole second must keep DATETIME as the floor: declaring DATETIME_SECOND would
    // narrow the type below what the engine has always used by default.
    insert("Sec", "before", SECONDS);
    database.command("cypher", "CREATE INDEX sec_created IF NOT EXISTS FOR (n:Sec) ON (n.created_at)");
    insert("Sec", "after", MILLIS);

    assertThat(database.getSchema().getType("Sec").getProperty("created_at").getType()).isEqualTo(Type.DATETIME);
    assertThat(readBack("Sec", "after")).isEqualTo(MILLIS);
  }

  @Test
  void constraintOnPopulatedTypePreservesMicrosecondPrecision() {
    // CREATE CONSTRAINT goes through the same inference as CREATE INDEX.
    insert("Uniq", "before", MICROS);
    database.command("cypher", "CREATE CONSTRAINT uniq_created IF NOT EXISTS FOR (n:Uniq) REQUIRE n.created_at IS UNIQUE");

    assertThat(database.getSchema().getType("Uniq").getProperty("created_at").getType()).isEqualTo(Type.DATETIME_MICROS);
    assertThat(readBack("Uniq", "before")).isEqualTo(MICROS);
  }

  @Test
  void indexCreatedBeforeAnyDataStillWorks() {
    database.command("cypher", "CREATE INDEX empty_created IF NOT EXISTS FOR (n:Empty) ON (n.created_at)");
    insert("Empty", "after", MICROS);

    assertThat(readBack("Empty", "after")).isEqualTo(MICROS);
  }
}
