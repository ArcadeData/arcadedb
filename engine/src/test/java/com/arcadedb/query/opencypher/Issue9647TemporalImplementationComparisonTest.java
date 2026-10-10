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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinarySerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9647: a stored DATE or DATETIME must answer the same Cypher temporal predicates whatever Java class
 * {@code arcadedb.dateImplementation} / {@code arcadedb.dateTimeImplementation} deserializes it into. A DATE read back as a
 * {@code java.util.Date}, a {@code LocalDateTime} or a {@code Calendar} matched no {@code date()} literal, and a DATETIME read
 * back as a {@code Calendar} matched nothing at all.
 */
class Issue9647TemporalImplementationComparisonTest extends TestHelper {
  private static final LocalDate DAY = LocalDate.of(2024, 2, 29);

  @ParameterizedTest
  @ValueSource(classes = { LocalDate.class, Date.class, LocalDateTime.class, Calendar.class })
  void storedDateMatchesDateLiteralUnderEveryDateImplementation(final Class<?> implementation) {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Object previous = serializer.getDateImplementation();
    try {
      serializer.setDateImplementation(implementation);
      database.transaction(() -> {
        database.getSchema().createVertexType("D").createProperty("d", Type.DATE);
        final VertexType indexed = database.getSchema().createVertexType("DI");
        indexed.createProperty("d", Type.DATE);
        indexed.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "d");
        for (final String type : new String[] { "D", "DI" }) {
          database.newVertex(type).set("n", 1).set("d", LocalDate.of(2022, 6, 1)).save();
          database.newVertex(type).set("n", 2).set("d", DAY).save();
          database.newVertex(type).set("n", 3).set("d", LocalDate.of(2025, 3, 1)).save();
        }
      });

      // the setting took effect: otherwise the run silently degrades to the default LocalDate path
      assertThat(database.iterateType("D", false).next().asDocument().get("d")).isInstanceOf(implementation);

      for (final String type : new String[] { "D", "DI" }) {
        assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d = date('2024-02-29') RETURN count(n) AS c")).isEqualTo(1L);
        assertThat(cypherCount("MATCH (n:" + type + " {d: date('2024-02-29')}) RETURN count(n) AS c")).isEqualTo(1L);
        assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d > date('2023-01-01') RETURN count(n) AS c")).isEqualTo(2L);
        assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d <= date('2024-02-29') RETURN count(n) AS c")).isEqualTo(2L);
        assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d <> date('2024-02-29') RETURN count(n) AS c")).isEqualTo(2L);
        assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.d.year = 2024 RETURN count(n) AS c")).isEqualTo(1L);
        // chained access on a non-variable base reads the property the way the variable-bound access does
        assertThat(cypherCount("MATCH (n:" + type + ") WITH {node: n} AS m WHERE m.node.d = date('2024-02-29') RETURN count(*) AS c"))
            .isEqualTo(1L);

        try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type + " {n: 2}) RETURN toString(n.d) AS s")) {
          assertThat((String) rs.next().getProperty("s")).isEqualTo("2024-02-29");
        }

        // SQL is the control: a scan and an index agree on the same predicate
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE d = date('2024-02-29','yyyy-MM-dd')")) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
        }
      }

      // MERGE is the MATCH of its pattern: it finds the stored row instead of creating a duplicate
      database.transaction(() -> database.command("opencypher", "MERGE (n:D {d: date('2024-02-29')})").close());
      assertThat(cypherCount("MATCH (n:D) RETURN count(n) AS c")).isEqualTo(3L);
    } finally {
      serializer.setDateImplementation(previous);
    }
  }

  @ParameterizedTest
  @ValueSource(classes = { LocalDateTime.class, Date.class, ZonedDateTime.class, Instant.class, Calendar.class })
  void storedDateTimeMatchesDateTimeLiteralUnderEveryDateTimeImplementation(final Class<?> implementation) {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Object previous = serializer.getDateTimeImplementation();
    try {
      serializer.setDateTimeImplementation(implementation);
      database.transaction(() -> {
        database.getSchema().createVertexType("E").createProperty("ts", Type.DATETIME);
        final VertexType indexed = database.getSchema().createVertexType("EI");
        indexed.createProperty("ts", Type.DATETIME);
        indexed.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "ts");
        for (final String type : new String[] { "E", "EI" }) {
          database.newVertex(type).set("n", 1).set("ts", ZonedDateTime.parse("2022-06-01T08:00:00Z")).save();
          database.newVertex(type).set("n", 2).set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save();
          database.newVertex(type).set("n", 3).set("ts", ZonedDateTime.parse("2027-03-01T09:30:00Z")).save();
        }
      });

      assertThat(database.iterateType("E", false).next().asDocument().get("ts")).isInstanceOf(implementation);

      for (final String type : new String[] { "E", "EI" }) {
        assertThat(where(type, "e.ts = datetime('2026-01-01T11:00:00Z')")).isEqualTo(1L);
        assertThat(where(type, "e.ts >= datetime('2026-01-01T11:00:00Z')")).isEqualTo(2L);
        assertThat(where(type, "e.ts < datetime('2026-01-01T11:00:01Z')")).isEqualTo(2L);
        assertThat(where(type, "e.ts = datetime('2026-01-01T12:00:00+01:00')")).isEqualTo(1L);
        assertThat(where(type, "e.ts = datetime('2026-01-01T11:00:00[UTC]')")).isEqualTo(1L);
        assertThat(where(type, "e.ts > datetime('2023-01-01T00:00:00Z')")).isEqualTo(2L);
        assertThat(where(type, "e.ts.year = 2026")).isEqualTo(1L);
        assertThat(cypherCount("MATCH (e:" + type + " {ts: datetime('2026-01-01T11:00:00Z')}) RETURN count(e) AS c")).isEqualTo(1L);

        try (final ResultSet rs = database.query("opencypher", "MATCH (e:" + type + " {n: 2}) RETURN toString(e.ts) AS s")) {
          final String rendered = rs.next().getProperty("s");
          assertThat(rendered).doesNotContain("Calendar").startsWith("2026-01-01T11:00");
        }
      }
    } finally {
      serializer.setDateTimeImplementation(previous);
    }
  }

  @ParameterizedTest
  @ValueSource(classes = { Date.class, LocalDateTime.class, Calendar.class })
  void storedDateOnAnEdgeMatchesInlineDateLiteral(final Class<?> implementation) {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Object previous = serializer.getDateImplementation();
    try {
      serializer.setDateImplementation(implementation);
      database.transaction(() -> {
        database.getSchema().createVertexType("V");
        database.getSchema().createEdgeType("R").createProperty("d", Type.DATE);
        database.command("opencypher", "CREATE (:V {n: 1})-[:R {d: date('2024-02-29')}]->(:V {n: 2})").close();
      });

      assertThat(cypherCount("MATCH (a:V)-[r:R {d: date('2024-02-29')}]->(b:V) RETURN count(r) AS c")).isEqualTo(1L);
      assertThat(cypherCount("MATCH (a:V {n: 1})-[r:R {d: date('2024-02-29')}]->(b) RETURN count(r) AS c")).isEqualTo(1L);
      assertThat(cypherCount("MATCH (a:V)-[r:R]->(b:V) WHERE r.d = date('2024-02-29') RETURN count(r) AS c")).isEqualTo(1L);
      // MERGE of the relationship finds the stored one instead of creating a second
      database.transaction(() -> database.command("opencypher",
          "MATCH (a:V {n: 1}), (b:V {n: 2}) MERGE (a)-[:R {d: date('2024-02-29')}]->(b)").close());
      assertThat(cypherCount("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
    } finally {
      serializer.setDateImplementation(previous);
    }
  }

  @ParameterizedTest
  @ValueSource(classes = { Date.class, Calendar.class })
  void preEpochDateKeepsItsDay(final Class<?> implementation) {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Object previous = serializer.getDateImplementation();
    try {
      serializer.setDateImplementation(implementation);
      database.transaction(() -> {
        database.getSchema().createVertexType("D").createProperty("d", Type.DATE);
        database.newVertex("D").set("d", LocalDate.of(1960, 5, 1)).save();
      });
      // a negative epoch is floored to its own day, not rounded to the next one
      assertThat(cypherCount("MATCH (n:D) WHERE n.d = date('1960-05-01') RETURN count(n) AS c")).isEqualTo(1L);
      assertThat(cypherCount("MATCH (n:D {d: date('1960-05-01')}) RETURN count(n) AS c")).isEqualTo(1L);
    } finally {
      serializer.setDateImplementation(previous);
    }
  }

  @ParameterizedTest
  @ValueSource(classes = { ZonedDateTime.class, Calendar.class })
  void calendarParameterComparesByItsInstant(final Class<?> implementation) {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Object previous = serializer.getDateTimeImplementation();
    try {
      serializer.setDateTimeImplementation(implementation);
      database.transaction(() -> {
        database.getSchema().createVertexType("E").createProperty("ts", Type.DATETIME);
        database.newVertex("E").set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save();
      });

      // a java.util.Calendar parameter (an embedded or Java client) carries an instant, as a java.util.Date one does
      // in a zone other than UTC, where the local day differs from the UTC one: it still compares by its instant
      final Calendar parameter = Calendar.getInstance(TimeZone.getTimeZone("Pacific/Kiritimati"));
      parameter.setTimeInMillis(Instant.parse("2026-01-01T11:00:00Z").toEpochMilli());
      try (final ResultSet rs = database.query("opencypher", "MATCH (e:E) WHERE e.ts = $p RETURN count(e) AS c", "p", parameter)) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (e:E) WHERE datetime('2026-01-01T12:00:00+01:00') = $p RETURN count(e) AS c",
          "p", parameter)) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      }
      try (final ResultSet rs = database.query("opencypher", "RETURN $p.year AS c", "p", parameter)) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2026L);
      }
      // SQL reads a Calendar as its instant on either side of '='
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM E WHERE ts = ?", parameter)) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM E WHERE ? = ts", parameter)) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      }
    } finally {
      serializer.setDateTimeImplementation(previous);
    }
  }

  private long where(final String type, final String predicate) {
    return cypherCount("MATCH (e:" + type + ") WHERE " + predicate + " RETURN count(e) AS c");
  }

  private long cypherCount(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
