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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #10710 (#10697, #10696): SQL {@code min()} / {@code max()} ordered with a raw {@code Comparable} cast, so they refused a BINARY,
 * ARRAY_OF_* or MAP column and a schemaless column mixing two Java representations of one instant, which {@code ORDER BY}, the index
 * and openCypher all order; and SQL {@code DISTINCT}, {@code GROUP BY} and {@code count(DISTINCT)} keyed a temporal by its Java
 * class, so one instant stored as a DATETIME and as a ZONED_DATETIME was two groups where {@code =} and openCypher say it is one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10710MinMaxAndGroupingKeyTest extends TestHelper {

  private static final long MS = 1_600_000_000_000L;

  /**
   * The census: for every property type, two rows, and {@code min()} / {@code max()} name the value {@code ORDER BY ... LIMIT 1}
   * names. A type added to {@link Type} without a case here fails the test, so the next one cannot be missed.
   */
  @Test
  void minAndMaxAgreeWithOrderByForEveryType() {
    database.command("sql", "CREATE DOCUMENT TYPE Target").close();
    database.command("sql", "CREATE DOCUMENT TYPE E").close();
    final RID[] links = new RID[2];
    database.transaction(() -> {
      links[0] = database.newDocument("Target").set("n", 0).save().getIdentity();
      links[1] = database.newDocument("Target").set("n", 1).save().getIdentity();
    });

    for (final Type type : Type.values()) {
      final String typeName = "T_" + type.name();
      database.command("sql", "CREATE DOCUMENT TYPE " + typeName).close();
      database.command("sql", "CREATE PROPERTY " + typeName + ".v " + type.name()).close();

      final Object[] values = switch (type) {
        case BOOLEAN -> new Object[] { false, true };
        case INTEGER -> new Object[] { 1, 2 };
        case SHORT -> new Object[] { (short) 1, (short) 2 };
        case LONG -> new Object[] { 1L, 2L };
        case FLOAT -> new Object[] { 1.5f, 2.5f };
        case DOUBLE -> new Object[] { 1.5d, 2.5d };
        case BYTE -> new Object[] { (byte) 1, (byte) 2 };
        case DECIMAL -> new Object[] { new BigDecimal("1.5"), new BigDecimal("2.5") };
        case STRING -> new Object[] { "a", "b" };
        case DATE -> new Object[] { LocalDate.of(2020, 1, 1), LocalDate.of(2021, 1, 1) };
        case DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS ->
            new Object[] { LocalDateTime.of(2020, 1, 1, 0, 0), LocalDateTime.of(2021, 1, 1, 0, 0) };
        case ZONED_DATETIME -> new Object[] { ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC) };
        case LOCAL_TIME -> new Object[] { LocalTime.of(10, 0), LocalTime.of(11, 0) };
        case OFFSET_TIME -> new Object[] { OffsetTime.of(10, 0, 0, 0, ZoneOffset.UTC), OffsetTime.of(11, 0, 0, 0, ZoneOffset.UTC) };
        case DURATION -> new Object[] { new CypherDuration(0, 1, 0, 0), new CypherDuration(0, 2, 0, 0) };
        case BINARY -> new Object[] { new byte[] { 0, 9 }, new byte[] { 1, 2 } };
        case MAP -> new Object[] { new LinkedHashMap<>(Map.of("a", 0)), new LinkedHashMap<>(Map.of("a", 1)) };
        case LINK -> new Object[] { links[0], links[1] };
        // A collection argument of SQL min()/max() is the set of its elements: the smallest and the largest element of all the rows
        case LIST -> new Object[] { new ArrayList<>(List.of(0, 9)), new ArrayList<>(List.of(1, 2)) };
        case ARRAY_OF_SHORTS -> new Object[] { new short[] { 0, 9 }, new short[] { 1, 2 } };
        case ARRAY_OF_INTEGERS -> new Object[] { new int[] { 0, 9 }, new int[] { 1, 2 } };
        case ARRAY_OF_LONGS -> new Object[] { new long[] { 0, 9 }, new long[] { 1, 2 } };
        case ARRAY_OF_FLOATS -> new Object[] { new float[] { 0, 9 }, new float[] { 1, 2 } };
        case ARRAY_OF_DOUBLES -> new Object[] { new double[] { 0, 9 }, new double[] { 1, 2 } };
        case EMBEDDED -> null;
      };

      if (type == Type.EMBEDDED) {
        // an embedded document has no order of its own: min() refuses it with a message naming the type, rather than a cast error
        database.transaction(() -> {
          for (int i = 0; i < 2; i++) {
            final MutableDocument doc = database.newDocument(typeName);
            doc.newEmbeddedDocument("E", "v").set("x", i);
            doc.save();
          }
        });
        assertThatThrownBy(() -> single("SELECT min(v) AS m FROM " + typeName)).as(type.name())
            .hasStackTraceContaining("min()").hasStackTraceContaining("EmbeddedDocument");
        continue;
      }

      database.transaction(() -> {
        database.newDocument(typeName).set("k", "A").set("v", values[1]).save();
        database.newDocument(typeName).set("k", "B").set("v", values[0]).save();
      });

      final Object min = single("SELECT min(v) AS m FROM " + typeName);
      final Object max = single("SELECT max(v) AS m FROM " + typeName);

      if (type == Type.LIST || type.name().startsWith("ARRAY_OF_")) {
        assertThat(((Number) min).doubleValue()).as(type.name()).isEqualTo(0d);
        assertThat(((Number) max).doubleValue()).as(type.name()).isEqualTo(9d);
        // ORDER BY orders the sequences element by element: [0, 9] before [1, 2]
        assertThat(single("SELECT k AS m FROM " + typeName + " ORDER BY v LIMIT 1")).as(type.name()).isEqualTo("B");
        assertThat(single("SELECT k AS m FROM " + typeName + " ORDER BY v DESC LIMIT 1")).as(type.name()).isEqualTo("A");
        continue;
      }

      final Object first = single("SELECT v AS m FROM " + typeName + " ORDER BY v LIMIT 1");
      final Object last = single("SELECT v AS m FROM " + typeName + " ORDER BY v DESC LIMIT 1");
      assertThat(BinaryComparator.compareTo(first, last)).as(type.name()).isNegative();
      assertThat(BinaryComparator.compareTo(min, first)).as(type.name() + " min " + min + " vs " + first).isZero();
      assertThat(BinaryComparator.compareTo(max, last)).as(type.name() + " max " + max + " vs " + last).isZero();
    }
  }

  /** A record and a RID are one kind, ordered by identity, not by the name of their classes. */
  @Test
  void aRecordAndARidOrderByIdentity() {
    database.command("sql", "CREATE DOCUMENT TYPE R").close();
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument("R").set("n", 0).save().getIdentity();
      rids[1] = database.newDocument("R").set("n", 1).save().getIdentity();
    });
    final List<Object> values = List.of(rids[0], rids[1].asDocument());
    try (final ResultSet rs = database.query("sql", "SELECT max(?) AS m, min(?) AS n", values, values)) {
      final Result row = rs.next();
      assertThat(((Identifiable) row.getProperty("m")).getIdentity()).isEqualTo(rids[1]);
      assertThat(((Identifiable) row.getProperty("n")).getIdentity()).isEqualTo(rids[0]);
    }
  }

  /** An array is a sequence, ordered against another array or a list element by element. */
  @Test
  void sequencesOrderElementByElement() {
    assertThat(BinaryComparator.compareTo(new int[] { 0, 9 }, new int[] { 1, 2 })).isNegative();
    assertThat(BinaryComparator.compareTo(new double[] { 1, 2 }, new long[] { 1, 2 })).isZero();
    assertThat(BinaryComparator.compareTo(new short[] { 1 }, new short[] { 1, 0 })).isNegative();
    assertThat(BinaryComparator.compareTo(new LinkedList<>(List.of(0, 9)), new int[] { 1, 2 })).isNegative();
    assertThat(BinaryComparator.compareTo(new float[] { 2 }, new ArrayList<>(List.of(1)))).isPositive();
  }

  /** #10697: the repro of the report, over declared properties. */
  @Test
  @SuppressWarnings("unchecked")
  void minAndMaxOfABinaryAnArrayAndAMapColumn() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.b BINARY").close();
    database.command("sql", "CREATE PROPERTY T.m MAP").close();
    database.transaction(() -> {
      database.newDocument("T").set("k", "A").set("b", new byte[] { 1, 2 }).set("m", Map.of("a", 1)).save();
      database.newDocument("T").set("k", "B").set("b", new byte[] { 0, 9 }).set("m", Map.of("a", 0)).save();
    });

    assertThat((byte[]) single("SELECT min(b) AS m FROM T")).containsExactly(0, 9);
    assertThat((byte[]) single("SELECT max(b) AS m FROM T")).containsExactly(1, 2);
    assertThat((Map<String, Object>) single("SELECT min(m) AS m FROM T")).containsEntry("a", 0);
    assertThat((Map<String, Object>) single("SELECT max(m) AS m FROM T")).containsEntry("a", 1);
    // unsigned, as the index and ORDER BY order bytes
    database.transaction(() -> database.newDocument("T").set("k", "C").set("b", new byte[] { (byte) 0xFF }).save());
    assertThat((byte[]) single("SELECT max(b) AS m FROM T")).containsExactly(0xFF);
  }

  /** #10696: one instant, three Java representations, a schemaless property. */
  @Test
  void oneInstantInThreeRepresentationsIsOneGroup() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.transaction(() -> {
      database.newVertex("V").set("k", "zoned", "v", ZonedDateTime.ofInstant(Instant.ofEpochMilli(MS), ZoneOffset.UTC)).save();
      database.newVertex("V").set("k", "plain", "v", new Date(MS)).save();
      database.newVertex("V").set("k", "plain2", "v", LocalDateTime.ofInstant(Instant.ofEpochMilli(MS), ZoneOffset.UTC)).save();
    });

    assertThat(count("SELECT DISTINCT v FROM V")).isEqualTo(1);
    assertThat(count("SELECT v, count(*) AS c FROM V GROUP BY v")).isEqualTo(1);
    assertThat(((Number) single("SELECT count(*) AS m FROM V GROUP BY v")).longValue()).isEqualTo(3L);
    assertThat(((Number) single("SELECT count(DISTINCT v) AS m FROM V")).longValue()).isEqualTo(1L);

    final Object min = single("SELECT min(v) AS m FROM V");
    final Object max = single("SELECT max(v) AS m FROM V");
    assertThat(min).isNotNull();
    assertThat(max).isNotNull();
    assertThat(QueryOperatorEquals.equals(min, new Date(MS))).isTrue();
    assertThat(QueryOperatorEquals.equals(max, new Date(MS))).isTrue();

    // and the same answers openCypher gives
    assertThat(((Number) cypher("MATCH (n:V) RETURN count(DISTINCT n.v) AS m")).longValue()).isEqualTo(1L);
  }

  /** #10696: the seven-way probe. SQL answers the groups openCypher answers: the zone-less values join the instant in UTC. */
  @Test
  void sevenRepresentationsGroupAsInOpenCypher() {
    database.command("sql", "CREATE VERTEX TYPE W").close();
    final Instant instant = Instant.ofEpochMilli(MS);
    database.transaction(() -> {
      database.newVertex("W").set("k", "date", "v", new Date(MS)).save();
      database.newVertex("W").set("k", "instant", "v", instant).save();
      database.newVertex("W").set("k", "timestamp", "v", new Timestamp(MS)).save();
      database.newVertex("W").set("k", "ldtUTC", "v", LocalDateTime.ofInstant(instant, ZoneOffset.UTC)).save();
      database.newVertex("W").set("k", "zdtUTC", "v", ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)).save();
      database.newVertex("W").set("k", "odtUTC", "v", OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)).save();
      database.newVertex("W").set("k", "zdtParis", "v", ZonedDateTime.ofInstant(instant, ZoneId.of("Europe/Paris"))).save();
    });

    final long cypherGroups = ((Number) cypher("MATCH (n:W) RETURN count(DISTINCT n.v) AS m")).longValue();
    assertThat(cypherGroups).isEqualTo(2L);
    assertThat(((Number) single("SELECT count(DISTINCT v) AS m FROM W")).longValue()).isEqualTo(cypherGroups);
    assertThat(count("SELECT DISTINCT v FROM W")).isEqualTo(cypherGroups);
    assertThat(count("SELECT v, count(*) FROM W GROUP BY v")).isEqualTo(cypherGroups);

    assertThat(single("SELECT min(v) AS m FROM W")).isNotNull();
    assertThat(single("SELECT max(v) AS m FROM W")).isNotNull();
  }

  /** A BINARY value and a temporal inside a list key by their content too, as the scalars do. */
  @Test
  void binaryAndTemporalElementsOfAListShareAKey() {
    database.command("sql", "CREATE DOCUMENT TYPE L").close();
    database.transaction(() -> {
      database.newDocument("L").set("b", new byte[] { 1, 2 }).set("t", new ArrayList<>(List.of(new Date(MS)))).save();
      database.newDocument("L").set("b", new byte[] { 1, 2 })
          .set("t", new ArrayList<>(List.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(MS), ZoneOffset.UTC)))).save();
    });
    assertThat(count("SELECT DISTINCT b FROM L")).isEqualTo(1);
    assertThat(count("SELECT DISTINCT t FROM L")).isEqualTo(1);
    assertThat(count("SELECT t, count(*) FROM L GROUP BY t")).isEqualTo(1);
  }

  private Object single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.hasNext()).as(sql).isTrue();
      final Object value = rs.next().getProperty("m");
      assertThat(rs.hasNext()).as(sql).isFalse();
      return value;
    }
  }

  private Object cypher(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().getProperty("m");
    }
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.stream().count();
    }
  }
}
