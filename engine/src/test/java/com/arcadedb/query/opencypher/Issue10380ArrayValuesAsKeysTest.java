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
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10380: a BINARY or ARRAY_OF_* value is a Java array, which has the identity {@code equals()} / {@code hashCode()}, so
 * openCypher {@code DISTINCT}, {@code GROUP BY}, {@code min()} and {@code max()} kept two records holding the very bytes
 * {@code a.b = c.b} reports equal as two values, and {@code min()} / {@code max()} answered whichever row came first. The four
 * mechanisms (equality, distinct / group key, ordering of {@code ORDER BY}, ordering of {@code min()} / {@code max()}) answer one
 * rule now.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10380ArrayValuesAsKeysTest extends TestHelper {

  @Test
  void binaryValuesWithTheSameBytesAreOneDistinctValueAndOneGroup() {
    createType("T", Type.BINARY);
    insert("T", "x", new byte[] { 1, 2, 3 });
    insert("T", "y", new byte[] { 1, 2, 3 });
    insert("T", "z", new byte[] { 9 });

    assertThat(rows("MATCH (n:T) RETURN DISTINCT n.p AS k")).hasSize(2);
    assertThat(scalar("MATCH (n:T) RETURN count(DISTINCT n.p) AS v")).isEqualTo(2L);
    assertThat(scalar("MATCH (n:T) RETURN size(collect(DISTINCT n.p)) AS v")).isEqualTo(2L);
    assertThat(counts("MATCH (n:T) RETURN n.p AS k, count(*) AS v")).containsExactlyInAnyOrder(2L, 1L);
    assertThat(rows("MATCH (n:T) WITH n.p AS k, collect(n.s) AS s RETURN k, s")).hasSize(2);
    assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c.p AS v")).isEqualTo(true);

    // SQL, the control: the same data, the same answers
    assertThat(sqlRows("SELECT DISTINCT p FROM T")).isEqualTo(2);
    assertThat(sqlRows("SELECT p, count(*) FROM T GROUP BY p")).isEqualTo(2);
  }

  @Test
  void unionDeduplicatesBinaryValues() {
    createType("T", Type.BINARY);
    insert("T", "x", new byte[] { 1, 2, 3 });
    insert("T", "y", new byte[] { 1, 2, 3 });

    assertThat(rows("MATCH (n:T) WHERE n.s = 'x' RETURN n.p AS k UNION MATCH (n:T) WHERE n.s = 'y' RETURN n.p AS k")).hasSize(1);
  }

  @Test
  void aBinaryValueInsideAListHasTheSameKey() {
    createType("T", Type.BINARY);
    insert("T", "x", new byte[] { 1, 2, 3 });
    insert("T", "y", new byte[] { 1, 2, 3 });
    insert("T", "z", new byte[] { 1, 2, 4 });

    assertThat(rows("MATCH (n:T) RETURN DISTINCT [n.p] AS k")).hasSize(2);
    assertThat(rows("MATCH (n:T) RETURN DISTINCT {b: n.p} AS k")).hasSize(2);
  }

  @Test
  void binaryKeysOfDifferentBytesStayApart() {
    createType("T", Type.BINARY);
    // same length, same sum, different content: a key that rendered only the length or a checksum would merge them
    insert("T", "x", new byte[] { 1, 2, 3 });
    insert("T", "y", new byte[] { 3, 2, 1 });
    insert("T", "z", new byte[] { 2, 2, 2 });

    assertThat(rows("MATCH (n:T) RETURN DISTINCT n.p AS k")).hasSize(3);
    assertThat(counts("MATCH (n:T) RETURN n.p AS k, count(*) AS v")).containsExactlyInAnyOrder(1L, 1L, 1L);
  }

  @Test
  void minAndMaxOfBinaryDoNotDependOnTheInsertionOrder() {
    createType("A", Type.BINARY);
    createType("B", Type.BINARY);
    insert("A", "first", new byte[] { 1, 2, 3 });
    insert("A", "second", new byte[] { 9 });
    insert("B", "first", new byte[] { 9 });
    insert("B", "second", new byte[] { 1, 2, 3 });

    for (final String type : new String[] { "A", "B" }) {
      final Result row = single("MATCH (n:" + type + ") RETURN min(n.p) AS lo, max(n.p) AS hi");
      assertThat(row.<byte[]>getProperty("lo")).containsExactly(1, 2, 3);
      assertThat(row.<byte[]>getProperty("hi")).containsExactly(9);

      // coll.min / coll.max take the same ordering
      final Result coll = single("MATCH (n:" + type + ") WITH collect(n.p) AS bs RETURN coll.min(bs) AS lo, coll.max(bs) AS hi");
      assertThat(coll.<byte[]>getProperty("lo")).containsExactly(1, 2, 3);
      assertThat(coll.<byte[]>getProperty("hi")).containsExactly(9);

      // and ORDER BY, which already did
      final List<Result> ordered = rows("MATCH (n:" + type + ") RETURN n.p AS k ORDER BY k");
      assertThat(ordered.get(0).<byte[]>getProperty("k")).containsExactly(1, 2, 3);
      assertThat(ordered.get(1).<byte[]>getProperty("k")).containsExactly(9);
    }
  }

  @Test
  void binaryIsOrderedUnsignedByMinAndMax() {
    createType("T", Type.BINARY);
    insert("T", "a", new byte[] { 0x7f });
    insert("T", "b", new byte[] { (byte) 0x80 });
    insert("T", "c", new byte[] {});

    final Result row = single("MATCH (n:T) RETURN min(n.p) AS lo, max(n.p) AS hi");
    assertThat(row.<byte[]>getProperty("lo")).isEmpty();
    assertThat(row.<byte[]>getProperty("hi")).containsExactly((byte) 0x80);
  }

  @Test
  void binaryIsOrderedUnsignedByOrderByToo() {
    createType("T", Type.BINARY);
    insert("T", "a", new byte[] { 0x7f });
    insert("T", "b", new byte[] { (byte) 0x80 });
    insert("T", "c", new byte[] {});

    final List<Result> ordered = rows("MATCH (n:T) RETURN n.s AS s ORDER BY n.p");
    assertThat(ordered.stream().map(r -> r.<String>getProperty("s")).toList()).containsExactly("c", "a", "b");
  }

  @Test
  void twoEmbeddedDocumentsCompareByContentUnderEquals() {
    // the comparison failed with a NullPointerException: an embedded document has no RID
    database.getSchema().createDocumentType("Emb");
    createType("T", Type.EMBEDDED);
    insert("T", "x", new HashMap<>(Map.of("@type", "Emb", "k", 1)));
    insert("T", "y", new HashMap<>(Map.of("@type", "Emb", "k", 1)));
    insert("T", "z", new HashMap<>(Map.of("@type", "Emb", "k", 2)));

    assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c.p AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'z' RETURN a.p = c.p AS v")).isEqualTo(false);
    assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'z' RETURN a.p <> c.p AS v")).isEqualTo(true);
    // an embedded document is never a record
    assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c AS v")).isEqualTo(false);
  }

  @Test
  void aBinaryValueIsNotAListAndComparesByItsBytesUnderTheOperators() {
    createType("T", Type.BINARY);
    insert("T", "a", new byte[] { 1, 2 });
    insert("T", "b", new byte[] { 1, 2 });
    insert("T", "c", new byte[] { 0x7f });
    insert("T", "d", new byte[] { (byte) 0x80 });

    // the bytes of a BINARY value are not the elements of a list, so DISTINCT, GROUP BY and = answer one rule
    assertThat(scalar("MATCH (n:T) WHERE n.s = 'a' RETURN n.p = [1, 2] AS v")).isEqualTo(false);
    assertThat(scalar("MATCH (n:T) WHERE n.s = 'a' RETURN n.p <> [1, 2] AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (a:T), (b:T) WHERE a.s = 'a' AND b.s = 'b' RETURN a.p = b.p AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (a:T), (b:T) WHERE a.s = 'a' AND b.s = 'b' RETURN a.p <> b.p AS v")).isEqualTo(false);
    // and they order unsigned, as ORDER BY and SQL do: 0x7f < 0x80, where the signed bytes said the opposite
    assertThat(scalar("MATCH (a:T), (b:T) WHERE a.s = 'c' AND b.s = 'd' RETURN a.p < b.p AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (a:T), (b:T) WHERE a.s = 'c' AND b.s = 'd' RETURN a.p > b.p AS v")).isEqualTo(false);
    assertThat(scalar("MATCH (n:T) WHERE n.s = 'a' RETURN n.p IN [[1, 2]] AS v")).isEqualTo(false);
    assertThat(rows("MATCH (n:T) WHERE n.p = $bytes RETURN n.s AS s", Map.of("bytes", new byte[] { 1, 2 }))).hasSize(2);
  }

  @Test
  void floatAndDoubleArraysOfTheSameNumbersShareAKey() {
    createType("A", Type.ARRAY_OF_FLOATS);
    createType("B", Type.ARRAY_OF_DOUBLES);
    insert("A", "x", new float[] { 1.0f, -0.0f });
    insert("B", "y", new double[] { 1.0d, 0.0d });

    assertThat(scalar("MATCH (a:A), (b:B) RETURN count(DISTINCT [a.p, b.p]) AS v")).isEqualTo(1L);
    assertThat(scalar("MATCH (a:A), (b:B) RETURN a.p = b.p AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (n) WHERE n:A OR n:B RETURN count(DISTINCT n.p) AS v")).isEqualTo(1L);
  }

  @Test
  void aFloatKeysByItsDecimalFormAsEqualsReadsIt() {
    // 0.1f is 0.10000000149011612 as a double, and equal to 0.1 for =, so it is one value for DISTINCT and GROUP BY too
    createType("A", Type.ARRAY_OF_FLOATS);
    createType("B", Type.ARRAY_OF_DOUBLES);
    insert("A", "x", new float[] { 0.1f, 2.5f });
    insert("B", "y", new double[] { 0.1d, 2.5d });

    assertThat(scalar("MATCH (a:A), (b:B) RETURN a.p = b.p AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (n) WHERE n:A OR n:B RETURN count(DISTINCT n.p) AS v")).isEqualTo(1L);

    createType("S", Type.FLOAT);
    createType("D", Type.DOUBLE);
    insert("S", "x", 0.1f);
    insert("D", "y", 0.1d);
    assertThat(scalar("MATCH (a:S), (b:D) RETURN a.p = b.p AS v")).isEqualTo(true);
    assertThat(scalar("MATCH (n) WHERE n:S OR n:D RETURN count(DISTINCT n.p) AS v")).isEqualTo(1L);
    assertThat(counts("MATCH (n) WHERE n:S OR n:D RETURN n.p AS k, count(*) AS v")).containsExactly(2L);
    assertThat(counts("MATCH (n) WHERE n:A OR n:B RETURN n.p AS k, count(*) AS v")).containsExactly(2L);
  }

  @Test
  void arraysOfNumbersAreValuesToo() {
    final Object[][] cases = {
        { Type.ARRAY_OF_SHORTS, new short[] { 1, 2 }, new short[] { 9 } },
        { Type.ARRAY_OF_INTEGERS, new int[] { 1, 2 }, new int[] { 9 } },
        { Type.ARRAY_OF_LONGS, new long[] { 1, 2 }, new long[] { 9 } },
        { Type.ARRAY_OF_FLOATS, new float[] { 1.5f, 2f }, new float[] { 9f } },
        { Type.ARRAY_OF_DOUBLES, new double[] { 1.5d, 2d }, new double[] { 9d } } };
    for (final Object[] c : cases) {
      final String type = "T" + c[0];
      createType(type, (Type) c[0]);
      insert(type, "x", c[1]);
      insert(type, "y", copy(c[1]));
      insert(type, "z", c[2]);

      assertThat(scalar("MATCH (n:" + type + ") RETURN count(DISTINCT n.p) AS v")).as("%s distinct", c[0]).isEqualTo(2L);
      assertThat(rows("MATCH (n:" + type + ") RETURN DISTINCT n.p AS k")).as("%s rows", c[0]).hasSize(2);
      assertThat(counts("MATCH (n:" + type + ") RETURN n.p AS k, count(*) AS v")).as("%s groups", c[0])
          .containsExactlyInAnyOrder(2L, 1L);
      assertThat(scalar("MATCH (a:" + type + "), (c:" + type + ") WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c.p AS v"))
          .as("%s equality", c[0]).isEqualTo(true);
      assertThat(sqlRows("SELECT DISTINCT p FROM " + type)).as("%s sql", c[0]).isEqualTo(2);

      // min()/max() and ORDER BY answer the same order, whatever came first
      final Result row = single("MATCH (n:" + type + ") RETURN min(n.p) AS lo, max(n.p) AS hi");
      assertThat(Objects.deepEquals(row.getProperty("lo"), c[1])).as("%s min", c[0]).isTrue();
      assertThat(Objects.deepEquals(row.getProperty("hi"), c[2])).as("%s max", c[0]).isTrue();
      final List<Result> ordered = rows("MATCH (n:" + type + ") RETURN n.p AS k ORDER BY k DESC");
      assertThat(Objects.deepEquals(ordered.get(0).getProperty("k"), c[2])).as("%s order desc", c[0]).isTrue();
    }
  }

  @Test
  void anArrayOfNumbersKeepsTheOrderOfTheListOfThoseNumbers() {
    createType("T", Type.ARRAY_OF_INTEGERS);
    insert("T", "a", new int[] { 1, 2, 3 });
    insert("T", "b", new int[] { 1, 2 });
    insert("T", "c", new int[] { 1, 10 });

    final List<Result> ordered = rows("MATCH (n:T) RETURN n.s AS s ORDER BY n.p");
    assertThat(ordered.stream().map(r -> r.<String>getProperty("s")).toList()).containsExactly("b", "a", "c");
    final Result row = single("MATCH (n:T) RETURN min(n.p) AS lo, max(n.p) AS hi");
    assertThat((int[]) row.getProperty("lo")).containsExactly(1, 2);
    assertThat((int[]) row.getProperty("hi")).containsExactly(1, 10);
  }

  /**
   * One enumeration of the engine's value types, so the type a later fix does not reach cannot slip by: two records holding equal
   * values of a type are one value to {@code =}, to {@code DISTINCT}, to {@code GROUP BY} and to {@code min()} / {@code max()}.
   */
  @ParameterizedTest
  @EnumSource(Type.class)
  void everyTypeEqualValuesAreOneValueInAllFourMechanisms(final Type type) {
    final Supplier<Object> value = sample(type);
    database.getSchema().createDocumentType("Emb");
    database.getSchema().createVertexType("T").createProperty("p", type);
    database.getSchema().getType("T").createProperty("s", Type.STRING);
    final Object first = value.get();
    final Object second = value.get();
    database.transaction(() -> {
      database.newVertex("T").set("s", "x").set("p", first).save();
      database.newVertex("T").set("s", "y").set("p", second).save();
    });

    assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c.p AS v")).as("%s equality", type)
        .isEqualTo(true);
    assertThat(scalar("MATCH (n:T) RETURN count(DISTINCT n.p) AS v")).as("%s count distinct", type).isEqualTo(1L);
    assertThat(rows("MATCH (n:T) RETURN DISTINCT n.p AS k")).as("%s distinct", type).hasSize(1);
    assertThat(counts("MATCH (n:T) RETURN n.p AS k, count(*) AS v")).as("%s group by", type).containsExactly(2L);
    final Result row = single("MATCH (n:T) RETURN min(n.p) AS lo, max(n.p) AS hi");
    assertThat(Objects.deepEquals(row.getProperty("lo"), row.getProperty("hi"))).as("%s min = max", type).isTrue();
    assertThat(rows("MATCH (n:T) RETURN n.p AS k ORDER BY k")).as("%s order by", type).hasSize(2);
  }

  /** A fresh value of the type on every call, so the two records of a case never share an array instance. */
  private Supplier<Object> sample(final Type type) {
    return switch (type) {
      case BOOLEAN -> () -> true;
      case INTEGER -> () -> 7;
      case SHORT -> () -> (short) 7;
      case LONG -> () -> 7L;
      case FLOAT -> () -> 1.5f;
      case DOUBLE -> () -> 1.5d;
      case DATETIME -> () -> new Date(1_000_000L);
      case STRING -> () -> "text";
      case BINARY -> () -> new byte[] { 1, 2, 3 };
      case LIST -> () -> new ArrayList<>(List.of(1, 2));
      case MAP -> () -> Map.of("k", 1);
      case LINK -> {
        database.getSchema().createDocumentType("Target");
        final RID[] target = new RID[1];
        database.transaction(() -> target[0] = database.newDocument("Target").save().getIdentity());
        yield () -> target[0];
      }
      case BYTE -> () -> (byte) 3;
      case DATE -> () -> LocalDate.of(2020, 1, 2);
      case DECIMAL -> () -> new BigDecimal("1.50");
      case EMBEDDED -> () -> new HashMap<>(Map.of("@type", "Emb", "k", 1));
      case DATETIME_MICROS -> () -> LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000);
      case DATETIME_NANOS -> () -> LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_789);
      case DATETIME_SECOND -> () -> LocalDateTime.of(2020, 1, 2, 3, 4, 5);
      case ARRAY_OF_SHORTS -> () -> new short[] { 1, 2 };
      case ARRAY_OF_INTEGERS -> () -> new int[] { 1, 2 };
      case ARRAY_OF_LONGS -> () -> new long[] { 1, 2 };
      case ARRAY_OF_FLOATS -> () -> new float[] { 1.5f, 2f };
      case ARRAY_OF_DOUBLES -> () -> new double[] { 1.5d, 2d };
      case OFFSET_TIME -> () -> OffsetTime.of(10, 11, 12, 0, ZoneOffset.ofHours(1));
      case LOCAL_TIME -> () -> LocalTime.of(10, 11, 12);
      case ZONED_DATETIME -> () -> ZonedDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);
      case DURATION -> () -> CypherDuration.parse("PT1H");
    };
  }

  private static Object copy(final Object array) {
    return switch (array) {
      case byte[] a -> a.clone();
      case short[] a -> a.clone();
      case int[] a -> a.clone();
      case long[] a -> a.clone();
      case float[] a -> a.clone();
      case double[] a -> a.clone();
      default -> throw new IllegalArgumentException("Not an array: " + array);
    };
  }

  private void createType(final String name, final Type type) {
    database.transaction(() -> {
      database.getSchema().createVertexType(name).createProperty("p", type);
      database.getSchema().getType(name).createProperty("s", Type.STRING);
    });
  }

  private void insert(final String type, final String label, final Object value) {
    database.transaction(() -> database.newVertex(type).set("s", label).set("p", value).save());
  }

  private List<Result> rows(final String query) {
    final List<Result> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next());
    }
    return out;
  }

  private List<Result> rows(final String query, final Map<String, Object> parameters) {
    final List<Result> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      while (rs.hasNext())
        out.add(rs.next());
    }
    return out;
  }

  private Result single(final String query) {
    final List<Result> rows = rows(query);
    assertThat(rows).hasSize(1);
    return rows.get(0);
  }

  private Object scalar(final String query) {
    return single(query).getProperty("v");
  }

  /** The {@code v} column of every row of a group-by query, as longs. */
  private List<Long> counts(final String query) {
    return rows(query).stream().map(r -> r.<Number>getProperty("v").longValue()).toList();
  }

  private int sqlRows(final String query) {
    int n = 0;
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
    }
    return n;
  }
}
