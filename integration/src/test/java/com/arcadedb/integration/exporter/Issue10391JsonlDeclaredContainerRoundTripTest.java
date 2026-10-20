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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10391: a JSONL EXPORT DATABASE / IMPORT DATABASE round trip aborted on a declared LIST OF DATE, MAP OF DATE, LIST OF
 * BINARY, MAP OF BINARY, MAP OF DOUBLE and MAP OF FLOAT, and silently dropped the scale of a MAP OF DECIMAL.
 * <p>
 * The test enumerates the SCHEMA rather than a bag of values: for every element type it declares the property as a scalar, as
 * a LIST OF and as a MAP OF, writes the awkward values of that type (a non-finite double, a BigDecimal with a trailing zero, a
 * date, a byte[], a long past 2^53), round-trips the database and asserts the value and its class.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10391JsonlDeclaredContainerRoundTripTest {
  private static final String SOURCE_PATH = "target/databases/issue10391-source";
  private static final String TARGET_PATH = "target/databases/issue10391-target";
  private static final String FILE        = "issue10391.jsonl.tgz";

  /** The types a container cannot be declared OF, or that have a round trip of their own. */
  private static final Set<Type> NOT_AN_ELEMENT_TYPE = EnumSet.of(Type.LIST, Type.MAP, Type.EMBEDDED, Type.LINK);

  private static final Set<Type> NOT_A_CONTAINER_ELEMENT = EnumSet.of(Type.ARRAY_OF_SHORTS, Type.ARRAY_OF_INTEGERS,
      Type.ARRAY_OF_LONGS, Type.ARRAY_OF_FLOATS, Type.ARRAY_OF_DOUBLES);

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File("exports/" + FILE).delete();
  }

  /** A type added to {@link Type} without a sample here fails this test, so it cannot reach a release unexamined. */
  @Test
  void everyTypeHasASample() {
    final Set<Type> covered = EnumSet.noneOf(Type.class);
    for (final Arguments sample : samples())
      covered.add((Type) sample.get()[0]);
    final Set<Type> missing = EnumSet.allOf(Type.class);
    missing.removeAll(covered);
    missing.removeAll(NOT_AN_ELEMENT_TYPE);
    assertThat(missing).as("types with no sample in this test").isEmpty();
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("cases")
  void aDeclaredPropertyRoundTripsExactly(final Type elementType, final String shape, final String sampleName,
      final Object sample) {
    final String declaration = switch (shape) {
      case "LIST" -> "LIST OF " + elementType.name();
      case "MAP" -> "MAP OF " + elementType.name();
      default -> elementType.name();
    };
    final Object value = switch (shape) {
      case "LIST" -> new ArrayList<>(List.of(sample));
      case "MAP" -> new LinkedHashMap<>(Map.of("k", sample));
      default -> sample;
    };

    final Object before;
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE D");
      source.command("sql", "CREATE PROPERTY D.v " + declaration);
      source.transaction(() -> source.newDocument("D").set("v", value).save());
      try (final ResultSet rs = source.query("sql", "SELECT FROM D")) {
        before = rs.next().getElement().get().get("v");
      }
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath());
      final Object after;
      try (final ResultSet rs = target.query("sql", "SELECT FROM D")) {
        after = rs.next().getElement().get().get("v");
      }
      assertThat(describe(after)).as(declaration + " " + sampleName).isEqualTo(describe(before));
    }
  }

  /**
   * A file written by a release that had no type tree for a declared container: the DATE is the serializer's epoch milliseconds,
   * the BINARY a JSON array, a non-finite number its marker and a DECIMAL a bare JSON number. All are read back from the
   * declaration, in a LIST and in a MAP alike.
   */
  @Test
  void aFileWrittenBeforeTheTypeTreeCoveredContainersIsStillRead() throws Exception {
    final File file = new File(SOURCE_PATH + "-legacy.jsonl");
    file.getParentFile().mkdirs();
    Files.writeString(file.toPath(), ""
        + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":2}}\n"
        + "{\"t\":\"schema\",\"c\":{\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{\"D\":{\"type\":\"d\",\"parents\":[],\"buckets\":[\"D_0\"],\"properties\":{"
        + "\"ld\":{\"type\":\"LIST\",\"of\":\"DATE\"},\"md\":{\"type\":\"MAP\",\"of\":\"DATE\"},"
        + "\"lb\":{\"type\":\"LIST\",\"of\":\"BINARY\"},\"mb\":{\"type\":\"MAP\",\"of\":\"BINARY\"},"
        + "\"mdb\":{\"type\":\"MAP\",\"of\":\"DOUBLE\"},\"mf\":{\"type\":\"MAP\",\"of\":\"FLOAT\"},\"mdc\":{\"type\":\"MAP\",\"of\":\"DECIMAL\"}"
        + "},\"indexes\":{},\"custom\":{}}}}}\n"
        + "{\"t\":\"d\",\"c\":{\"p\":{\"ld\":[1577923200000],\"md\":{\"k\":1577923200000},\"lb\":[[1,2,3]],\"mb\":{\"k\":[1,2,3]},"
        + "\"mdb\":{\"k\":\"PosInfinity\"},\"mf\":{\"k\":\"NaN\"},\"mdc\":{\"k\":3.70}},\"r\":\"#1:0\",\"t\":\"D\"}}\n");

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath());
      try (final ResultSet rs = target.query("sql", "SELECT FROM D")) {
        final Document d = rs.next().getElement().get();
        assertThat(describe(d.get("ld"))).isEqualTo(describe(List.of(LocalDate.of(2020, 1, 2))));
        assertThat(describe(d.get("md"))).isEqualTo(describe(Map.of("k", LocalDate.of(2020, 1, 2))));
        assertThat(describe(d.get("lb"))).isEqualTo(describe(List.of(new byte[] { 1, 2, 3 })));
        assertThat(describe(d.get("mb"))).isEqualTo(describe(Map.of("k", new byte[] { 1, 2, 3 })));
        assertThat(describe(d.get("mdb"))).isEqualTo(describe(Map.of("k", Double.POSITIVE_INFINITY)));
        assertThat(describe(d.get("mf"))).isEqualTo(describe(Map.of("k", Float.NaN)));
        assertThat(describe(d.get("mdc"))).isEqualTo(describe(Map.of("k", new BigDecimal("3.70"))));
      }
    } finally {
      file.delete();
    }
  }

  /** A declared MAP with a "t" key is a map, not an embedded document of the type that key names. */
  @Test
  void aDeclaredMapWithATKeyIsStillAMap() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE D");
      source.command("sql", "CREATE PROPERTY D.m MAP OF STRING");
      source.transaction(() -> source.newDocument("D").set("m", new LinkedHashMap<>(Map.of("t", "temperature", "p", "x"))).save());
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath());
      try (final ResultSet rs = target.query("sql", "SELECT FROM D")) {
        assertThat(describe(rs.next().getElement().get().get("m"))).isEqualTo(
            describe(Map.of("t", "temperature", "p", "x")));
      }
    }
  }

  /** The value and the class of every element, so a Double read back as a Float or a 3.70 as a 3.7 differs, and a NaN equals a NaN. */
  private static String describe(final Object value) {
    if (value == null)
      return "null";
    if (value instanceof List<?> list) {
      final StringBuilder text = new StringBuilder("[");
      for (final Object item : list)
        text.append(describe(item)).append(',');
      return text.append(']').toString();
    }
    if (value instanceof Map<?, ?> map) {
      final StringBuilder text = new StringBuilder("{");
      for (final Map.Entry<?, ?> entry : new TreeMap<Object, Object>(map).entrySet())
        text.append(entry.getKey()).append('=').append(describe(entry.getValue())).append(',');
      return text.append('}').toString();
    }
    if (value.getClass().isArray()) {
      final StringBuilder text = new StringBuilder(value.getClass().getSimpleName()).append('[');
      for (int i = 0; i < Array.getLength(value); i++)
        text.append(describe(Array.get(value, i))).append(',');
      return text.append(']').toString();
    }
    return value.getClass().getSimpleName() + ":" + value;
  }

  static Stream<Arguments> cases() {
    final List<Arguments> cases = new ArrayList<>();
    for (final Arguments sample : samples()) {
      final Object[] s = sample.get();
      for (final String shape : new String[] { "SCALAR", "LIST", "MAP" })
        // AN ARRAY TYPE CANNOT BE THE ELEMENT OF A CONTAINER: the schema refuses the declaration
        if (shape.equals("SCALAR") || !NOT_A_CONTAINER_ELEMENT.contains((Type) s[0]))
          cases.add(Arguments.of(s[0], shape, s[1], s[2]));
    }
    return cases.stream();
  }

  private static List<Arguments> samples() {
    final LocalDateTime dateTime = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 6_000_000);
    return List.of(//
        sample(Type.BOOLEAN, "true", true), //
        sample(Type.INTEGER, "42", 42), //
        sample(Type.SHORT, "7", (short) 7), //
        sample(Type.LONG, "past 2^53", (1L << 53) + 1), //
        sample(Type.FLOAT, "1.25", 1.25f), //
        sample(Type.FLOAT, "NaN", Float.NaN), //
        sample(Type.DOUBLE, "1.25", 1.25d), //
        sample(Type.DOUBLE, "Infinity", Double.POSITIVE_INFINITY), //
        sample(Type.DOUBLE, "NaN", Double.NaN), //
        sample(Type.DATETIME, "millis", dateTime), //
        sample(Type.DATETIME_SECOND, "seconds", dateTime.withNano(0)), //
        sample(Type.DATETIME_MICROS, "micros", dateTime.withNano(123_456_000)), //
        sample(Type.DATETIME_NANOS, "nanos", dateTime.withNano(123_456_789)), //
        sample(Type.STRING, "text", "hello"), //
        sample(Type.BINARY, "bytes", new byte[] { 1, 2, 3 }), //
        sample(Type.BYTE, "5", (byte) 5), //
        sample(Type.DATE, "2020-01-02", LocalDate.of(2020, 1, 2)), //
        sample(Type.DECIMAL, "trailing zero", new BigDecimal("3.70")), //
        sample(Type.DECIMAL, "past 17 digits", new BigDecimal("12345678901234567890.123456789")), //
        sample(Type.OFFSET_TIME, "offset", OffsetTime.of(1, 2, 3, 0, ZoneOffset.ofHours(2))), //
        sample(Type.LOCAL_TIME, "local", LocalTime.of(1, 2, 3)), //
        sample(Type.ZONED_DATETIME, "zoned", ZonedDateTime.of(dateTime, ZoneOffset.UTC)), //
        sample(Type.DURATION, "P1D", CypherDuration.parse("P1D")), //
        sample(Type.ARRAY_OF_SHORTS, "shorts", new short[] { 1, 2 }), //
        sample(Type.ARRAY_OF_INTEGERS, "ints", new int[] { 1, 2 }), //
        sample(Type.ARRAY_OF_LONGS, "longs", new long[] { 1L, (1L << 53) + 1 }), //
        sample(Type.ARRAY_OF_FLOATS, "floats", new float[] { 1.5f, Float.NaN }), //
        sample(Type.ARRAY_OF_DOUBLES, "doubles", new double[] { 1.5d, Double.POSITIVE_INFINITY }));
  }

  private static Arguments sample(final Type type, final String name, final Object value) {
    return Arguments.of(type, name, value);
  }
}
