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
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12449: the reference columns of {@code pg_type} answered 0 where PostgreSQL 16 names an array type ({@code typarray}), an
 * element type ({@code typelem}) or a collation ({@code typcollation}) the catalog did not model. The reference values below were read
 * from a stock {@code postgres:16} with {@code SELECT oid, typname, typelem, typarray, typcollation, typlen, typbyval, typcategory,
 * typstorage, typinput, typreceive FROM pg_type}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12449PgTypeReferencesTest {
  private static final String DATABASE_PATH = "./target/databases/Issue12449PgTypeReferencesTest";
  private static final String USER          = "root";

  /** oid, typname, typelem, typarray, typcollation, typlen, typbyval, typcategory, typstorage, typinput, typreceive. */
  private static final Object[][] POSTGRESQL_16 = { //
      { 19, "name", 18, 1003, 950, 64, false, "S", "p", "namein", "namerecv" },//
      { 22, "int2vector", 21, 1006, 0, -1, false, "A", "p", "int2vectorin", "int2vectorrecv" },//
      { 26, "oid", 0, 1028, 0, 4, true, "N", "p", "oidin", "oidrecv" },//
      { 1083, "time", 0, 1183, 0, 8, true, "D", "p", "time_in", "time_recv" },//
      { 1184, "timestamptz", 0, 1185, 0, 8, true, "D", "p", "timestamptz_in", "timestamptz_recv" },//
      { 1186, "interval", 0, 1187, 0, 16, false, "T", "p", "interval_in", "interval_recv" },//
      { 1266, "timetz", 0, 1270, 0, 12, false, "D", "p", "timetz_in", "timetz_recv" },//
      { 3802, "jsonb", 0, 3807, 0, -1, false, "U", "x", "jsonb_in", "jsonb_recv" },//
      { 1003, "_name", 19, 0, 950, -1, false, "A", "x", "array_in", "array_recv" },//
      { 1006, "_int2vector", 22, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
      { 1028, "_oid", 26, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
      { 1183, "_time", 1083, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
      { 1185, "_timestamptz", 1184, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
      { 1187, "_interval", 1186, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
      { 1270, "_timetz", 1266, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
      { 3807, "_jsonb", 3802, 0, 0, -1, false, "A", "x", "array_in", "array_recv" } };

  /** The columns of {@link #POSTGRESQL_16}, in order. */
  private static final String[] COLUMNS = { "oid", "typname", "typelem", "typarray", "typcollation", "typlen", "typbyval",
      "typcategory", "typstorage", "typinput", "typreceive" };
  /** oid, typname and the three reference columns the issue is about. */
  private static final int      REFERENCE_COLUMNS = 5;

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void theRowsTheIssueNamesAnswerWhatPostgreSQL16Answers() {
    final String projection = String.join(", ", COLUMNS);
    for (final Object[] expected : POSTGRESQL_16) {
      // both routes a client reaches pg_type by: the recogniser and the generic catalog
      final List<Map<String, Object>> recognised = PostgresTypeCatalog.resolve(
          "SELECT " + projection + " FROM pg_type WHERE oid = " + expected[0]);
      final List<Map<String, Object>> generic = resolve("SELECT " + projection + " FROM pg_catalog.pg_type WHERE typname = '" + expected[1]
          + "'").rows;
      // every column of the array types this issue adds; the reference columns of the scalars, whose attribute columns are #12313's
      final int columns = ((String) expected[1]).startsWith("_") ? COLUMNS.length : REFERENCE_COLUMNS;
      for (final List<Map<String, Object>> rows : List.of(recognised, generic)) {
        assertThat(rows).as("the row of %s", expected[1]).hasSize(1);
        for (int i = 0; i < columns; i++)
          assertThat(normalized(rows.getFirst().get(COLUMNS[i]))).as("%s.%s", expected[1], COLUMNS[i]).isEqualTo(normalized(expected[i]));
      }
    }
  }

  @Test
  void theCollationOfNameIsARowOfPgCollation() {
    final PostgresCatalog.Answer collations = resolve("select oid, collname, collprovider, collcollate from pg_collation");
    assertThat(column(collations, "oid")).containsExactly(100, 950);
    assertThat(column(collations, "collname")).containsExactly("default", "C");
    assertThat(column(collations, "collcollate")).containsExactly(null, "C");

    // a join on typcollation reads the collation the type names
    assertThat(column(resolve("select t.typname, c.collname from pg_catalog.pg_type t join pg_catalog.pg_collation c "
        + "on c.oid = t.typcollation where t.typname in ('name', 'text', '_name')"), "collname")).containsExactlyInAnyOrder("C",
        "default", "C");
  }

  @Test
  void theElementJoinFollowsTypelem() {
    // the self-join a driver resolves an array's element with, now also for the arrays #12449 adds, and for the typelem of name
    assertThat(PostgresTypeCatalog.resolve("SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 1183 AND t.typelem = e.oid"))
        .containsExactly(Map.of("typname", "time"));
    assertThat(PostgresTypeCatalog.resolve("SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 19 AND t.typelem = e.oid"))
        .containsExactly(Map.of("typname", "char"));
  }

  @Test
  void nameAndInt2vectorAreNotArraysOfThisProtocol() {
    assertThat(PostgresType.NAME.isArrayType()).isFalse();
    assertThat(PostgresType.INT2VECTOR.isArrayType()).isFalse();
    assertThat(PostgresType.ARRAY_NAME.isArrayType()).isTrue();
    assertThat(PostgresType.ARRAY_NAME.sqlName()).isEqualTo("name[]");
    assertThat(PostgresType.ARRAY_TIMESTAMPTZ.sqlName()).isEqualTo("timestamp with time zone[]");
  }

  @Test
  void theNewArrayTypesAreReadAsParameters() {
    assertThat(PostgresType.deserialize(1183, 0, bytes("{12:00:00,13:30:15}"))).isEqualTo(List.of(LocalTime.of(12, 0),
        LocalTime.of(13, 30, 15)));
    assertThat(PostgresType.deserialize(1028, 0, bytes("{1,4294967295}"))).isEqualTo(List.of(1L, 4294967295L));
    assertThat(PostgresType.deserialize(1003, 0, bytes("{a,b}"))).isEqualTo(List.of("a", "b"));

    // binary: time[] of two elements, microseconds since midnight
    final ByteBuffer times = arrayHeader(2, 1083);
    times.putInt(8).putLong(43_200_000_000L).putInt(8).putLong(48_615_000_000L);
    assertThat(PostgresType.deserialize(1183, 1, times.array())).isEqualTo(List.of(LocalTime.of(12, 0), LocalTime.of(13, 30, 15)));

    // binary: oid[] holds unsigned 32-bit numbers
    final ByteBuffer oids = arrayHeader(2, 26);
    oids.putInt(4).putInt(7).putInt(4).putInt(-1);
    assertThat(PostgresType.deserialize(1028, 1, oids.array())).isEqualTo(List.of(7L, 4294967295L));

    // binary: timetz[] of one element, the offset in seconds WEST of UTC
    final ByteBuffer timetz = arrayHeader(1, 1266);
    timetz.putInt(12).putLong(3_600_000_000L).putInt(-7200);
    assertThat(PostgresType.deserialize(1270, 1, timetz.array())).isEqualTo(List.of(OffsetTime.of(LocalTime.of(1, 0),
        ZoneOffset.ofHours(2))));

    // binary: interval[] of one element: microseconds, days, months
    final ByteBuffer interval = arrayHeader(1, 1186);
    interval.putInt(16).putLong(1_500_000L).putInt(2).putInt(3);
    assertThat(PostgresType.deserialize(1187, 1, interval.array())).isEqualTo(List.of(new CypherDuration(3, 2, 1, 500_000_000)));

    // binary: jsonb[] elements carry jsonb_send's version byte
    final byte[] json = bytes("{\"a\":1}");
    final ByteBuffer jsonb = arrayHeader(1, 3802);
    jsonb.putInt(json.length + 1).put((byte) 1).put(json);
    final List<?> documents = (List<?>) PostgresType.deserialize(3807, 1, jsonb.array());
    assertThat(documents).hasSize(1);
    assertThat(documents.getFirst().toString()).contains("\"a\"").contains("1");
  }

  @Test
  void theScalarsOfTheNewArraysAreReadInBinary() {
    assertThat(PostgresType.deserialize(26, 1, ByteBuffer.allocate(4).putInt(-1).array())).isEqualTo(4294967295L);
    assertThat(PostgresType.deserialize(19, 1, bytes("pg_class"))).isEqualTo("pg_class");

    // int2vectorsend is the array format of int2[], and the text format reads "1 3"
    final ByteBuffer vector = arrayHeader(2, 21);
    vector.putInt(2).putShort((short) 1).putInt(2).putShort((short) 3);
    assertThat(PostgresType.deserialize(22, 1, vector.array())).isEqualTo("1 3");
    assertThat(PostgresType.deserialize(22, 0, bytes("1 3"))).isEqualTo("1 3");
  }

  /** A one-dimensional binary array of {@code count} elements of {@code elementOid}, with room for the elements. */
  private static ByteBuffer arrayHeader(final int count, final int elementOid) {
    final ByteBuffer buffer = ByteBuffer.allocate(20 + count * 24);
    buffer.putInt(1).putInt(0).putInt(elementOid).putInt(count).putInt(1);
    return buffer;
  }

  private static byte[] bytes(final String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private static Object normalized(final Object value) {
    return value instanceof Number number ? (Object) number.longValue() : value;
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("answered: %s", query).isNotNull();
    assertThat(answer.rows).as("answered rather than declined: %s", query).isNotNull();
    return answer;
  }

  private static List<Object> column(final PostgresCatalog.Answer answer, final String name) {
    final List<Object> values = new ArrayList<>();
    for (final Map<String, Object> row : answer.rows)
      values.add(row.get(name));
    return values;
  }
}
