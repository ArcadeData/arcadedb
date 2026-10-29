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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10865: a property declared on a SUPERTYPE was invisible to the JSON serializer, which looked the declaration up on
 * the record's own type only. A {@code java.util.Date} for an inherited DATE was written as epoch milliseconds, which the
 * server reads as epoch days, and an inherited DATETIME_MICROS / DATETIME_SECOND lost the column's precision in the output.
 * The second half: a DATE fed the epoch milliseconds of a UTC midnight (what {@code JSONObject} writes for a {@code LocalDate}
 * query parameter, and what an HTTP client sends for a date) was read as a count of days and refused as out of range.
 */
class Issue10865InheritedDateAndMidnightMillisTest extends TestHelper {
  private static final LocalDate DAY        = LocalDate.of(2024, 1, 15);
  private static final long      DAY_MILLIS = DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

  @BeforeEach
  void createSchema() {
    database.transaction(() -> {
      final DocumentType own = database.getSchema().createDocumentType("Own");
      own.createProperty("fecha", Type.DATE);
      own.createProperty("micros", Type.DATETIME_MICROS);
      own.createProperty("seconds", Type.DATETIME_SECOND);

      final DocumentType base = database.getSchema().createDocumentType("Base");
      base.createProperty("fecha", Type.DATE);
      base.createProperty("micros", Type.DATETIME_MICROS);
      base.createProperty("seconds", Type.DATETIME_SECOND);
      database.getSchema().createDocumentType("Child").addSuperType("Base");
      database.getSchema().createDocumentType("GrandChild").addSuperType("Child");
    });
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // map2json (the remote client's write path, and every toJSON() of a record)
  // ---------------------------------------------------------------------------------------------------------------------

  @Test
  void map2jsonEncodesAnInheritedDateAsEpochDays() {
    final Date date = Date.from(DAY.atTime(15, 30).toInstant(ZoneOffset.UTC));
    final Map<String, Object> map = new HashMap<>();
    map.put("fecha", date);

    for (final String typeName : new String[] { "Own", "Child", "GrandChild" }) {
      final JSONObject json = new JsonSerializer(database).map2json(map, database.getSchema().getType(typeName), false);
      assertThat(json.getLong("fecha")).as(typeName).isEqualTo(DAY.toEpochDay());
    }
  }

  @Test
  void map2jsonTypeHintNamesTheInheritedDeclaration() {
    final Map<String, Object> map = new HashMap<>();
    map.put("fecha", Date.from(DAY.atStartOfDay().toInstant(ZoneOffset.UTC)));

    final JSONObject json = new JsonSerializer(database).setIncludeTypeHints(true)
        .map2json(map, database.getSchema().getType("Child"), false);
    assertThat(json.getString("@props")).isEqualTo("fecha:" + Type.DATE.getId());
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // serializeDocument / serializeResult projection (the HTTP API's rendering of a record)
  // ---------------------------------------------------------------------------------------------------------------------

  @Test
  void serializeDocumentFormatsAnInheritedColumnWithItsDeclaredPrecision() {
    final LocalDateTime value = LocalDateTime.of(2024, 1, 15, 10, 20, 30, 123_000_000);
    final Document own = saved("Own", value);
    final Document child = saved("Child", value);
    final Document grandChild = saved("GrandChild", value);

    final JsonSerializer serializer = new JsonSerializer(database);
    final JSONObject expected = serializer.serializeDocument(own);
    assertThat(expected.getString("micros")).endsWith(".123000");

    for (final Document doc : new Document[] { child, grandChild }) {
      final JSONObject json = serializer.serializeDocument(doc);
      assertThat(json.get("micros")).as(doc.getTypeName()).isEqualTo(expected.get("micros"));
      assertThat(json.get("seconds")).as(doc.getTypeName()).isEqualTo(expected.get("seconds"));
      assertThat(json.get("fecha")).as(doc.getTypeName()).isEqualTo(expected.get("fecha"));
    }
  }

  @Test
  void serializeResultProjectionFormatsAnInheritedColumnWithItsDeclaredPrecision() {
    final LocalDateTime value = LocalDateTime.of(2024, 1, 15, 10, 20, 30, 123_000_000);
    final Document own = saved("Own", value);
    final Document child = saved("Child", value);

    final JsonSerializer serializer = new JsonSerializer(database);
    final Object ownMicros = projectedMicros(serializer, own);
    assertThat(ownMicros.toString()).endsWith(".123000");
    assertThat(projectedMicros(serializer, child)).isEqualTo(ownMicros);
  }

  @Test
  void serializeResultOfAWholeRecordFormatsAnInheritedColumnWithItsDeclaredPrecision() {
    final LocalDateTime value = LocalDateTime.of(2024, 1, 15, 10, 20, 30, 123_000_000);
    saved("Own", value);
    saved("Child", value);

    final JsonSerializer serializer = new JsonSerializer(database);
    final Object ownMicros = selectedMicros(serializer, "Own");
    assertThat(ownMicros.toString()).endsWith(".123000");
    assertThat(selectedMicros(serializer, "Child")).isEqualTo(ownMicros);
  }

  @Test
  void jsonGraphSerializerEncodesAnInheritedColumnWithItsDeclaredPrecision() {
    final LocalDateTime value = LocalDateTime.of(2024, 1, 15, 10, 20, 30, 123_456_000);
    final Document own = saved("Own", value);
    final Document child = saved("Child", value);

    final JsonGraphSerializer serializer = JsonGraphSerializer.createJsonGraphSerializer().setPrecisionAwareTemporals(true);
    final Object ownMicros = serializer.serializeGraphElement(own).getJSONObject("p").get("micros");
    assertThat(serializer.serializeGraphElement(child).getJSONObject("p").get("micros")).isEqualTo(ownMicros);
  }

  // ---------------------------------------------------------------------------------------------------------------------
  // DATE conversion of a number (the server's read of a LocalDate query parameter / HTTP date)
  // ---------------------------------------------------------------------------------------------------------------------

  @Test
  void dateParameterSentAsMidnightMillisIsTheSameDay() {
    database.transaction(() -> database.command("sql", "INSERT INTO Own SET fecha = ?", DAY_MILLIS));
    database.transaction(() -> database.command("sql", "INSERT INTO Child SET fecha = ?", DAY_MILLIS));
    database.transaction(() -> database.command("sql", "INSERT INTO Own CONTENT {\"fecha\": " + DAY_MILLIS + "}"));

    assertThat(dates("Own")).containsExactly(DAY, DAY);
    assertThat(dates("Child")).containsExactly(DAY);
  }

  @Test
  void dateStillReadsAPlainNumberAsEpochDays() {
    assertThat(written(DAY.toEpochDay())).isEqualTo(DAY);
    assertThat(written(0L)).isEqualTo(LocalDate.of(1970, 1, 1));
    assertThat(written(-1L)).isEqualTo(LocalDate.of(1969, 12, 31));
    assertThat(written(String.valueOf(DAY.toEpochDay()))).isEqualTo(DAY);
  }

  @Test
  void dateReadsTheMillisOfAnyUtcMidnight() {
    assertThat(written(DAY_MILLIS)).isEqualTo(DAY);
    assertThat(written((int) 86_400_000)).isEqualTo(LocalDate.of(1970, 1, 2));
    assertThat(written(String.valueOf(DAY_MILLIS))).isEqualTo(DAY);
    // the first days after the epoch, whose millis are small enough to be a valid day count too
    assertThat(written(86_400_000L)).isEqualTo(LocalDate.of(1970, 1, 2));
    // before the epoch
    final LocalDate preEpoch = LocalDate.of(1960, 5, 1);
    assertThat(written(preEpoch.toEpochDay() * 86_400_000L)).isEqualTo(preEpoch);
  }

  @Test
  void dateStillRefusesMillisThatAreNotAMidnight() {
    assertThatThrownBy(() -> written(DAY_MILLIS + 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void listOfDateReadsTheMillisOfAUtcMidnight() {
    database.transaction(() -> {
      database.getSchema().getType("Own").createProperty("fechas", Type.LIST, "DATE");
      database.command("sql", "INSERT INTO Own SET fechas = ?", List.of(DAY_MILLIS, DAY.toEpochDay() + 1));
    });
    try (final ResultSet rs = database.query("sql", "SELECT fechas FROM Own")) {
      final List<Object> read = rs.next().getProperty("fechas");
      assertThat(read).containsExactly(DAY, DAY.plusDays(1));
    }
  }

  @Test
  void aComparisonStillReadsTheNumberAsDays() {
    database.transaction(() -> database.newDocument("Own").set("fecha", DAY).save());
    // only a WRITE reads the millis of a midnight as that day: a number compared with a date never equals it (#8913)
    try (final ResultSet rs = database.query("sql", "SELECT FROM Own WHERE fecha = ?", DAY_MILLIS)) {
      assertThat(rs.hasNext()).isFalse();
    }
    assertThat(Type.convert(database, DAY.toEpochDay(), LocalDate.class)).isEqualTo(DAY);
  }

  /**
   * The DATE a value becomes when written into a DATE property.
   */
  private LocalDate written(final Object value) {
    final LocalDate[] result = new LocalDate[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Own").set("fecha", value);
      result[0] = doc.getLocalDate("fecha");
    });
    return result[0];
  }

  private Document saved(final String typeName, final LocalDateTime value) {
    final Document[] result = new Document[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument(typeName);
      doc.set("fecha", value.toLocalDate());
      doc.set("micros", value);
      doc.set("seconds", value.withNano(0));
      doc.save();
      result[0] = doc;
    });
    return result[0];
  }

  /**
   * The branch of serializeResult an openCypher {@code RETURN n} takes: the row is the element, tagged with its projection name.
   */
  private Object projectedMicros(final JsonSerializer serializer, final Document document) {
    final ResultInternal row = new ResultInternal(document);
    row.setMetadata("_projectionName", "n");
    return serializer.serializeResult(database, row).get("micros");
  }

  private Object selectedMicros(final JsonSerializer serializer, final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName)) {
      return serializer.serializeResult(database, rs.next()).get("micros");
    }
  }

  private List<LocalDate> dates(final String typeName) {
    final List<LocalDate> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT fecha FROM " + typeName)) {
      while (rs.hasNext())
        result.add(rs.next().getProperty("fecha"));
    }
    return result;
  }
}
