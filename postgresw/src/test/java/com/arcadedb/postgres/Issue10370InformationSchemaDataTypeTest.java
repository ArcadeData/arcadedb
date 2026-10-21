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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10370: {@code information_schema.columns.data_type} answered the internal type name ({@code int4}, {@code varchar},
 * {@code _text}) where PostgreSQL answers the SQL standard name ({@code integer}, {@code character varying}, {@code ARRAY}). The
 * same row contradicted itself: {@code format_type()} and {@code ::regtype} read {@code integer} for the same column, and
 * {@code data_type} and {@code udt_name} were always equal, which PostgreSQL deliberately keeps apart. The precision columns the
 * relation declares were never written either.
 * <p>
 * The SQL name now lives in one place, {@link PostgresType#sqlName()}, which {@code format_type}, {@code regtype} and the
 * {@code information_schema} row all read, and one test drives every ArcadeDB type through both surfaces so a new type cannot
 * disagree with itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10370InformationSchemaDataTypeTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10370InformationSchemaDataTypeTest";
  private static final String USER          = "root";

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

  /** The reproduction of the issue: one column of each shape, read through information_schema. */
  @Test
  void dataTypeIsTheSqlNameAndUdtNameIsTheInternalOne() {
    database.transaction(() -> {
      final DocumentType person = database.getSchema().createVertexType("Person");
      person.createProperty("age", Type.INTEGER);
      person.createProperty("name", Type.STRING);
      person.createProperty("born", Type.DATETIME);
      person.createProperty("score", Type.DOUBLE);
      person.createProperty("ok", Type.BOOLEAN);
      person.createProperty("tags", Type.LIST, "STRING");
    });

    final Map<String, Map<String, Object>> columns = byColumn(resolve(
        "select column_name, data_type, udt_name from information_schema.columns where table_name = 'Person' order by ordinal_position"));

    assertThat(columns.get("age")).containsEntry("data_type", "integer").containsEntry("udt_name", "int4");
    assertThat(columns.get("name")).containsEntry("data_type", "character varying").containsEntry("udt_name", "varchar");
    assertThat(columns.get("born")).containsEntry("data_type", "timestamp without time zone").containsEntry("udt_name", "timestamp");
    assertThat(columns.get("score")).containsEntry("data_type", "double precision").containsEntry("udt_name", "float8");
    assertThat(columns.get("ok")).containsEntry("data_type", "boolean").containsEntry("udt_name", "bool");
    // PostgreSQL puts the literal ARRAY in data_type for any array column: the element type is in element_types, and "_text" is
    // not a value data_type can ever hold
    assertThat(columns.get("tags")).containsEntry("data_type", "ARRAY").containsEntry("udt_name", "_text");
  }

  /**
   * The guarantee that stops the family coming back: for a column of EVERY ArcadeDB type, {@code format_type()} and
   * {@code data_type} name the same type, except that an array is {@code ARRAY} in the second and {@code <element>[]} in the first.
   */
  @Test
  void everyTypeIsNamedTheSameByFormatTypeAndByTheInformationSchema() {
    database.transaction(() -> database.getSchema().createDocumentType("AllTypes"));
    final DocumentType type = database.getSchema().getType("AllTypes");
    final List<Type> created = new ArrayList<>();
    for (final Type arcadeType : Type.values()) {
      try {
        database.transaction(() -> type.createProperty("p_" + arcadeType.name(), arcadeType));
        created.add(arcadeType);
      } catch (final RuntimeException e) {
        // a type a property cannot be declared with on its own has no column to compare
      }
    }
    assertThat(created).as("columns compared").hasSizeGreaterThan(15);

    final Map<String, Map<String, Object>> formatted = byColumn(resolve(
        "select a.attname as column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) as ft, a.atttypid::pg_catalog.regtype as rt "
            + "from pg_catalog.pg_attribute a, pg_catalog.pg_class c where a.attrelid = c.oid and c.relname = 'AllTypes' "
            + "order by a.attnum"));
    final Map<String, Map<String, Object>> information = byColumn(resolve(
        "select column_name, data_type, udt_name from information_schema.columns where table_name = 'AllTypes' order by ordinal_position"));

    assertThat(information.keySet()).containsExactlyInAnyOrderElementsOf(formatted.keySet());
    for (final String column : information.keySet()) {
      final String formatType = (String) formatted.get(column).get("ft");
      final String dataType = (String) information.get(column).get("data_type");
      final String udtName = (String) information.get(column).get("udt_name");

      assertThat(formatted.get(column).get("rt")).as("regtype of %s", column).isEqualTo(formatType);
      if (formatType.endsWith("[]")) {
        assertThat(dataType).as("data_type of the array column %s", column).isEqualTo("ARRAY");
        assertThat(udtName).as("udt_name of the array column %s", column).startsWith("_");
      } else {
        assertThat(dataType).as("data_type of %s must read what format_type() reads", column).isEqualTo(formatType);
        // udt_name is the pg_type name, so it differs from the SQL name exactly where PostgreSQL's does
        assertThat(udtName).as("udt_name of %s", column).isNotNull();
      }
    }
  }

  /** {@code sqlName()} is the single accessor: every constant answers a name, and an array answers its element's, bracketed. */
  @Test
  void everyPostgresTypeHasOneSqlName() {
    for (final PostgresType type : PostgresType.values()) {
      assertThat(type.sqlName()).as("sqlName of %s", type).isNotBlank();
      if (type.isArrayType()) {
        final PostgresType element = PostgresType.byCode(type.elementCode);
        assertThat(type.sqlName()).as("sqlName of %s", type).isEqualTo(element.sqlName() + "[]");
      }
    }
    assertThat(PostgresType.INTEGER.sqlName()).isEqualTo("integer");
    assertThat(PostgresType.VARCHAR.sqlName()).isEqualTo("character varying");
    assertThat(PostgresType.ARRAY_TEXT.sqlName()).isEqualTo("text[]");
    assertThat(PostgresType.INTEGER.typeName).as("the pg_type name is a different thing and stays").isEqualTo("int4");
  }

  /** The precision columns the relation declares, written for the built-in types that have them (typmod -1: none declared). */
  @Test
  void thePrecisionColumnsAreWrittenAsPostgreSqlWritesThem() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Precise");
      type.createProperty("i", Type.INTEGER);
      type.createProperty("s", Type.SHORT);
      type.createProperty("l", Type.LONG);
      type.createProperty("f", Type.FLOAT);
      type.createProperty("d", Type.DOUBLE);
      type.createProperty("n", Type.DECIMAL);
      type.createProperty("t", Type.DATETIME);
      type.createProperty("dt", Type.DATE);
      type.createProperty("v", Type.STRING);
      type.createProperty("b", Type.BOOLEAN);
    });

    final Map<String, Map<String, Object>> columns = byColumn(resolve(
        "select column_name, character_maximum_length, character_octet_length, numeric_precision, numeric_precision_radix, "
            + "numeric_scale, datetime_precision from information_schema.columns where table_name = 'Precise' order by ordinal_position"));

    assertNumbers(columns.get("i"), null, null, 32, 2, 0, null);
    assertNumbers(columns.get("s"), null, null, 16, 2, 0, null);
    assertNumbers(columns.get("l"), null, null, 64, 2, 0, null);
    assertNumbers(columns.get("f"), null, null, 24, 2, null, null);
    assertNumbers(columns.get("d"), null, null, 53, 2, null, null);
    // NUMERIC with no declared precision has none to report, and is the one type counted in base 10
    assertNumbers(columns.get("n"), null, null, null, 10, null, null);
    assertNumbers(columns.get("t"), null, null, null, null, null, 6);
    assertNumbers(columns.get("dt"), null, null, null, null, null, 0);
    // a varchar with no declared length has no maximum, and PostgreSQL's octet length for it is 2^30
    assertNumbers(columns.get("v"), null, 1_073_741_824, null, null, null, null);
    assertNumbers(columns.get("b"), null, null, null, null, null, null);
  }

  private static void assertNumbers(final Map<String, Object> column, final Integer characterMaximumLength,
      final Integer characterOctetLength, final Integer numericPrecision, final Integer radix, final Integer numericScale,
      final Integer datetimePrecision) {
    assertThat(column).as("column row").isNotNull();
    assertThat(column.get("character_maximum_length")).as("character_maximum_length of %s", column.get("column_name"))
        .isEqualTo(characterMaximumLength);
    assertThat(column.get("character_octet_length")).as("character_octet_length of %s", column.get("column_name"))
        .isEqualTo(characterOctetLength);
    assertThat(column.get("numeric_precision")).as("numeric_precision of %s", column.get("column_name")).isEqualTo(numericPrecision);
    assertThat(column.get("numeric_precision_radix")).as("numeric_precision_radix of %s", column.get("column_name")).isEqualTo(radix);
    assertThat(column.get("numeric_scale")).as("numeric_scale of %s", column.get("column_name")).isEqualTo(numericScale);
    assertThat(column.get("datetime_precision")).as("datetime_precision of %s", column.get("column_name")).isEqualTo(datetimePrecision);
  }

  private List<Map<String, Object>> resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("no answer: %s", query).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    return answer.rows;
  }

  private static Map<String, Map<String, Object>> byColumn(final List<Map<String, Object>> rows) {
    final Map<String, Map<String, Object>> byName = new HashMap<>();
    for (final Map<String, Object> row : rows)
      byName.put((String) row.get("column_name"), row);
    return byName;
  }
}
