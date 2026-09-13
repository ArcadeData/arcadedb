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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7567: a TIMESERIES type's columns live in {@code LocalTimeSeriesType.tsColumns}, filled once by
 * {@code CREATE TIMESERIES TYPE}, and the write path reads a document under those names and no others. A property
 * added afterwards with {@code CREATE PROPERTY} landed in the type's ordinary property map instead: it showed up in
 * the schema listing, and every write silently dropped whatever arrived under its name. {@code ALTER PROPERTY ...
 * CUSTOM role = "FIELD"} was the second half of the same misunderstanding - a column's role is fixed at creation and
 * is never read back out of property metadata.
 * <p>
 * Both are now refused at DDL time, which is the closest point to the mistake. The mirror case is refused with them:
 * dropping a declared column's property would leave the engine storing a column the type no longer declares.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7567TimeSeriesPropertyDDLTest extends TestHelper {

  private static final String TYPE = "Reading";

  private void createReadingType() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)");
  }

  /**
   * The shape the reporter expected to work: everything declared inline is stored and read back. This is the control
   * for every rejection below - the rejections must not be the reason the values survive.
   */
  @Test
  void declaredColumnsAreStoredAndReadBack() {
    createReadingType();

    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = 1000, sensor = 'a', value = 7.5"));

    final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE);
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<String>getProperty("sensor")).isEqualTo("a");
    assertThat(((Number) row.getProperty("value")).doubleValue()).isEqualTo(7.5);
  }

  /**
   * Entry point 1: SQL {@code CREATE PROPERTY} on a TIMESERIES type.
   */
  @Test
  void sqlCreatePropertyOnTimeSeriesTypeIsRefused() {
    createReadingType();

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + TYPE + ".humidity DOUBLE"))
        .hasMessageContaining("humidity")
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType(TYPE).existsProperty("humidity")).isFalse();
  }

  /**
   * Entry point 2: the {@code IF NOT EXISTS} form. A name that is NOT a declared column still has to be refused - it
   * reaches the same funnel - while a name that IS one keeps the documented no-op behaviour of issue #7143.
   */
  @Test
  void sqlCreatePropertyIfNotExistsOnTimeSeriesTypeIsRefused() {
    createReadingType();

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + TYPE + ".humidity IF NOT EXISTS DOUBLE"))
        .hasMessageContaining("humidity")
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType(TYPE).existsProperty("humidity")).isFalse();

    final ResultSet existing = database.command("sql", "CREATE PROPERTY " + TYPE + ".value IF NOT EXISTS DOUBLE");
    assertThat(existing.next().<Boolean>getProperty("created")).isFalse();
  }

  /**
   * Entry point 3: the schema API. Every {@code createProperty}/{@code getOrCreateProperty} overload delegates to
   * {@code createProperty(String, Type, String)}, so both spellings below exercise the one funnel that the SQL
   * statement above also reaches.
   */
  @Test
  void schemaApiCreatePropertyOnTimeSeriesTypeIsRefused() {
    createReadingType();
    final DocumentType type = database.getSchema().getType(TYPE);

    assertThatThrownBy(() -> type.createProperty("humidity", Type.DOUBLE))
        .hasMessageContaining("humidity")
        .hasMessageContaining("TIMESERIES");

    assertThatThrownBy(() -> type.getOrCreateProperty("humidity", Type.DOUBLE))
        .hasMessageContaining("humidity")
        .hasMessageContaining("TIMESERIES");

    assertThat(type.existsProperty("humidity")).isFalse();
  }

  /**
   * Entry point 4: {@code ALTER PROPERTY ... CUSTOM role}, the second route in the issue. Any other CUSTOM key stays
   * free-form - the refusal is about the one key that pretends to reconfigure the column.
   */
  @Test
  void sqlAlterPropertyCustomRoleOnTimeSeriesTypeIsRefused() {
    createReadingType();

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".value CUSTOM role = 'TAG'"))
        .hasMessageContaining("role")
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType(TYPE).getProperty("value").getCustomValue("role")).isNull();

    assertThatCode(() -> database.command("sql", "ALTER PROPERTY " + TYPE + ".value CUSTOM unit = 'celsius'"))
        .doesNotThrowAnyException();
    assertThat(database.getSchema().getType(TYPE).getProperty("value").getCustomValue("unit")).isEqualTo("celsius");
  }

  /**
   * Entry point 5: the same key through the schema API, and spelled in any case - SQL lower-cases nothing on the way
   * in, so {@code CUSTOM ROLE} reaches the setter verbatim.
   */
  @Test
  void schemaApiSetCustomRoleOnTimeSeriesTypeIsRefused() {
    createReadingType();
    final Property value = database.getSchema().getType(TYPE).getProperty("value");

    assertThatThrownBy(() -> value.setCustomValue("role", "TAG"))
        .hasMessageContaining("TIMESERIES")
        .hasMessageContaining("FIELD");

    assertThatThrownBy(() -> value.setCustomValue("ROLE", "TAG"))
        .hasMessageContaining("TIMESERIES");

    assertThat(value.getCustomValue("role")).isNull();
  }

  /**
   * Removing the key is not making the claim. A database written before this fix can carry a CUSTOM {@code role} it
   * can no longer set, so the refusal must not also be what stops it being cleaned up: {@code = null} is the
   * documented removal form and stays allowed.
   */
  @Test
  void removingAStaleCustomRoleIsAllowed() throws Exception {
    createReadingType();
    final String databasePath = database.getDatabasePath();
    database.close();

    final File schemaFile = new File(databasePath, "schema.json");
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    final JSONObject value = schema.getJSONObject("types").getJSONObject(TYPE).getJSONObject("properties")
        .getJSONObject("value");
    value.put("custom", new JSONObject().put("role", "TAG"));
    Files.writeString(schemaFile.toPath(), schema.toString(), StandardCharsets.UTF_8);

    database = factory.open();
    assertThat(database.getSchema().getType(TYPE).getProperty("value").getCustomValue("role")).isEqualTo("TAG");

    database.command("sql", "ALTER PROPERTY " + TYPE + ".value CUSTOM role = null");
    assertThat(database.getSchema().getType(TYPE).getProperty("value").getCustomValue("role")).isNull();
  }

  /**
   * Entry point 6: the mirror case. Dropping a declared column's property would leave the engine storing a column the
   * type had stopped declaring - the same divergence seen from the other side.
   */
  @Test
  void sqlDropPropertyOfDeclaredColumnIsRefused() {
    createReadingType();

    for (final String column : new String[] { "ts", "sensor", "value" })
      assertThatThrownBy(() -> database.command("sql", "DROP PROPERTY " + TYPE + "." + column))
          .as("dropping the declared column '%s' must be refused", column)
          .hasMessageContaining(column)
          .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType(TYPE).getPropertyNames()).contains("ts", "sensor", "value");
  }

  /**
   * Back-compat: a database created BEFORE this fix can already carry a stray property, because nothing refused it.
   * Refusing it at schema load would make that database unopenable, so the load path warns and carries on - and
   * {@code DROP PROPERTY} on the stray name stays allowed, which is how the database is cleaned up.
   * <p>
   * The stray property is injected straight into {@code schema.json} because there is, deliberately, no longer an
   * API that can produce one.
   */
  @Test
  void preFixStrayPropertyStillOpensAndCanBeDropped() throws Exception {
    createReadingType();
    final String databasePath = database.getDatabasePath();
    database.close();

    final File schemaFile = new File(databasePath, "schema.json");
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    final JSONObject properties = schema.getJSONObject("types").getJSONObject(TYPE).getJSONObject("properties");
    properties.put("humidity", new JSONObject().put("type", "DOUBLE"));
    Files.writeString(schemaFile.toPath(), schema.toString(), StandardCharsets.UTF_8);

    database = factory.open();

    final DocumentType reopened = database.getSchema().getType(TYPE);
    assertThat(reopened).isInstanceOf(LocalTimeSeriesType.class);
    assertThat(reopened.existsProperty("humidity")).as("the stray property must survive the open").isTrue();
    assertThat(((LocalTimeSeriesType) reopened).isDeclaredColumn("humidity")).isFalse();

    database.command("sql", "DROP PROPERTY " + TYPE + ".humidity");
    assertThat(database.getSchema().getType(TYPE).existsProperty("humidity")).isFalse();

    // And the type still works after the cleanup.
    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE + " SET ts = 2000, sensor = 'b', value = 1.25"));
    final List<Result> rows = database.query("sql", "SELECT FROM " + TYPE).stream().toList();
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("value")).doubleValue()).isEqualTo(1.25);
  }

  /**
   * A plain DOCUMENT type is untouched: the refusals are scoped to TIMESERIES types and nothing else.
   */
  @Test
  void documentTypePropertyDDLIsUnaffected() {
    database.command("sql", "CREATE DOCUMENT TYPE Note");
    database.command("sql", "CREATE PROPERTY Note.body STRING");
    database.command("sql", "ALTER PROPERTY Note.body CUSTOM role = 'whatever'");

    assertThat(database.getSchema().getType("Note").getProperty("body").getCustomValue("role")).isEqualTo("whatever");

    database.command("sql", "DROP PROPERTY Note.body");
    assertThat(database.getSchema().getType("Note").existsProperty("body")).isFalse();
  }
}
