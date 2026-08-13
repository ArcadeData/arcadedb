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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6134. A property DEFAULT is stored verbatim by {@code setDefaultValue()} and evaluated as an SQL expression by
 * {@code getDefaultValue()}. Nothing used to validate it in between and the evaluation swallowed every exception, so a
 * `DEFAULT this is (not parseable` silently populated every record of the type with its own source text, a bare
 * `DEFAULT active` silently evaluated to null against the (always null) current record, and the expression was
 * re-parsed on every single record create. The expression is now compiled and validated once, at DDL time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PropertyDefaultValueTest extends TestHelper {

  @Test
  void unparseableDefaultIsRejectedAtDdlTime() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      final Property p = type.createProperty("broken", Type.STRING);

      assertThatThrownBy(() -> p.setDefaultValue("this is (not parseable")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Probe.broken").hasMessageContaining("this is (not parseable");

      // THE REJECTED DEFAULT MUST NOT HAVE BEEN APPLIED
      assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).doesNotContain("broken");

      final MutableDocument doc = database.newDocument("Probe").save();
      assertThat(doc.has("broken")).isFalse();
    });
  }

  @Test
  void bareIdentifierDefaultIsRejectedAtDdlTime() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      final Property p = type.createProperty("bare", Type.STRING);

      assertThatThrownBy(() -> p.setDefaultValue("active")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Probe.bare").hasMessageContaining("active");

      assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).doesNotContain("bare");
    });
  }

  @Test
  void bareIdentifierDefaultIsRejectedThroughSql() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE ProbeSql");
      database.command("sql", "CREATE PROPERTY ProbeSql.bare STRING");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY ProbeSql.bare DEFAULT active")).isInstanceOf(
          SchemaException.class).hasMessageContaining("field reference");
      // CREATE PROPERTY wraps it, so the reason has to survive into the wrapper's own message - that is what a client
      // sees over HTTP.
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY ProbeSql.bare2 STRING (DEFAULT active)"))
          .hasMessageContaining("field reference");
    });
  }

  @Test
  void recordScopedDefaultFailsLoudlyInsteadOfStoringItsOwnSourceText() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      // `@rid` parses, so it is not caught by the DDL-time syntax check, but it can only be resolved against a
      // current record and the default expression is always evaluated against a null one. It used to NPE inside
      // getDefaultValue(), get swallowed, and write the literal string "@rid" on every record.
      type.createProperty("ridDefault", Type.STRING).setDefaultValue("@rid");

      assertThatThrownBy(() -> database.newDocument("Probe").save()).hasMessageContaining("Probe.ridDefault")
          .hasMessageContaining("@rid");
    });
  }

  @Test
  void validDefaultsKeepWorking() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("quoted", Type.STRING).setDefaultValue("'ok'");
      type.createProperty("number", Type.INTEGER).setDefaultValue("7");
      type.createProperty("expression", Type.INTEGER).setDefaultValue("3 + 4");
      type.createProperty("method", Type.STRING).setDefaultValue("'a'.append('b')");

      final MutableDocument doc = database.newDocument("Probe").save();
      assertThat(doc.getString("quoted")).isEqualTo("ok");
      assertThat(doc.getInteger("number")).isEqualTo(7);
      assertThat(doc.getInteger("expression")).isEqualTo(7);
      assertThat(doc.getString("method")).isEqualTo("ab");
    });
  }

  @Test
  void nonStringDefaultsAreNotEvaluatedAsExpressions() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("num", Type.INTEGER).setDefaultValue(42);

      assertThat(type.getProperty("num").getDefaultValue()).isEqualTo(42);
      assertThat(database.newDocument("Probe").save().getInteger("num")).isEqualTo(42);
    });
  }

  /**
   * The compiled expression is cached, the evaluated result is not: a {@code sysdate()} default must still produce a
   * fresh value on every record.
   */
  @Test
  void perRecordDefaultsAreStillReEvaluated() throws InterruptedException {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("createdOn", Type.DATETIME_MICROS).setDefaultValue("sysdate()");
    });

    final AtomicReference<LocalDateTime> first = new AtomicReference<>();
    final AtomicReference<LocalDateTime> second = new AtomicReference<>();

    database.transaction(() -> first.set((LocalDateTime) database.newDocument("Probe").save().get("createdOn")));
    Thread.sleep(50);
    database.transaction(() -> second.set((LocalDateTime) database.newDocument("Probe").save().get("createdOn")));

    assertThat(second.get()).isAfter(first.get());
  }

  /**
   * The expression must be parsed once at DDL time, not on every {@code getDefaultValue()} call. There is no public
   * hook for "how many times did you parse", so this asserts the observable consequence: the same compiled
   * {@link Expression} instance backs every evaluation.
   */
  @Test
  void defaultExpressionIsCompiledOnlyOnce() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      final AbstractProperty p = (AbstractProperty) type.createProperty("quoted", Type.STRING).setDefaultValue("'ok'");

      final Expression compiled = p.getDefaultValueExpression();
      assertThat(compiled).isNotNull();

      for (int i = 0; i < 100; i++) {
        assertThat(p.getDefaultValue()).isEqualTo("ok");
        assertThat(p.getDefaultValueExpression()).isSameAs(compiled);
      }
    });
  }

  /**
   * A default that evaluates to null fills in nothing: the property stays absent instead of being explicitly set to
   * null. That also keeps the NOTNULL check pointing at the real gap - {@code DocumentValidator} only raises "cannot
   * be null" when {@code has()} is true.
   */
  @Test
  void nullEvaluatingDefaultLeavesThePropertyAbsent() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("nullDefault", Type.STRING).setDefaultValue("null");
      type.createProperty("other", Type.STRING).setDefaultValue("'ok'");

      final MutableDocument doc = database.newDocument("Probe").save();
      assertThat(doc.has("nullDefault")).isFalse();
      assertThat(doc.getString("other")).isEqualTo("ok");
      assertThat(doc.toJSON().has("nullDefault")).isFalse();
    });
  }

  /**
   * The SQL insert path (which runs {@code ApplyDefaultsStep}) and the engine path (which runs
   * {@code LocalDatabase.setDefaultValues}) must agree on the null rule.
   */
  @Test
  void sqlInsertAgreesWithTheEnginePathOnNullDefaults() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Probe");
      database.command("sql", "CREATE PROPERTY Probe.nullDefault STRING (DEFAULT null)");
      database.command("sql", "CREATE PROPERTY Probe.other STRING (DEFAULT 'ok')");

      database.command("sql", "INSERT INTO Probe SET marker = 1");

      final ResultSet rs = database.query("sql", "SELECT FROM Probe");
      final MutableDocument doc = rs.next().getElement().get().asDocument().modify();
      assertThat(doc.has("nullDefault")).isFalse();
      assertThat(doc.getString("other")).isEqualTo("ok");
    });
  }

  @Test
  void updateApplyDefaultsAgreesOnNullDefaults() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Probe");
      database.command("sql", "CREATE PROPERTY Probe.marker INTEGER");
      database.command("sql", "INSERT INTO Probe SET marker = 1");

      database.command("sql", "CREATE PROPERTY Probe.nullDefault STRING (DEFAULT null)");
      database.command("sql", "CREATE PROPERTY Probe.other STRING (DEFAULT 'ok')");

      database.command("sql", "UPDATE Probe SET marker = 2 APPLY DEFAULTS");

      final ResultSet rs = database.query("sql", "SELECT FROM Probe");
      final MutableDocument doc = rs.next().getElement().get().asDocument().modify();
      assertThat(doc.has("nullDefault")).isFalse();
      assertThat(doc.getString("other")).isEqualTo("ok");
    });
  }

  /**
   * A NOTNULL property whose default cannot fill in a value now fails as "mandatory, but not found", pointing at the
   * schema gap, rather than as "cannot be null", which used to point at the caller's data.
   */
  @Test
  void notNullPropertyWithNullDefaultIsNotReportedAsANullViolation() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("status", Type.STRING).setDefaultValue("null").setNotNull(true).setMandatory(true);

      assertThatThrownBy(() -> database.newDocument("Probe").save()).hasMessageContaining("mandatory");
    });
  }

  /**
   * Schema metadata reports the DEFINITION of a default, never an evaluation of it. Two things used to be wrong here:
   * the {@code DEFAULT_NOT_SET} sentinel leaked out of {@code getDefaultValue()}, so every plain property came back
   * with {@code "default": "<DEFAULT_NOT_SET>"}, and a {@code sysdate()} default was reported as a snapshot taken when
   * the row was fetched rather than as the expression that was declared.
   */
  @Test
  void schemaMetadataReportsTheDefinitionAndNotTheSentinel() {
    database.command("sql", "CREATE DOCUMENT TYPE Probe");
    database.command("sql", "CREATE PROPERTY Probe.plain STRING");
    database.command("sql", "CREATE PROPERTY Probe.withDefault STRING (DEFAULT 'ok')");
    database.command("sql", "CREATE PROPERTY Probe.createdOn DATETIME (DEFAULT sysdate())");

    final DocumentType type = database.getSchema().getType("Probe");
    assertThat(type.getProperty("plain").getDefaultValue()).isNull();
    assertThat(type.getProperty("plain").getDefaultValueDefinition()).isNull();
    assertThat(type.getProperty("withDefault").getDefaultValueDefinition()).isEqualTo("'ok'");
    assertThat(type.getProperty("createdOn").getDefaultValueDefinition()).isEqualTo("sysdate()");

    final String json = database.query("sql", "SELECT properties FROM schema:types WHERE name = 'Probe'").next().toJSON()
        .toString();
    assertThat(json).doesNotContain("DEFAULT_NOT_SET");
    assertThat(json).contains("sysdate()");
  }

  /**
   * An existing database whose schema.json already holds an invalid default must still open: the load path logs a
   * warning and keeps the pre-#6134 behaviour for that property rather than refusing to hydrate the schema. And the one
   * statement that can repair it, ALTER PROPERTY, must work on it - which means not evaluating the outgoing default.
   */
  @Test
  void aLegacyInvalidDefaultDoesNotBlockTheSchemaLoadAndCanBeRepaired() throws IOException {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Probe");
      type.createProperty("broken", Type.STRING).setDefaultValue("'placeholder'");
    });

    database.close();

    final File schemaFile = new File(getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject schema = new JSONObject(FileUtils.readFileAsString(schemaFile));
    schema.getJSONObject("types").getJSONObject("Probe").getJSONObject("properties").getJSONObject("broken")
        .put("default", "this is (not parseable");
    Files.write(schemaFile.toPath(), schema.toString().getBytes(StandardCharsets.UTF_8));

    database = factory.open();

    // PRE-#6134 BEHAVIOUR PRESERVED FOR AN ALREADY-PERSISTED INVALID DEFAULT
    assertThat(database.getSchema().getType("Probe").getProperty("broken").getDefaultValue()).isEqualTo(
        "this is (not parseable");

    database.command("sql", "ALTER PROPERTY Probe.broken DEFAULT 'fixed'");
    assertThat(database.getSchema().getType("Probe").getProperty("broken").getDefaultValue()).isEqualTo("fixed");
  }
}
