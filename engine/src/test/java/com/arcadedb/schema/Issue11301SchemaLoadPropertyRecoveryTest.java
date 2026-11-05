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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #11301: one property the schema loader could not restore aborted the property loop, so every
 * property, every type index and the inheritance of every type after it vanished from the opened schema, a UNIQUE index stopped
 * being enforced, and the next DDL statement saved the truncated schema over {@code schema.json}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11301SchemaLoadPropertyRecoveryTest extends TestHelper {

  /**
   * The reachable trigger: {@code DROP TYPE} of a type a {@code LIST OF} property names. Before the fix the reopen threw out of
   * {@code setOfType} for the LIST branch only (EMBEDDED and LINK already accepted a missing type while loading).
   */
  @Test
  void aListOfADroppedTypeReloadsAndTheTypesAfterItKeepTheirSchema() {
    database.command("sql", "CREATE DOCUMENT TYPE Address");
    database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
    database.command("sql", "CREATE PROPERTY AaaHolder.tags LIST OF Address");
    database.command("sql", "CREATE PROPERTY AaaHolder.items MAP OF Address");
    database.command("sql", "CREATE PROPERTY AaaHolder.one EMBEDDED OF Address");
    database.command("sql", "CREATE PROPERTY AaaHolder.link LINK OF Address");
    createGuardedType();
    database.command("sql", "DROP TYPE Address");

    reopenDatabase();

    final DocumentType holder = database.getSchema().getType("AaaHolder");
    assertThat(holder.getProperty("tags").getOfType()).isEqualTo("Address");
    assertThat(holder.getProperty("items").getOfType()).isEqualTo("Address");
    assertThat(holder.getProperty("one").getOfType()).isEqualTo("Address");
    assertThat(holder.getProperty("link").getOfType()).isEqualTo("Address");
    assertGuardedTypeIntact();

    // the next DDL statement saves the schema: it must still carry everything
    database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
    reopenDatabase();
    assertThat(database.getSchema().getType("AaaHolder").getProperty("tags").getOfType()).isEqualTo("Address");
    assertGuardedTypeIntact();
  }

  /**
   * The loader itself: an attribute a setter refuses on load costs that attribute only. Every other attribute of the property, and
   * every property and index of the other types, is restored, and the refused attribute is written back as it was read, so the
   * next save does not lose what the file declared.
   */
  @Test
  void anAttributeTheLoaderCannotRestoreCostsOnlyThatAttribute() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
    database.command("sql", "CREATE PROPERTY AaaHolder.payload STRING (mandatory true, notnull true)");
    database.command("sql", "CREATE PROPERTY AaaHolder.other STRING (readonly true)");
    createGuardedType();
    database.close();

    editSchema(root -> root.getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties").getJSONObject("payload")
        .put("compression", "no-such-codec"));

    database = factory.open();

    final Property payload = database.getSchema().getType("AaaHolder").getProperty("payload");
    assertThat(payload.isMandatory()).isTrue();
    assertThat(payload.isNotNull()).isTrue();
    assertThat(payload.getCompression()).isEqualTo("none");
    assertThat(database.getSchema().getType("AaaHolder").getProperty("other").isReadonly()).isTrue();
    assertGuardedTypeIntact();

    database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
    assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties")
        .getJSONObject("payload").getString("compression")).isEqualTo("no-such-codec");

    // setting the attribute again replaces what the file held
    database.command("sql", "ALTER PROPERTY AaaHolder.payload COMPRESSION 'fast'");
    assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties")
        .getJSONObject("payload").getString("compression")).isEqualTo("fast");
    reopenDatabase();
    assertThat(database.getSchema().getType("AaaHolder").getProperty("payload").getCompression()).isEqualTo("fast");
  }

  /**
   * A property the loader cannot create at all (a type this release does not know) is not served, keeps its definition in the
   * schema file, and does not take the rest of the schema with it. DROP PROPERTY removes the definition.
   */
  @Test
  void aPropertyTheLoaderCannotCreateKeepsItsDefinitionAndCostsNothingElse() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
    database.command("sql", "CREATE PROPERTY AaaHolder.future STRING");
    database.command("sql", "CREATE PROPERTY AaaHolder.present STRING (mandatory true)");
    createGuardedType();
    database.close();

    editSchema(root -> root.getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties").getJSONObject("future")
        .put("type", "NO_SUCH_TYPE"));

    database = factory.open();

    final DocumentType holder = database.getSchema().getType("AaaHolder");
    assertThat(holder.existsProperty("future")).isFalse();
    assertThat(holder.getProperty("present").isMandatory()).isTrue();
    assertGuardedTypeIntact();

    database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
    assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties")
        .getJSONObject("future").getString("type")).isEqualTo("NO_SUCH_TYPE");

    database.command("sql", "DROP PROPERTY AaaHolder.future");
    assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties").has("future")).isFalse();
    reopenDatabase();
    assertThat(database.getSchema().getType("AaaHolder").existsProperty("future")).isFalse();
    assertGuardedTypeIntact();
  }

  /**
   * Creating a property under the name of one the loader could not create replaces the kept definition.
   */
  @Test
  void creatingThePropertyAgainReplacesTheKeptDefinition() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE AaaHolder");
    database.command("sql", "CREATE PROPERTY AaaHolder.future STRING");
    database.close();

    editSchema(root -> root.getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties").getJSONObject("future")
        .put("type", "NO_SUCH_TYPE"));

    database = factory.open();
    database.command("sql", "CREATE PROPERTY AaaHolder.future INTEGER");
    assertThat(readSchema().getJSONObject("types").getJSONObject("AaaHolder").getJSONObject("properties")
        .getJSONObject("future").getString("type")).isEqualTo("INTEGER");
    reopenDatabase();
    assertThat(database.getSchema().getType("AaaHolder").getProperty("future").getType()).isEqualTo(Type.INTEGER);
  }

  private void createGuardedType() {
    database.command("sql", "CREATE DOCUMENT TYPE Zzz1");
    database.command("sql", "CREATE PROPERTY Zzz1.name STRING (mandatory true, notnull true)");
    database.command("sql", "CREATE INDEX ON Zzz1 (name) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO Zzz1 SET name = 'keep-me'"));
  }

  private void assertGuardedTypeIntact() {
    final DocumentType zzz = database.getSchema().getType("Zzz1");
    assertThat(zzz.existsProperty("name")).isTrue();
    assertThat(zzz.getProperty("name").isMandatory()).isTrue();
    assertThat(zzz.getProperty("name").isNotNull()).isTrue();
    assertThat(database.getSchema().existsIndex("Zzz1[name]")).isTrue();
    assertThat(database.getSchema().getIndexByName("Zzz1[name]").isUnique()).isTrue();
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO Zzz1 SET name = 'keep-me'")))
        .isInstanceOf(DuplicatedKeyException.class);
    assertThat(database.query("sql", "SELECT count(*) AS n FROM Zzz1").next().<Long>getProperty("n")).isEqualTo(1L);
  }

  private interface SchemaEdit {
    void apply(JSONObject root);
  }

  private void editSchema(final SchemaEdit edit) throws Exception {
    final JSONObject root = readSchema();
    edit.apply(root);
    Files.writeString(schemaFile().toPath(), root.toString());
    // the fallback copy must not stand in for the edited primary
    final File prev = new File(getDatabasePath() + File.separator + "schema.prev.json");
    if (prev.exists())
      Files.writeString(prev.toPath(), root.toString());
  }

  private JSONObject readSchema() throws Exception {
    return new JSONObject(Files.readString(schemaFile().toPath()));
  }

  private File schemaFile() {
    return new File(getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME);
  }
}
