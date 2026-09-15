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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7589: there was no in-place rename primitive for a property, neither in the Java API
 * nor in SQL - only {@link DocumentType#rename} existed, one level up.
 * <p>
 * The rename implemented here is schema-metadata-only, deliberately: a record stores each field under a small
 * integer id resolved from whatever name it was written with, from a dictionary that is shared database-wide and
 * also referenced by ordinary string VALUES that happen to match an identifier - so there is no cheap way to tell
 * whether renaming that shared id would be safe, and no schema DDL in ArcadeDB today mutates record content as a
 * side effect (not even {@code DROP PROPERTY}, which leaves the field sitting in every document that already had
 * it). A rename that copied values across every record would therefore be a new category of behaviour. Instead,
 * {@code Property.rename}/{@code ALTER PROPERTY ... NAME} only relabels the schema going forward: existing
 * documents keep answering under the old name, and only a write made after the rename lands under the new one -
 * consistent with the same schema/data decoupling every other DDL statement already relies on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7589PropertyRenameTest extends TestHelper {

  private void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.name STRING");
    database.command("sql", "ALTER PROPERTY Person.name MANDATORY true");
  }

  @Test
  void javaApiRenamesTheSchemaPropertyAndCarriesMetadataAcross() {
    createType();

    final DocumentType type = database.getSchema().getType("Person");
    final Property renamed = type.getProperty("name").rename("fullName");

    assertThat(type.existsProperty("name")).isFalse();
    assertThat(type.existsProperty("fullName")).isTrue();
    assertThat(renamed.getName()).isEqualTo("fullName");
    assertThat(renamed.getType()).isEqualTo(Type.STRING);
    assertThat(renamed.isMandatory()).as("metadata must carry across the rename").isTrue();
  }

  @Test
  void sqlAlterPropertyNameRenamesTheProperty() {
    createType();

    database.command("sql", "ALTER PROPERTY Person.name NAME fullName");

    final DocumentType type = database.getSchema().getType("Person");
    assertThat(type.existsProperty("name")).isFalse();
    assertThat(type.existsProperty("fullName")).isTrue();
    assertThat(type.getProperty("fullName").isMandatory()).isTrue();
  }

  /**
   * The load-bearing behaviour: a rename does NOT rewrite existing documents. A value already stored under the
   * old name keeps reading back under the old name; a new write after the rename lands under the new name.
   */
  @Test
  void existingDocumentsAreNotRevisitedByARename() {
    createType();

    database.transaction(() -> database.newDocument("Person").set("name", "Ada").save());

    database.command("sql", "ALTER PROPERTY Person.name NAME fullName");

    database.transaction(() -> database.newDocument("Person").set("fullName", "Grace").save());

    final ResultSet oldRow = database.query("sql", "SELECT FROM Person WHERE name = 'Ada'");
    assertThat(oldRow.hasNext()).as("a pre-existing document keeps answering under the OLD field name").isTrue();
    assertThat(oldRow.next().<Object>getProperty("fullName")).as("it does not retroactively appear under the new name").isNull();

    final ResultSet newRow = database.query("sql", "SELECT FROM Person WHERE fullName = 'Grace'");
    assertThat(newRow.hasNext()).as("a document written after the rename uses the new field name").isTrue();
  }

  @Test
  void renamingToAnAlreadyExistingNameIsRefused() {
    createType();
    database.command("sql", "CREATE PROPERTY Person.email STRING");

    assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("email"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  void renamingAnIndexedPropertyIsRefused() {
    createType();
    database.command("sql", "CREATE INDEX ON Person (name) UNIQUE");

    assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("fullName"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("index");

    assertThat(database.getSchema().getType("Person").existsProperty("name")).isTrue();
  }

  @Test
  void renamingADeclaredTimeSeriesColumnIsRefused() {
    database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)");

    assertThatThrownBy(() -> database.getSchema().getType("Reading").getProperty("value").rename("v"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("TIMESERIES");
  }

  @Test
  void aStaleHandleFromBeforeTheRenameCannotBeUsedAgain() {
    createType();

    final Property original = database.getSchema().getType("Person").getProperty("name");
    original.rename("fullName");

    assertThatThrownBy(() -> original.rename("somethingElse")).isInstanceOf(SchemaException.class);
  }

  @Test
  void renamingToItsOwnCurrentNameIsANoOp() {
    createType();

    final Property property = database.getSchema().getType("Person").getProperty("name");
    final Property result = property.rename("name");

    assertThat(result.getName()).isEqualTo("name");
    assertThat(database.getSchema().getType("Person").existsProperty("name")).isTrue();
  }
}
