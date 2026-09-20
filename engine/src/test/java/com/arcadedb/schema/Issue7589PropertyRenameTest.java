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
   * <p>
   * Which has a consequence worth stating, and which {@code CHECK DATABASE} reports since #7952: renaming a
   * {@code MANDATORY} property leaves every pre-existing document violating that constraint under the NEW name, the
   * same state {@code ALTER PROPERTY ... MANDATORY TRUE} on a populated type produces. Nothing in the engine said so
   * before, and a document in it cannot be updated until the property is supplied - so the check reporting it is
   * the point rather than a nuisance, and this test now pins that it does.
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

    // #7952: the pre-existing document now violates the MANDATORY constraint that moved with the name.
    try (final ResultSet check = database.command("sql", "CHECK DATABASE")) {
      assertThat((Long) check.next().getProperty("totalConstraintViolations"))
          .as("the document written before the rename no longer satisfies Person.fullName").isEqualTo(1L);
    }

    // Taken back out so the shared end-of-test integrity assertion, which requires a run with no warnings, still
    // has a clean database to look at.
    database.command("sql", "CHECK DATABASE FIX DELETE INVALID RECORDS").close();
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
  void renamingANonExistentPropertyIsRefused() {
    createType();

    assertThatThrownBy(() -> database.getSchema().getType("Person").renameProperty("doesNotExist", "newName"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("does not exist");
  }

  @Test
  void renamingToANameAlreadyDefinedInASuperTypeIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.email STRING");
    createType();
    database.command("sql", "ALTER TYPE Person SUPERTYPE +Base");

    assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("email"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already defined in a super type");
  }

  @Test
  void aDefaultValueCarriesOverToTheNewNameAndNoLongerAppliesUnderTheOldOne() {
    createType();
    database.command("sql", "ALTER PROPERTY Person.name DEFAULT 'Anonymous'");

    database.command("sql", "ALTER PROPERTY Person.name NAME fullName");

    final DocumentType type = database.getSchema().getType("Person");
    assertThat(type.getProperty("fullName").getDefaultValueDefinition()).isEqualTo("'Anonymous'");
    assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).contains("fullName").doesNotContain("name");
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

  /**
   * {@link LocalDocumentType#getAllIndexes(boolean)} only ever walks up (this type and its super types), so an
   * index a SUBTYPE declares on an inherited property is invisible to it. The rename must still refuse: otherwise
   * the subtype's index is left bound to the old name while every write after the rename lands under the new one.
   */
  @Test
  void renamingAPropertyIndexedOnlyByASubtypeIsRefused() {
    createType();
    database.command("sql", "CREATE DOCUMENT TYPE Employee");
    database.command("sql", "ALTER TYPE Employee SUPERTYPE +Person");
    database.command("sql", "CREATE INDEX ON Employee (name) NOTUNIQUE");

    assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("fullName"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("index");

    assertThat(database.getSchema().getType("Person").existsProperty("name")).isTrue();
  }

  /**
   * Same shape as {@link #renamingAPropertyIndexedOnlyByASubtypeIsRefused}, one level deeper: the index sits on a
   * subtype of a subtype, exercising {@code findDescendantIndexOnProperty}'s recursive case rather than just its
   * first level.
   */
  @Test
  void renamingAPropertyIndexedOnlyByAGrandchildSubtypeIsRefused() {
    createType();
    database.command("sql", "CREATE DOCUMENT TYPE Employee");
    database.command("sql", "ALTER TYPE Employee SUPERTYPE +Person");
    database.command("sql", "CREATE DOCUMENT TYPE Manager");
    database.command("sql", "ALTER TYPE Manager SUPERTYPE +Employee");
    database.command("sql", "CREATE INDEX ON Manager (name) NOTUNIQUE");

    assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("fullName"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("index");

    assertThat(database.getSchema().getType("Person").existsProperty("name")).isTrue();
  }

  /**
   * A subtype may independently declare its own property under a name a super type also uses -
   * {@code addSuperType}'s own conflict check only warns about this, never refuses it - and from that subtype
   * down, the name resolves to the subtype's shadowing property, not the super type's. An index on the shadowing
   * property must not block renaming the super type's unrelated, same-named property.
   */
  @Test
  void renamingAPropertyIsNotBlockedByAnIndexOnAnUnrelatedShadowingSubtypeProperty() {
    database.command("sql", "CREATE DOCUMENT TYPE Employee");
    database.command("sql", "CREATE PROPERTY Employee.name STRING");
    database.command("sql", "CREATE INDEX ON Employee (name) NOTUNIQUE");

    createType();
    // Links Employee under Person with both already independently declaring "name" - a conflict addSuperType only
    // warns about, producing the shadow this test targets.
    database.command("sql", "ALTER TYPE Employee SUPERTYPE +Person");

    final Property renamed = database.getSchema().getType("Person").getProperty("name").rename("fullName");

    assertThat(renamed.getName()).isEqualTo("fullName");
    assertThat(database.getSchema().getType("Employee").existsProperty("name"))
        .as("Employee's own shadowing property is unrelated to Person's rename").isTrue();
  }

  @Test
  void sqlAlterPropertyNameAcceptsABacktickQuotedNameWithSpaces() {
    createType();

    database.command("sql", "ALTER PROPERTY Person.name NAME `full name`");

    final DocumentType type = database.getSchema().getType("Person");
    assertThat(type.existsProperty("name")).isFalse();
    assertThat(type.existsProperty("full name")).isTrue();
    assertThat(type.getProperty("full name").isMandatory()).isTrue();
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
