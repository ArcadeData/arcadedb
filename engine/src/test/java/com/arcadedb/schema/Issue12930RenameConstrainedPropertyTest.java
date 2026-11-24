/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12930: a rename moves the property's constraints to the new name and leaves the values under the old one (#7589), so
 * renaming a MANDATORY property of a type with records left every record violating it and refused every later update of them. The
 * rename is refused like {@code ALTER PROPERTY ... MANDATORY true} over the same records, and is still accepted when no stored
 * record would violate the constraints under the new name.
 */
class Issue12930RenameConstrainedPropertyTest extends TestHelper {

  @Test
  void aMandatoryPropertyWithRecordsIsNotRenamed() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.old INTEGER (mandatory true)");
    database.transaction(() -> database.command("sql", "INSERT INTO R SET old = 1"));

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY R.old NAME new")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot rename property 'R.old' to 'new': record #").hasMessageContaining("violates it")
        .hasMessageContaining("property 'R.new' is mandatory");

    assertSchemaUnchanged("old", "new");
    assertThat(database.getSchema().getType("R").getProperty("old").isMandatory()).isTrue();
    // the records can still be updated, which the accepted rename made impossible
    database.transaction(() -> database.command("sql", "UPDATE R SET z = 1"));
    assertNoViolation();
  }

  @Test
  void theJavaApiRefusesTheSameRename() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.old INTEGER (mandatory true)");
    database.transaction(() -> database.command("sql", "INSERT INTO R SET old = 1"));

    assertThatThrownBy(() -> database.getSchema().getType("R").getProperty("old").rename("new"))
        .isInstanceOf(ValidationException.class).hasMessageContaining("Cannot rename property 'R.old' to 'new'");
    assertSchemaUnchanged("old", "new");
  }

  @Test
  void aNotNullPropertyIsNotRenamedOverARecordThatHoldsNullUnderTheNewName() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.old INTEGER (notnull true)");
    database.transaction(() -> database.command("sql", "INSERT INTO R SET old = 1, new = null"));

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY R.old NAME new")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot rename property 'R.old' to 'new': record #").hasMessageContaining("cannot be null");
    assertSchemaUnchanged("old", "new");
  }

  @Test
  void aNotNullPropertyIsRenamedOverRecordsThatLackTheNewName() {
    // NOTNULL refuses a null value, not a missing one: a record without the new name violates nothing
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.old INTEGER (notnull true)");
    database.transaction(() -> database.command("sql", "INSERT INTO R SET old = 1"));

    database.command("sql", "ALTER PROPERTY R.old NAME new");

    assertSchemaUnchanged("new", "old");
    assertThat(database.getSchema().getType("R").getProperty("new").isNotNull()).isTrue();
    assertNoViolation();
  }

  @Test
  void aMaxIsCheckedAgainstTheValuesAlreadyStoredUnderTheNewName() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.old INTEGER (max 10)");
    database.transaction(() -> database.command("sql", "INSERT INTO R SET old = 1, new = 20"));

    assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY R.old NAME new")).isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot rename property 'R.old' to 'new': record #").hasMessageContaining("'R.new'");
    assertSchemaUnchanged("old", "new");

    // values copied under the new name first, as the message advises: the rename goes through
    database.transaction(() -> database.command("sql", "UPDATE R SET new = old"));
    database.command("sql", "ALTER PROPERTY R.old NAME new");
    assertSchemaUnchanged("new", "old");
    assertNoViolation();
  }

  @Test
  void aMandatoryPropertyIsRenamedOnceTheValuesAreCopiedUnderTheNewName() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.old INTEGER (mandatory true)");
    database.transaction(() -> database.command("sql", "INSERT INTO R SET old = 1"));
    database.transaction(() -> database.command("sql", "UPDATE R SET new = old"));

    database.command("sql", "ALTER PROPERTY R.old NAME new");

    assertSchemaUnchanged("new", "old");
    assertNoViolation();
  }

  @Test
  void aPropertyWithoutConstraintsIsStillRenamed() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.old INTEGER");
    database.transaction(() -> database.command("sql", "INSERT INTO R SET old = 1"));

    database.command("sql", "ALTER PROPERTY R.old NAME new");

    assertSchemaUnchanged("new", "old");
    try (final ResultSet rs = database.query("sql", "SELECT old FROM R")) {
      assertThat(rs.next().<Integer>getProperty("old")).as("the value stays under the old name (#7589)").isEqualTo(1);
    }
    assertNoViolation();
  }

  @Test
  void aMandatoryPropertyOfAnEmptyTypeIsRenamed() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.old INTEGER (mandatory true, notnull true, max 10)");

    database.command("sql", "ALTER PROPERTY R.old NAME new");

    assertSchemaUnchanged("new", "old");
    final Property renamed = database.getSchema().getType("R").getProperty("new");
    assertThat(renamed.isMandatory()).isTrue();
    assertThat(renamed.isNotNull()).isTrue();
    assertThat(renamed.getMax()).isEqualTo("10");
  }

  private void assertSchemaUnchanged(final String present, final String absent) {
    final DocumentType type = database.getSchema().getType("R");
    assertThat(type.existsProperty(present)).isTrue();
    assertThat(type.existsProperty(absent)).isFalse();
  }

  private void assertNoViolation() {
    try (final ResultSet check = database.command("sql", "CHECK DATABASE")) {
      assertThat((Long) check.next().getProperty("totalConstraintViolations")).isEqualTo(0L);
    }
  }
}
