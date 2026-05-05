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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduction for an issue reported by mdre via email tied to issue #4083 / #4081 follow-up: a
 * SQL schema script defines a parent type with a {@code FULL_TEXT} index on a property, then
 * later a sub-type that adds both a {@code FULL_TEXT} and a {@code UNIQUE} index on the same
 * inherited property. The script reports success, but Studio's refresh later fails with
 * "index Investigacion[cuij] could not be created" because the parent type index ends up gone
 * after the script finishes.
 * <p>
 * Hypothesis: {@code TypeIndexBuilder.create()} calls {@code getPolymorphicIndexByProperties}
 * which walks UP the type hierarchy. When the user creates {@code UNIQUE} on the sub-type's
 * property, that call returns the PARENT's {@code FULL_TEXT} TypeIndex; the unique flag mismatch
 * triggers {@code existingTypeIndex.drop()} which drops the parent's index instead of leaving
 * it untouched and creating an independent sub-type index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PolymorphicIndexConflictTest {

  private static final String DB_PATH = "./target/databases/PolymorphicIndexConflictTest";

  @AfterEach
  void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void uniqueIndexOnSubtypeMustNotDropParentFullTextIndex() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.command("sql", "CREATE VERTEX TYPE SObject");
        db.command("sql", "CREATE VERTEX TYPE Investigacion EXTENDS SObject");
        db.command("sql", "CREATE PROPERTY Investigacion.cuij STRING");
        db.command("sql", "CREATE INDEX IF NOT EXISTS ON Investigacion (cuij) FULL_TEXT");

        // The parent's TypeIndex should exist after this point.
        assertThat(db.getSchema().existsIndex("Investigacion[cuij]"))
            .as("parent FULL_TEXT TypeIndex must exist after CREATE INDEX")
            .isTrue();

        db.command("sql", "CREATE VERTEX TYPE Legajo EXTENDS Investigacion");

        // Sub-type re-declares the same FULL_TEXT - polymorphic lookup returns parent and
        // (per current TypeIndexBuilder logic) we leave the parent untouched. OK.
        db.command("sql", "CREATE INDEX IF NOT EXISTS ON Legajo (cuij) FULL_TEXT");

        assertThat(db.getSchema().existsIndex("Investigacion[cuij]"))
            .as("parent FULL_TEXT TypeIndex must still exist after sub-type re-declares the same index")
            .isTrue();

        // Sub-type tries to add a UNIQUE index on the inherited property whose parent index
        // is FULL_TEXT (unique=false). Before the fix, the polymorphic lookup returned the
        // parent's FULL_TEXT TypeIndex; the unique mismatch then triggered existingTypeIndex.drop()
        // and silently wiped the PARENT's index, leaving a stale entry in the schema JSON that
        // failed to load on the next refresh. After the fix, this attempt raises a clear error
        // identifying the inheritance conflict, so the operator can fix the schema instead of
        // discovering a broken database hours later.
        assertThatThrownBy(() -> db.command("sql", "CREATE INDEX IF NOT EXISTS ON Legajo (cuij) UNIQUE"))
            .as("creating a UNIQUE index on a sub-type whose parent already has a FULL_TEXT index "
                + "on the same inherited property must raise a clear conflict error rather than silently "
                + "dropping the parent's index")
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Investigacion")
            .hasMessageContaining("cuij");

        // After the rejected UNIQUE, the parent's FULL_TEXT TypeIndex MUST still be intact.
        assertThat(db.getSchema().existsIndex("Investigacion[cuij]"))
            .as("parent FULL_TEXT TypeIndex must NOT be dropped when a sub-type's CREATE INDEX is rejected")
            .isTrue();
      }
    }

    // Reopen the database. The parent's index must still load cleanly (no orphan entries left
    // in the schema JSON from a half-completed in-memory drop).
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.open(ComponentFile.MODE.READ_WRITE)) {
        assertThat(db.getSchema().existsIndex("Investigacion[cuij]"))
            .as("parent FULL_TEXT TypeIndex must still exist after database reopen")
            .isTrue();

        // Inserting a vertex that uses the parent's index should not throw, and a subsequent
        // lookup must return it.
        db.transaction(() -> db.command("sql",
            "CREATE VERTEX Investigacion SET cuij = '12-3456789-0'"));

        assertThat(db.countType("Investigacion", true)).isEqualTo(1L);
      }
    }
  }
}
