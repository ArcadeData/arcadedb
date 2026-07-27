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
package com.arcadedb.database;

import com.arcadedb.exception.DatabaseOperationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers {@link LocalDatabase#closeForDrop()}, the close half of {@link LocalDatabase#drop()} split out so a
 * caller can remove the files itself - for example by renaming the directory aside and deleting it off the
 * calling thread.
 */
class LocalDatabaseCloseForDropTest {

  @TempDir
  private Path          tempDir;
  private LocalDatabase database;

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.close();
  }

  private LocalDatabase createDatabase() {
    final LocalDatabase db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("mydb").toString()).create();
    db.transaction(() -> {
      db.getSchema().createDocumentType("Doc");
      db.newDocument("Doc").set("name", "first").save();
    });
    return db;
  }

  @Test
  void closeForDropClosesTheDatabaseButKeepsTheFiles() {
    database = createDatabase();
    final File databaseDirectory = new File(database.getDatabasePath());

    database.closeForDrop();

    assertThat(database.isOpen()).isFalse();
    assertThat(databaseDirectory).as("closeForDrop must leave the files for the caller to remove").exists();
    assertThat(new File(databaseDirectory, "schema.json")).exists();
  }

  @Test
  void aDatabaseClosedForDropCanBeReopened() {
    database = createDatabase();
    final String databasePath = database.getDatabasePath();

    database.closeForDrop();

    database = (LocalDatabase) new DatabaseFactory(databasePath).open();
    assertThat(database.countType("Doc", true)).isEqualTo(1);
  }

  @Test
  void dropStillRemovesTheFiles() {
    database = createDatabase();
    final File databaseDirectory = new File(database.getDatabasePath());

    database.drop();

    assertThat(database.isOpen()).isFalse();
    assertThat(databaseDirectory).doesNotExist();
  }

  @Test
  void closeForDropIsRejectedInsideATransaction() {
    database = createDatabase();
    database.begin();
    try {
      assertThatThrownBy(() -> database.closeForDrop()).isInstanceOf(DatabaseOperationException.class);
    } finally {
      database.rollback();
    }
  }
}
