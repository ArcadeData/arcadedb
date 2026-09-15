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
package com.arcadedb.integration.backup;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7464: a full backup that cannot archive {@code schema.json} must fail rather than report success.
 * <p>
 * Both backup paths used to treat the file as optional - {@code backupFromFrozenFiles} through
 * {@code compressFile}, which logs " not found" and returns 0, and {@code backupFromSnapshot} through
 * {@code PageManager.captureConfigurationFiles}, which omits a file that raised {@code NoSuchFileException} at
 * t0. That is right for {@code configuration.json}, which exists only once a setting has been persisted, and
 * wrong for {@code schema.json}, which {@code LocalDatabase.create()} writes before the database is usable.
 * <p>
 * The archive such a backup produced satisfied neither arm of {@code DatabaseFactory.exists()} - which looks for
 * {@code schema.json} and then for {@code schema.prev.json}, and neither path archives the latter - so it
 * restored to a directory ArcadeDB did not recognise as a database at all, from a backup that had printed
 * "Full backup completed".
 * <p>
 * Every case here is run against both paths, because the skip was implemented separately in each.
 */
class Issue7464BackupRequiresSchemaTest {
  private static final String DATABASE_PATH = "target/databases/issue7464-backup-schema";
  private static final String RESTORED_PATH = "target/databases/issue7464-backup-schema-restored";
  private static final String BACKUP_FILE   = "target/issue7464-backup-schema.zip";
  private static final String TYPE          = "Issue7464Doc";

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * The reported case: {@code schema.json} is gone from an otherwise open database. The backup must fail, name
   * the file, and leave no archive behind for retention to count or for an operator to restore from.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aBackupWhoseSchemaFileIsMissingFailsAndLeavesNoArchive(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);
    try (final Database database = createDatabase()) {
      final File schemaFile = schemaFileOf(database);
      FileUtils.deleteFile(schemaFile);
      assertThat(schemaFile).doesNotExist();

      // rootCause(), because Backup.backupDatabase() wraps whatever the format threw in a BackupException of its
      // own whose message names only the database and the target file
      assertThatThrownBy(() -> new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase())
          .isInstanceOf(BackupException.class)
          .rootCause().isInstanceOf(BackupException.class)
          .hasMessageContaining(LocalSchema.SCHEMA_FILE_NAME);

      assertThat(new File(BACKUP_FILE))
          .as("a backup that failed must not leave an archive a restore would accept").doesNotExist();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * A zero-length {@code schema.json} is not caught by a presence check, and it is the quieter half of the same
   * defect: {@code LocalSchema.readConfiguration()} treats {@code length() == 0} exactly as it treats a missing
   * file, so such an archive restores to a database that opens cleanly with an EMPTY schema - every type
   * invisible, and no error anywhere. The archive has to carry bytes, not just an entry name.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aBackupWhoseSchemaFileIsEmptyFailsToo(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);
    try (final Database database = createDatabase()) {
      final File schemaFile = schemaFileOf(database);
      new FileOutputStream(schemaFile).close();
      assertThat(schemaFile).exists();
      assertThat(schemaFile.length()).isZero();

      // rootCause(), because Backup.backupDatabase() wraps whatever the format threw in a BackupException of its
      // own whose message names only the database and the target file
      assertThatThrownBy(() -> new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase())
          .isInstanceOf(BackupException.class)
          .rootCause().isInstanceOf(BackupException.class)
          .hasMessageContaining(LocalSchema.SCHEMA_FILE_NAME);

      assertThat(new File(BACKUP_FILE)).doesNotExist();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * The check must not be able to pass by refusing everything: an ordinary backup still completes on both paths
   * and the archive really does carry the entry the refusal is about.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void anOrdinaryBackupStillSucceedsAndCarriesTheSchema(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);
    try (final Database database = createDatabase()) {
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    assertThat(new File(BACKUP_FILE)).exists();
    assertThat(schemaEntrySize()).isPositive();
  }

  /**
   * The case the new refusal could most plausibly break, and the one no other test covers: a database created
   * and never given a type, so nothing has run a DDL. It still has a {@code schema.json} - {@code
   * LocalDatabase.create()} calls {@code schema.saveConfiguration()} directly (LocalDatabase:351), before any
   * type exists - and its backup must complete, restore, and reopen.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aBackupOfABrandNewDatabaseWithNoSchemaYetStillSucceeds(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      assertThat(schemaFileOf(database)).as("a database with no types still has a schema file").exists();

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    assertThat(schemaEntrySize()).isPositive();

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();
    assertThat(new DatabaseFactory(RESTORED_PATH).exists()).isTrue();
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.getSchema().getTypes()).isEmpty();
    }
    TestHelper.checkActiveDatabases();
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static long schemaEntrySize() throws Exception {
    final List<String> names = new ArrayList<>();
    try (final ZipFile zip = new ZipFile(BACKUP_FILE)) {
      final Enumeration<? extends ZipEntry> it = zip.entries();
      while (it.hasMoreElements()) {
        final ZipEntry entry = it.nextElement();
        names.add(entry.getName());
        if (LocalSchema.SCHEMA_FILE_NAME.equals(entry.getName()))
          return entry.getSize();
      }
    }
    throw new AssertionError("the archive carries no '" + LocalSchema.SCHEMA_FILE_NAME + "', only " + names);
  }

  private static File schemaFileOf(final Database database) {
    return ((LocalSchema) database.getSchema()).getConfigurationFile();
  }

  private static Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE);
      for (int i = 0; i < 20; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
    return database;
  }
}
