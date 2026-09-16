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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7637: neither full-backup path archived {@code schema.prev.json}, so a restored database had no schema
 * fallback until its first DDL recreated one.
 * <p>
 * That file is the copy {@code LocalSchema.readConfiguration()} falls back to when {@code schema.json} is missing,
 * zero-length or unparseable, and the second arm of {@code DatabaseFactory.exists()}. Before #7464 its absence was
 * part of a real failure - a backup that silently skipped {@code schema.json} produced an archive satisfying
 * neither arm, so the restored directory was not recognised as a database at all. #7464 closed that by failing the
 * backup instead; what was left is narrower and is what this test pins: the source database had a corruption
 * fallback and the restored copy did not.
 * <p>
 * Both paths are exercised, because the two build their entry list separately -
 * {@code FullBackupFormat.backupFromFrozenFiles} names the files itself and {@code backupFromSnapshot} archives
 * whatever {@code PageManager.captureConfigurationFiles} captured at t0 - and the whole point of adding it to the
 * barrier rather than only to the frozen path is that the archive's file set must not depend on which one ran.
 * <p>
 * Captured inside the t0 barrier rather than read off the live filesystem afterwards, deliberately: read late, the
 * previous copy can hold a generation NEWER than the archived {@code schema.json} (two DDLs after t0 leave it
 * holding the first one's result), which would be a fallback describing a page set the archive does not contain.
 * {@link #theArchivedPreviousSchemaIsTheT0OneNotALaterGeneration} is what pins that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7637BackupArchivesPreviousSchemaTest {
  private static final String DATABASE_PATH = "target/databases/issue7637-backup-prev-schema";
  private static final String RESTORED_PATH = "target/databases/issue7637-backup-prev-schema-restored";
  private static final String BACKUP_FILE   = "target/issue7637-backup-prev-schema.zip";
  private static final String TYPE          = "Issue7637Doc";

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * The round trip: a database that has re-saved its schema at least once has a {@code schema.prev.json}, and the
   * database restored from its backup must have one too - byte-identical, and still a working fallback, which is
   * asserted by removing the restored {@code schema.json} and reopening.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void theRestoredDatabaseKeepsTheSchemaFallbackItsSourceHad(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);
    final String sourcePrevious;
    try (final Database database = createDatabase()) {
      final File previous = previousSchemaFileOf(database);
      assertThat(previous).as("a second DDL must have left the source database a previous schema copy").exists();
      sourcePrevious = Files.readString(previous.toPath(), StandardCharsets.UTF_8);

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    assertThat(entrySize(LocalSchema.SCHEMA_PREV_FILE_NAME))
        .as("the archive must carry the previous schema, with bytes rather than just an entry name").isPositive();

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

    final File restoredPrevious = new File(RESTORED_PATH, LocalSchema.SCHEMA_PREV_FILE_NAME);
    assertThat(restoredPrevious).exists();
    assertThat(Files.readString(restoredPrevious.toPath(), StandardCharsets.UTF_8)).isEqualTo(sourcePrevious);

    // AND IT REALLY WORKS AS A FALLBACK, which is the only reason to ship it: with the primary gone the restored
    // database must still be recognised and still open with its type, exactly as the source database would.
    FileUtils.deleteFile(new File(RESTORED_PATH, LocalSchema.SCHEMA_FILE_NAME));
    assertThat(new DatabaseFactory(RESTORED_PATH).exists())
        .as("the second arm of DatabaseFactory.exists() is schema.prev.json").isTrue();
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.getSchema().existsType(TYPE)).isTrue();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * A database whose schema has never been re-saved has no previous copy at all, and that has to stay a legitimate
   * state: the backup must complete and the archive must carry no zero-length entry a restore would extract as a
   * file where none belongs - which would be worse than the absence, since an empty
   * {@code schema.prev.json} is a fallback that parses to nothing.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aDatabaseWithNoPreviousSchemaStillBacksUpAndRestores(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      final File previous = previousSchemaFileOf(database);
      FileUtils.deleteFile(previous);
      assertThat(previous).doesNotExist();

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    assertThat(entryNames()).doesNotContain(LocalSchema.SCHEMA_PREV_FILE_NAME);

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();
    assertThat(new File(RESTORED_PATH, LocalSchema.SCHEMA_PREV_FILE_NAME)).doesNotExist();
    assertThat(new DatabaseFactory(RESTORED_PATH).exists()).isTrue();
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.getSchema().getTypes()).isEmpty();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The semantic reason the capture belongs inside the t0 barrier. Two DDLs run after the window opens: read off
   * the live filesystem afterwards, {@code schema.prev.json} would then hold the FIRST of them - a generation
   * newer than the archived {@code schema.json}, describing a page set the archive does not contain. The window
   * has to serve the copy as it stood at t0, which is genuinely older than the archived primary.
   * <p>
   * A plain {@code @Test} and not a parameterized one: this is the snapshot path only, because the frozen-files
   * path freezes the database for the duration and so has no "after t0" for a DDL to land in.
   */
  @Test
  void theArchivedPreviousSchemaIsTheT0OneNotALaterGeneration() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
    final String previousAtT0;
    try (final Database database = createDatabase()) {
      previousAtT0 = Files.readString(previousSchemaFileOf(database).toPath(), StandardCharsets.UTF_8);
      assertThat(previousAtT0).doesNotContain("AfterT0First").doesNotContain("AfterT0Second");

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();

      // The two DDLs that would have moved the live previous copy forward had it been read after the window
      database.getSchema().createDocumentType("AfterT0First");
      database.getSchema().createDocumentType("AfterT0Second");
      assertThat(Files.readString(previousSchemaFileOf(database).toPath(), StandardCharsets.UTF_8))
          .as("the live previous copy must really have moved on, or this test proves nothing")
          .contains("AfterT0First");
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();
    assertThat(Files.readString(new File(RESTORED_PATH, LocalSchema.SCHEMA_PREV_FILE_NAME).toPath(),
        StandardCharsets.UTF_8)).isEqualTo(previousAtT0);
    TestHelper.checkActiveDatabases();
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static long entrySize(final String name) throws Exception {
    try (final ZipFile zip = new ZipFile(BACKUP_FILE)) {
      final Enumeration<? extends ZipEntry> it = zip.entries();
      while (it.hasMoreElements()) {
        final ZipEntry entry = it.nextElement();
        if (name.equals(entry.getName()))
          return entry.getSize();
      }
    }
    throw new AssertionError("the archive carries no '" + name + "', only " + entryNames());
  }

  private static List<String> entryNames() throws Exception {
    final List<String> names = new ArrayList<>();
    try (final ZipFile zip = new ZipFile(BACKUP_FILE)) {
      final Enumeration<? extends ZipEntry> it = zip.entries();
      while (it.hasMoreElements())
        names.add(it.nextElement().getName());
    }
    return names;
  }

  private static File previousSchemaFileOf(final Database database) {
    return new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME);
  }

  /**
   * A database with a type and some rows, then a SECOND type. {@code LocalSchema.update()} copies the current
   * {@code schema.json} to {@code schema.prev.json} before publishing the new one, so the previous copy is always
   * the generation immediately before the last save: the second DDL is what guarantees {@link #TYPE} is in it, and
   * so that a fallback opened from it is a database with the type in it rather than an empty one.
   */
  private static Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE);
      for (int i = 0; i < 20; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
    database.getSchema().createDocumentType(TYPE + "Later");
    return database;
  }
}
