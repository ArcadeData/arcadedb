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
package com.arcadedb.integration.restore;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.backup.Backup;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.function.UnaryOperator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7464, restore side: an archive that carries no schema file at all restores to a directory
 * {@code DatabaseFactory.exists()} does not recognise as a database, and the restore reports success anyway.
 * <p>
 * The backup-side half of #7464 stops this build producing such an archive. It cannot do anything about the ones
 * already on disk, written by every build up to this one - so the restore refuses them instead of extracting
 * them into a directory that will later be reported as "database not found" with no clue as to why.
 * <p>
 * The condition is deliberately the same two-arm one as {@code DatabaseFactory.exists()}: {@code schema.json} OR
 * {@code schema.prev.json}, because {@code LocalSchema.readConfiguration()} loads from the previous copy when the
 * primary is missing or empty. An archive carrying only the fallback really does restore to a working database,
 * so it must not be refused.
 */
class Issue7464RestoreRequiresSchemaTest {
  private static final String DATABASE_PATH = "target/databases/issue7464-restore-schema";
  private static final String RESTORED_PATH = "target/databases/issue7464-restore-schema-restored";
  private static final String BACKUP_FILE   = "target/issue7464-restore-schema.zip";
  private static final String DOCTORED_FILE = "target/issue7464-restore-schema-doctored.zip";
  private static final String TYPE          = "Issue7464Doc";
  private static final int    RECORDS       = 20;

  @BeforeEach
  void buildTheArchive() throws Exception {
    clean();
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE);
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE).set("id", i).save();
      });
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    }
    TestHelper.checkActiveDatabases();
  }

  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
    new File(DOCTORED_FILE).delete();
  }

  /**
   * Both restore paths are covered: {@code threads == 0} selects the sequential {@code ZipInputStream} walk (also
   * the only path an http(s) or encrypted archive has), anything above it the parallel central-directory
   * extractor of #6086. The check has to sit where both of them return through, not in one of the walks.
   */
  @ParameterizedTest
  @ValueSource(ints = { 0, 4 })
  void aRestoreFromAnArchiveWithNoSchemaAtAllIsRefused(final int threads) throws Exception {
    rewriteArchive(name -> LocalSchema.SCHEMA_FILE_NAME.equals(name) ? null : name);

    assertThatThrownBy(() -> new Restore(DOCTORED_FILE, RESTORED_PATH)
        .setRestoreThreads(threads).setVerboseLevel(0).restoreDatabase())
        .isInstanceOf(RestoreException.class)
        .rootCause().isInstanceOf(RestoreException.class)
        .hasMessageContaining(LocalSchema.SCHEMA_FILE_NAME);

    assertThat(new DatabaseFactory(RESTORED_PATH).exists())
        .as("the refusal is exactly about the directory not being a database").isFalse();
  }

  /**
   * A zero-length {@code schema.json} passes an {@code exists()} presence check and still loads as an empty
   * schema, so the restore has to look at the size too - the same {@code length() == 0} test
   * {@code LocalSchema.readConfiguration()} applies before falling back.
   */
  @ParameterizedTest
  @ValueSource(ints = { 0, 4 })
  void aRestoreFromAnArchiveWhoseSchemaIsEmptyIsRefused(final int threads) throws Exception {
    rewriteArchive(name -> name, LocalSchema.SCHEMA_FILE_NAME);

    assertThatThrownBy(() -> new Restore(DOCTORED_FILE, RESTORED_PATH)
        .setRestoreThreads(threads).setVerboseLevel(0).restoreDatabase())
        .isInstanceOf(RestoreException.class)
        .rootCause().isInstanceOf(RestoreException.class)
        .hasMessageContaining(LocalSchema.SCHEMA_FILE_NAME);
  }

  /**
   * The fallback arm: an archive carrying {@code schema.prev.json} and no primary is what
   * {@code DatabaseFactory.exists()} accepts and what {@code readConfiguration()} loads from, so the restore must
   * accept it too. This is the assertion that stops the refusal being tightened into "schema.json or nothing".
   */
  @Test
  void aRestoreFromAnArchiveCarryingOnlyThePreviousSchemaIsAccepted() throws Exception {
    rewriteArchive(name -> LocalSchema.SCHEMA_FILE_NAME.equals(name) ? LocalSchema.SCHEMA_PREV_FILE_NAME : name);

    new Restore(DOCTORED_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

    assertThat(new DatabaseFactory(RESTORED_PATH).exists()).isTrue();
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.countType(TYPE, false)).isEqualTo(RECORDS);
    }
    TestHelper.checkActiveDatabases();
  }

  /** The control: an untouched archive still restores, and to a database that opens and reads back. */
  @ParameterizedTest
  @ValueSource(ints = { 0, 4 })
  void anUntouchedArchiveStillRestores(final int threads) throws Exception {
    new Restore(BACKUP_FILE, RESTORED_PATH).setRestoreThreads(threads).setVerboseLevel(0).restoreDatabase();

    assertThat(new DatabaseFactory(RESTORED_PATH).exists()).isTrue();
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.countType(TYPE, false)).isEqualTo(RECORDS);
    }
    TestHelper.checkActiveDatabases();
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static void rewriteArchive(final UnaryOperator<String> rename) throws IOException {
    rewriteArchive(rename, null);
  }

  /**
   * Copies {@link #BACKUP_FILE} to {@link #DOCTORED_FILE}, applying {@code rename} to every entry name - a
   * {@code null} result drops the entry - and writing {@code emptyEntry}, when named, with no content. This is
   * how an archive from an older build looked, reproduced without having to check one in.
   */
  private static void rewriteArchive(final UnaryOperator<String> rename, final String emptyEntry) throws IOException {
    try (final ZipFile source = new ZipFile(BACKUP_FILE);
        final ZipOutputStream target = new ZipOutputStream(new FileOutputStream(DOCTORED_FILE))) {
      final Enumeration<? extends ZipEntry> it = source.entries();
      while (it.hasMoreElements()) {
        final ZipEntry entry = it.nextElement();
        final String name = rename.apply(entry.getName());
        if (name == null)
          continue;

        target.putNextEntry(new ZipEntry(name));
        if (!name.equals(emptyEntry))
          copy(source.getInputStream(entry), target);
        target.closeEntry();
      }
    }
  }

  private static void copy(final InputStream input, final OutputStream output) throws IOException {
    try (input) {
      final byte[] buffer = new byte[8192];
      int read;
      while ((read = input.read(buffer)) > 0)
        output.write(buffer, 0, read);
    }
  }
}
