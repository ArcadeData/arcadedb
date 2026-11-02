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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11058: a restore that failed part way left the directory it created, with whatever entries it had written, and
 * nothing removed it. Every later restore into that path - the retry of the same command with a good archive included - was
 * then refused with "already exist and '-o' setting is false", so one failure turned a retryable operation into manual
 * filesystem cleanup. With {@code -o} it was worse: the database being replaced was deleted before a byte was read, so a
 * failed restore destroyed it. The restore now extracts into a hidden sibling and moves it into place only on success.
 * <p>
 * {@code threads == 0} is the sequential walk (the one that extracts entries before reaching the cut), anything above it the
 * parallel central-directory extractor of #6086.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11058FailedRestoreLeavesNothingBehindTest {
  private static final String DATABASE_PATH  = "target/databases/issue11058-source";
  private static final String RESTORE_PARENT = "target/databases/issue11058-restores";
  private static final String RESTORED_NAME  = "restored";
  private static final String RESTORED_PATH  = RESTORE_PARENT + "/" + RESTORED_NAME;
  private static final String BACKUP_FILE    = "target/issue11058-good.zip";
  private static final String CUT_FILE       = "target/issue11058-cut.zip";
  private static final String TYPE           = "Issue11058Doc";
  private static final String EXISTING_TYPE  = "Issue11058Existing";
  private static final int    RECORDS        = 2_000;

  @BeforeEach
  void buildTheArchives() throws Exception {
    clean();
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE);
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE).set("id", i).set("payload", "payload-" + i + "-".repeat(i % 50)).save();
      });
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    }
    TestHelper.checkActiveDatabases();
    cutArchive(new File(BACKUP_FILE), new File(CUT_FILE));
  }

  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORE_PARENT));
    new File(BACKUP_FILE).delete();
    new File(CUT_FILE).delete();
  }

  /** The reported shape: a cut archive twice into a fresh target, then the good archive into that same target, no -o. */
  @ParameterizedTest
  @ValueSource(ints = { 0, 4 })
  void aFailedRestoreIsRetryableWithTheSameCommand(final int threads) {
    for (int attempt = 1; attempt <= 2; attempt++) {
      assertThatThrownBy(() -> restore(CUT_FILE, threads, false))
          .as("attempt " + attempt + " must fail on the archive, not on a directory left by the attempt before")
          .isInstanceOf(RestoreException.class)
          .satisfies(e -> assertThat(rootMessage(e)).doesNotContain("already exist"));

      assertThat(new File(RESTORED_PATH)).as("a failed restore must not leave the target behind").doesNotExist();
      assertNoStagingLeftBehind();
    }

    restore(BACKUP_FILE, threads, false);
    assertRestoredDatabase();
    assertNoStagingLeftBehind();
  }

  /** With {@code -o}, a failed restore must leave the database it would have replaced exactly as it was. */
  @ParameterizedTest
  @ValueSource(ints = { 0, 4 })
  void aFailedOverwriteRestoreKeepsTheDatabaseItWouldHaveReplaced(final int threads) {
    try (final Database existing = new DatabaseFactory(RESTORED_PATH).create()) {
      existing.getSchema().createDocumentType(EXISTING_TYPE);
    }

    assertThatThrownBy(() -> restore(CUT_FILE, threads, true)).isInstanceOf(RestoreException.class);

    try (final Database existing = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(existing.getSchema().existsType(EXISTING_TYPE)).as("the database -o would have replaced is intact").isTrue();
      assertThat(existing.getSchema().existsType(TYPE)).isFalse();
    }
    assertNoStagingLeftBehind();

    restore(BACKUP_FILE, threads, true);
    assertRestoredDatabase();
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.getSchema().existsType(EXISTING_TYPE)).as("a successful -o restore still replaces").isFalse();
    }
    assertNoStagingLeftBehind();
  }

  /** The counterweight: an existing target without {@code -o} is still refused, and left untouched. */
  @ParameterizedTest
  @ValueSource(ints = { 0, 4 })
  void anExistingTargetWithoutOverwriteIsStillRefused(final int threads) {
    try (final Database existing = new DatabaseFactory(RESTORED_PATH).create()) {
      existing.getSchema().createDocumentType(EXISTING_TYPE);
    }

    assertThatThrownBy(() -> restore(BACKUP_FILE, threads, false))
        .isInstanceOf(RestoreException.class)
        .satisfies(e -> assertThat(rootMessage(e)).contains("already exist"));

    try (final Database existing = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(existing.getSchema().existsType(EXISTING_TYPE)).isTrue();
    }
    assertNoStagingLeftBehind();
  }

  private static void restore(final String archive, final int threads, final boolean overwrite) {
    final String[] args = overwrite ?
        new String[] { "-f", archive, "-d", RESTORED_PATH, "-o" } :
        new String[] { "-f", archive, "-d", RESTORED_PATH };
    new Restore(args).setRestoreThreads(threads).setVerboseLevel(0).restoreDatabase();
  }

  private static void assertRestoredDatabase() {
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.countType(TYPE, false)).isEqualTo(RECORDS);
    }
    TestHelper.checkActiveDatabases();
  }

  /** The staging directory and the aside copy of a replaced database are hidden siblings of the target. */
  private static void assertNoStagingLeftBehind() {
    final String[] leftovers = new File(RESTORE_PARENT).list((dir, name) -> name.startsWith("." + RESTORED_NAME));
    assertThat(leftovers).as("no staging or replaced directory may survive the restore").isNullOrEmpty();
  }

  private static String rootMessage(final Throwable e) {
    Throwable root = e;
    while (root.getCause() != null)
      root = root.getCause();
    return String.valueOf(root.getMessage());
  }

  /** Two thirds of the archive: the cut lands inside an entry, and the central directory at the end is gone. */
  private static void cutArchive(final File source, final File target) throws IOException {
    final long keep = source.length() * 2 / 3;
    try (final InputStream in = new FileInputStream(source); final OutputStream out = new FileOutputStream(target)) {
      final byte[] buffer = new byte[8192];
      long written = 0;
      int read;
      while (written < keep && (read = in.read(buffer, 0, (int) Math.min(buffer.length, keep - written))) > 0) {
        out.write(buffer, 0, read);
        written += read;
      }
    }
  }
}
