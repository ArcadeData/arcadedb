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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7586, defect B: on Windows the dictionary's file path was assembled with a literal '/' while every other
 * component path used the platform {@code File.separator}, and a {@code ComponentFile.open()} lookup keyed on
 * {@code File.separator} alone missed the '/', leaving the database directory name in the dictionary's parsed file
 * name. The snapshot-based full backup then archived that file name verbatim, so the dictionary entry alone
 * carried a {@code <database-name>/} prefix while every other entry sat at the archive root - an archive a restore
 * could not open.
 * <p>
 * Neither the mixed separator nor a nested entry can be manufactured through the public API on this JVM's own
 * platform (the database path is built consistently with one separator throughout, here as on Windows before this
 * fix), so what is pinned instead is the invariant defect B broke: every entry in a full-backup archive sits at
 * the archive root, and the archive restores intact. {@code LocalSchema}, {@code TimeSeriesShard} and
 * {@code TimeSeriesTagDictionary} now build every component path with {@code File.separator} rather than a
 * literal '/', {@code ComponentFile.open()} strips a directory built with either separator, and
 * {@code FullBackupFormat.compressEntry} strips one defensively as well - so a future path built with the wrong
 * separator on some other platform cannot repeat this failure silently.
 * <p>
 * Issue #7758: the frozen-files run ({@code [2]}) restored 0 of the 50 records, intermittently, in the CI lane. The
 * cause was the backup, not the test: a flush suspension that began while a commit's pages were still queued froze the
 * files without them, so the archive carried the bucket as it stood before the commit (issue #8111, fixed by PR #8128
 * and pinned in the engine by {@code Issue8111SuspendFlushDrainsPendingPagesTest}). What #7758 asked of the test is
 * kept here: each parameter has its own paths and chooses the backup path on its own database's configuration, and the
 * archive is checked for the committed records BEFORE the restore, so a recurrence names the half that lost them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7586BackupArchiveEntriesSitAtArchiveRootIT {
  private static final String TYPE = "Issue7586Doc";

  @BeforeEach
  @AfterEach
  void clean() {
    for (final boolean snapshot : new boolean[] { true, false }) {
      FileUtils.deleteRecursively(new File(databasePath(snapshot)));
      FileUtils.deleteRecursively(new File(restoredPath(snapshot)));
      new File(backupFile(snapshot)).delete();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void everyArchiveEntrySitsAtTheArchiveRootAndTheArchiveRestoresIntact(final boolean snapshot) throws Exception {
    final String backupFile = backupFile(snapshot);

    // Chosen on THIS database's configuration, which is what FullBackupFormat reads, rather than on the process-wide
    // setting, which any other test in the JVM can read or change (issue #7758)
    final DatabaseFactory factory = new DatabaseFactory(databasePath(snapshot));
    factory.getContextConfiguration().setValue(GlobalConfiguration.PAGE_SNAPSHOT_ENABLED, snapshot);
    try (final Database database = factory.create()) {
      assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.PAGE_SNAPSHOT_ENABLED))
          .isEqualTo(snapshot);
      createSchema(database);
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newDocument(TYPE).set("id", i).set("name", "issue7586-" + i).save();
      });

      new Backup(database, backupFile).setVerboseLevel(0).backupDatabase();
    }

    final List<String> entries = new ArrayList<>();
    final ByteArrayOutputStream archivedBuckets = new ByteArrayOutputStream();
    try (final ZipFile zip = new ZipFile(backupFile)) {
      final Enumeration<? extends ZipEntry> it = zip.entries();
      while (it.hasMoreElements()) {
        final ZipEntry entry = it.nextElement();
        entries.add(entry.getName());
        if (entry.getName().startsWith(TYPE) && entry.getName().endsWith("." + LocalBucket.BUCKET_EXT))
          try (final InputStream in = zip.getInputStream(entry)) {
            in.transferTo(archivedBuckets);
          }
      }
    }

    assertThat(entries).isNotEmpty();
    for (final String entry : entries)
      assertThat(entry).as("archive entry '%s' must sit at the archive root", entry).doesNotContain("/", "\\");

    // Records keep their strings verbatim, so every committed name is in the archived bucket pages - or the backup of
    // the open database froze the bucket without them, which is the half issue #7758 could not tell from the restore
    final String archivedBucketBytes = archivedBuckets.toString(StandardCharsets.ISO_8859_1);
    for (int i = 0; i < 50; i++)
      assertThat(archivedBucketBytes).as("the archive must carry the committed record 'issue7586-%d'", i)
          .contains("issue7586-" + i);

    new Restore(backupFile, restoredPath(snapshot)).setVerboseLevel(0).restoreDatabase();

    try (final Database restored = new DatabaseFactory(restoredPath(snapshot)).open()) {
      assertThat(restored.countType(TYPE, false)).isEqualTo(50);
      assertThat(restored.command("sql", "check database").nextIfAvailable().<Long>getProperty("totalErrors"))
          .isZero();
    }
    TestHelper.checkActiveDatabases();
  }

  /** One set of paths per parameter, so the two runs never share a directory or an archive (issue #7758). */
  private static String databasePath(final boolean snapshot) {
    return "target/databases/issue7586-backup-root-" + (snapshot ? "snapshot" : "frozen");
  }

  private static String restoredPath(final boolean snapshot) {
    return databasePath(snapshot) + "-restored";
  }

  private static String backupFile(final boolean snapshot) {
    return "target/issue7586-backup-root-" + (snapshot ? "snapshot" : "frozen") + ".zip";
  }

  private static void createSchema(final Database database) {
    final Schema schema = database.getSchema();
    final Property property = schema.createDocumentType(TYPE).createProperty("name", Type.STRING);
    property.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
  }
}
