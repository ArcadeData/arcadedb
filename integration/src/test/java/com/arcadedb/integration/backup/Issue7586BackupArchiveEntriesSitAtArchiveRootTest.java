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
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7586BackupArchiveEntriesSitAtArchiveRootTest {
  private static final String DATABASE_PATH = "target/databases/issue7586-backup-root";
  private static final String RESTORED_PATH = "target/databases/issue7586-backup-root-restored";
  private static final String BACKUP_FILE   = "target/issue7586-backup-root.zip";
  private static final String TYPE          = "Issue7586Doc";

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void everyArchiveEntrySitsAtTheArchiveRootAndTheArchiveRestoresIntact(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);
    try {
      try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
        createSchema(database);
        database.transaction(() -> {
          for (int i = 0; i < 50; i++)
            database.newDocument(TYPE).set("id", i).set("name", "issue7586-" + i).save();
        });

        new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
      }

      final List<String> entries = new ArrayList<>();
      try (final ZipFile zip = new ZipFile(BACKUP_FILE)) {
        final Enumeration<? extends ZipEntry> it = zip.entries();
        while (it.hasMoreElements())
          entries.add(it.nextElement().getName());
      }

      assertThat(entries).isNotEmpty();
      for (final String entry : entries)
        assertThat(entry).as("archive entry '%s' must sit at the archive root", entry).doesNotContain("/", "\\");

      new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

      try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
        assertThat(restored.countType(TYPE, false)).isEqualTo(50);
        assertThat(restored.command("sql", "check database").nextIfAvailable().<Long>getProperty("totalErrors"))
            .isZero();
      }
      TestHelper.checkActiveDatabases();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    }
  }

  private static void createSchema(final Database database) {
    final Schema schema = database.getSchema();
    final Property property = schema.createDocumentType(TYPE).createProperty("name", Type.STRING);
    property.createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
  }
}
