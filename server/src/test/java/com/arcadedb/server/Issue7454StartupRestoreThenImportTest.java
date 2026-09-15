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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one startup path issue #7454's other tests do not drive: the real {@code loadDefaultDatabases()} loop,
 * with a {@code {restore:…,import:…}} pair.
 * <p>
 * #7454 moved the drop of the database a {@code restore:} command replaces out of that loop and into
 * {@code restoreDatabaseFromStartupCommand}, so the drop happens under the same maintenance slot as the
 * extraction. The loop's own {@code database} local is read once per iteration, before the command list is
 * walked, and a {@code restore:} invalidates it: the instance it names has been dropped, and the {@code import:}
 * case reuses that same local when it is non-null rather than resolving the database again. The loop therefore
 * re-resolves the handle after a restore.
 * <p>
 * This is not a regression the move introduced - the loop dropped the instance and kept the variable before it
 * too - but it is the only consequence of the move that {@code loadDefaultDatabases()} itself can show, so it is
 * pinned here rather than argued.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7454StartupRestoreThenImportTest extends StaticBaseServerTest {
  private static final String DB_NAME         = "restorethenimport7454";
  private static final String SOURCE_DB       = "sourcerti7454";
  private static final String ARCHIVE_NAME    = "backup-rti-7454.zip";
  private static final String RESTORED_TYPE   = "Doc7454RTI";
  private static final int    RESTORED_COUNT  = 12;
  private static final String IMPORTED_TYPE   = "Document";
  private static final int    IMPORTED_COUNT  = 3;

  private ArcadeDBServer server;
  private File           csv;

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    // The startup commands read both URLs from the static global rather than from a ContextConfiguration.
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
  }

  @AfterEach
  @Override
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    server = null;
    if (csv != null)
      csv.delete();
    FileUtils.deleteRecursively(new File("./target/databases/" + DB_NAME));
    FileUtils.deleteRecursively(new File("./target/backups"));
    super.endTest();
  }

  @Test
  @Timeout(180)
  void aRestoreFollowedByAnImportInOneStartupCommandListRunsBoth() throws Exception {
    final File archive = produceArchive();
    csv = writeCsv();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
    // Neither URL may contain a comma: loadDefaultDatabases() splits the '{...}' block on every comma before
    // any command is parsed, so a comma inside one would be read as the start of a second command.
    config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES,
        DB_NAME + "[root]{restore:file://" + archive.getAbsolutePath()
            + ",import:file://" + csv.getAbsolutePath() + "}");

    server = new ArcadeDBServer(config);
    server.start();

    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);

    final Database database = server.getDatabase(DB_NAME);
    assertThat(database.countType(RESTORED_TYPE, false))
        .as("the restore half of the startup command list did not land").isEqualTo(RESTORED_COUNT);
    assertThat(database.getSchema().existsType(IMPORTED_TYPE))
        .as("the import half ran against the handle the restore invalidated").isTrue();
    assertThat(database.countType(IMPORTED_TYPE, false)).isEqualTo(IMPORTED_COUNT);
  }

  private static File produceArchive() {
    FileUtils.deleteRecursively(new File("./target/backups"));
    final String directory = "./target/databases" + File.separator + SOURCE_DB;
    FileUtils.deleteRecursively(new File(directory));

    try (final DatabaseFactory factory = new DatabaseFactory(directory)) {
      try (final Database database = factory.create()) {
        database.getSchema().createDocumentType(RESTORED_TYPE);
        database.transaction(() -> {
          for (int i = 0; i < RESTORED_COUNT; i++)
            database.newDocument(RESTORED_TYPE).set("i", i).save();
        });
        database.command("sql", "backup database file://" + ARCHIVE_NAME).close();
        database.drop();
      }
    }

    final File produced = new File("./target/backups/" + SOURCE_DB + "/" + ARCHIVE_NAME);
    assertThat(produced).exists();
    return produced;
  }

  private static File writeCsv() throws Exception {
    final File file = new File("./target/import-7454-rti.csv").getAbsoluteFile();
    java.nio.file.Files.writeString(file.toPath(), "id,name\n1,a\n2,b\n3,c\n");
    return file;
  }
}
