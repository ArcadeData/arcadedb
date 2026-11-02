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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.backup.Backup;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11050: an overwrite restore ({@code -o}) of an encrypted archive with a mistyped key must leave the database it was
 * going to replace exactly as it found it, and must report the failure as a probable wrong key rather than as the generic
 * "Unable to perform restore". The staging directory of #11058 and the up-front tail check of #11041 provide both; this pins
 * the exact scenario of the report on the local archive and on the http(s) stream, which is the sequential walk.
 */
class Issue11050WrongKeyOverwriteRestoreTest {
  private static final String SOURCE_PATH   = "target/databases/issue11050-source";
  private static final String PROD_PATH     = "target/databases/issue11050-prod";
  private static final String ARCHIVE       = "target/issue11050.zip";
  private static final String RIGHT_KEY     = "Issue11050RightPassword";
  private static final String WRONG_KEY     = "Issue11050WrongPassword";
  private static final int    PROD_RECORDS  = 1_000;
  private static final int    SOURCE_RECORDS = 200;

  @BeforeEach
  void setup() throws IOException {
    clean();
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(4).create();
      source.transaction(() -> {
        for (int i = 0; i < SOURCE_RECORDS; i++)
          source.newDocument("Doc").set("id", i).save();
      });
      new Backup(source, ARCHIVE).setVerboseLevel(0).setEncryptionKey(RIGHT_KEY).backupDatabase();
    }
    try (final Database prod = new DatabaseFactory(PROD_PATH).create()) {
      prod.getSchema().buildDocumentType().withName("Live").withTotalBuckets(4).create();
      prod.transaction(() -> {
        for (int i = 0; i < PROD_RECORDS; i++)
          prod.newDocument("Live").set("id", i).save();
      });
    }
    TestHelper.checkActiveDatabases();
  }

  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(PROD_PATH));
    new File(ARCHIVE).delete();
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.reset();
  }

  @Test
  void aWrongKeyOnALocalArchiveKeepsTheDatabaseAndNamesTheKey() {
    for (final String threads : new String[] { "0", "-1" }) {
      assertThatThrownBy(() -> restore(ARCHIVE, WRONG_KEY, threads)).as("restoreThreads=%s", threads)
          .isInstanceOf(RestoreException.class).rootCause().hasMessageContaining("wrong key");
      assertProdIntact();
    }
  }

  @Test
  void aWrongKeyOverHttpKeepsTheDatabaseAndNamesTheKey() throws Exception {
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
    final byte[] served = Files.readAllBytes(new File(ARCHIVE).toPath());
    final HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/backup.zip", exchange -> {
      exchange.sendResponseHeaders(200, served.length);
      try (final OutputStream out = exchange.getResponseBody()) {
        out.write(served);
      }
    });
    server.start();
    try {
      final String url = "http://localhost:" + server.getAddress().getPort() + "/backup.zip";
      assertThatThrownBy(() -> restore(url, WRONG_KEY, "0")).isInstanceOf(RestoreException.class).rootCause()
          .hasMessageContaining("wrong key");
      assertProdIntact();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void theRightKeyStillReplacesTheDatabase() {
    restore(ARCHIVE, RIGHT_KEY, "0");
    try (final Database restored = new DatabaseFactory(PROD_PATH).open(ComponentFile.MODE.READ_ONLY)) {
      assertThat(restored.countType("Doc", true)).isEqualTo(SOURCE_RECORDS);
    }
  }

  private void assertProdIntact() {
    assertThat(new DatabaseFactory(PROD_PATH).exists()).isTrue();
    try (final Database prod = new DatabaseFactory(PROD_PATH).open(ComponentFile.MODE.READ_ONLY)) {
      assertThat(prod.countType("Live", true)).isEqualTo(PROD_RECORDS);
    }
    final File[] leftovers = new File(PROD_PATH).getParentFile().listFiles((dir, name) -> name.startsWith(".issue11050-prod"));
    assertThat(leftovers).isEmpty();
  }

  private void restore(final String file, final String key, final String threads) {
    final Restore restore = new Restore(
        ("-f " + file + " -d " + PROD_PATH + " -o -encryptionKey " + key + " -restoreThreads " + threads).split(" "));
    restore.setLogger(new ConsoleLogger(0, message -> {
    }));
    restore.restoreDatabase();
  }
}
