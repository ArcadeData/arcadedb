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
import com.arcadedb.database.DatabaseComparator;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10773: the restore of an encrypted archive read its 16-byte salt and its 16-byte IV with
 * one {@code InputStream.read(byte[])} each and refused the archive as truncated when that one call returned fewer
 * bytes. A local file practically always fills the request; a socket does not, and a {@code BufferedInputStream} over
 * it returns early as soon as the socket reports nothing more available. So a valid backup fetched over http(s) could
 * be refused for no reason but the way the network split it.
 * <p>
 * The archive is served over loopback HTTP, in two writes with a pause between them, and the split is put INSIDE the
 * salt, then inside the IV: the first read of each header field meets a chunk boundary with nothing else available.
 * A restore from a local file would pass with the bug in place, which is why this test does not use one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10773EncryptedRestoreShortReadTest {
  private static final String DATABASE_PATH  = "target/databases/issue10773";
  private static final String RESTORED_PATH  = "target/databases/issue10773-restored";
  private static final String ENCRYPTED_FILE = "target/issue10773-encrypted.zip";
  private static final String ENCRYPTION_KEY = "Issue10773Hard2GuessPassword";

  @BeforeEach
  void buildTheArchive() {
    clean();
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.getSchema().createDocumentType("Item");
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          database.newDocument("Item").set("id", i).save();
      });
      new Backup(database, ENCRYPTED_FILE).setVerboseLevel(0).setEncryptionKey(ENCRYPTION_KEY).backupDatabase();
    }
    TestHelper.checkActiveDatabases();
  }

  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(ENCRYPTED_FILE).delete();
  }

  /**
   * @param splitAt where the archive is cut in two: 8 falls inside the salt (bytes 0-15), 24 inside the IV (16-31)
   */
  @ParameterizedTest
  @ValueSource(ints = { 8, 24 })
  void anEncryptedArchiveSplitInsideItsHeaderRestoresOverHttp(final int splitAt) throws Exception {
    final byte[] archive = Files.readAllBytes(new File(ENCRYPTED_FILE).toPath());

    final HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/backup.zip", exchange -> {
      // LENGTH 0 = CHUNKED: EACH FLUSH BELOW REACHES THE CLIENT AS ITS OWN CHUNK
      exchange.sendResponseHeaders(200, 0);
      try (final OutputStream out = exchange.getResponseBody()) {
        out.write(archive, 0, splitAt);
        out.flush();
        // LONG ENOUGH FOR THE CLIENT TO HAVE CONSUMED THE FIRST CHUNK AND FIND NOTHING MORE AVAILABLE
        LockSupport.parkNanos(300_000_000L);
        out.write(archive, splitAt, archive.length - splitAt);
      }
    });
    server.start();
    // SERVED BY THIS TEST OVER LOOPBACK, WHICH THE DEFAULT REFUSES AS A LOCAL ADDRESS (ISSUE #6381)
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
    try {
      final String url = "http://localhost:" + server.getAddress().getPort() + "/backup.zip";
      final Restore restore = new Restore(
          ("-f " + url + " -d " + RESTORED_PATH + " -o -encryptionKey " + ENCRYPTION_KEY).split(" "));
      restore.setLogger(new ConsoleLogger(0, message -> {
      }));
      restore.restoreDatabase();

      try (final Database source = new DatabaseFactory(DATABASE_PATH).open(ComponentFile.MODE.READ_ONLY);
           final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(source, restored);
      }
    } finally {
      server.stop(0);
      GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.reset();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The other half of the contract: an archive that really ends inside its header is still refused, and the refusal now
   * says how much of the field there was.
   */
  @Test
  void anArchiveEndingInsideItsHeaderIsStillRefused() throws Exception {
    final byte[] archive = Files.readAllBytes(new File(ENCRYPTED_FILE).toPath());
    final File truncated = new File("target/issue10773-truncated.zip");
    Files.write(truncated.toPath(), Arrays.copyOf(archive, 20));
    try {
      final Restore restore = new Restore(
          ("-f " + truncated.getPath() + " -d " + RESTORED_PATH + " -o -encryptionKey " + ENCRYPTION_KEY).split(" "));
      restore.setLogger(new ConsoleLogger(0, message -> {
      }));

      assertThatThrownBy(restore::restoreDatabase).isInstanceOf(RestoreException.class).rootCause()
          .hasMessageContaining("Unable to read IV").hasMessageContaining("ends after 4 of its 16 bytes");
    } finally {
      truncated.delete();
    }
  }
}
