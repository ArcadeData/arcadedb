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
import com.arcadedb.integration.backup.IoThrottler;
import com.arcadedb.integration.backup.format.BackupArchiveWriter;
import com.arcadedb.integration.backup.format.ZipStreamArchiveWriter;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11041: the sequential restore walk reads an archive with {@code ZipInputStream}, which reads local headers only, so
 * an archive with no central directory - the prefix an aborted backup deliberately leaves unterminated, or any copy cut at an
 * entry boundary - was a successful walk that stopped early. The restore reported success over whatever fraction of the
 * records had arrived and {@code CHECK DATABASE} found nothing wrong. That walk is the only one an encrypted or an http(s)
 * archive can take, which includes every server-side {@code restore database <name> <url>}.
 * <p>
 * The partial archives are written the way an aborted backup writes one: the backup's own {@link ZipStreamArchiveWriter},
 * fed the first entries of a real backup, then {@link BackupArchiveWriter#abort() aborted}. The guarantee pinned here: an
 * archive the backup did not finish does not restore on any path, by any transport, and a local one is refused before the
 * database it would replace is deleted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11041UnterminatedArchiveRestoreTest {
  private static final String DATABASE_PATH  = "target/databases/issue11041";
  private static final String RESTORED_PATH  = "target/databases/issue11041-restored";
  private static final String ARCHIVE        = "target/issue11041-full.zip";
  private static final String PARTIAL        = "target/issue11041-partial.zip";
  private static final String ENCRYPTION_KEY = "Issue11041Hard2GuessPassword";
  private static final int    RECORDS        = 4_000;

  private final List<String> entryNames = new ArrayList<>();

  @BeforeEach
  void buildTheArchive() throws IOException {
    clean();
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(8).create();
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument("Doc").set("id", i).set("payload", "value-" + i).save();
      });
      new Backup(database, ARCHIVE).setVerboseLevel(0).backupDatabase();
    }
    TestHelper.checkActiveDatabases();

    try (final ZipFile zip = new ZipFile(ARCHIVE)) {
      for (final ZipEntry entry : Collections.list(zip.entries()))
        entryNames.add(entry.getName());
    }
    assertThat(entryNames).as("the archive has entries to cut between").hasSizeGreaterThan(8);
  }

  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(ARCHIVE).delete();
    new File(PARTIAL).delete();
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.reset();
  }

  /** The report's probe 1: every entry boundary, on the sequential walk and on the parallel path alike. */
  @Test
  void anArchiveCutAtAnyEntryBoundaryIsRefusedOnEveryPath() throws Exception {
    for (int entries = 1; entries < entryNames.size(); entries++) {
      Files.write(new File(PARTIAL).toPath(), partialArchive(entries, null));
      for (final String threads : new String[] { "0", "-1" }) {
        final String probe = "cut after " + entries + " of " + entryNames.size() + " entries, restoreThreads=" + threads;
        assertThatThrownBy(() -> restore(PARTIAL, "-restoreThreads", threads)).as(probe).isInstanceOf(RestoreException.class)
            .rootCause().hasMessageContaining("is incomplete");
        assertThat(new File(RESTORED_PATH)).as(probe).doesNotExist();
      }
    }
  }

  /** Refused before the destination is touched: an unfinished archive must not cost the database it would replace. */
  @Test
  void theDatabaseARefusedArchiveWouldReplaceIsKept() throws Exception {
    restore(ARCHIVE, "-restoreThreads", "0");
    assertThat(countRestored()).isEqualTo(RECORDS);

    Files.write(new File(PARTIAL).toPath(), partialArchive(entryNames.size() / 2, null));
    assertThatThrownBy(() -> restore(PARTIAL, "-restoreThreads", "0")).isInstanceOf(RestoreException.class);
    assertThat(countRestored()).as("the database the restore would have replaced").isEqualTo(RECORDS);
  }

  /** An encrypted archive on disk: its tail is decrypted without reading the front, and checked before anything is restored. */
  @Test
  void anEncryptedArchiveCutAtAnEntryBoundaryIsRefused() throws Exception {
    restore(ARCHIVE, "-restoreThreads", "0");

    final byte[] partial = partialArchive(entryNames.size() / 2, ENCRYPTION_KEY);
    Files.write(new File(PARTIAL).toPath(), partial);
    assertThatThrownBy(() -> restore(PARTIAL, "-encryptionKey", ENCRYPTION_KEY)).isInstanceOf(RestoreException.class)
        .rootCause().hasMessageContaining("is incomplete");
    assertThat(countRestored()).as("refused before the destination was deleted").isEqualTo(RECORDS);

    // and the complete one, encrypted the same way, restores
    Files.write(new File(PARTIAL).toPath(), partialArchive(entryNames.size(), ENCRYPTION_KEY));
    restore(PARTIAL, "-encryptionKey", ENCRYPTION_KEY);
    assertThat(countRestored()).isEqualTo(RECORDS);
  }

  /**
   * The report's probe 3, the server's own transport: over http(s) the archive can be read only once, so the walk checks the
   * central directory on the bytes it streamed. The complete archives, plain and encrypted, are the positive control.
   */
  @Test
  void overHttpOnlyAFinishedArchiveRestores() throws Exception {
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
    final byte[][] served = new byte[1][];
    final HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/backup.zip", exchange -> {
      exchange.sendResponseHeaders(200, served[0].length);
      try (final OutputStream out = exchange.getResponseBody()) {
        out.write(served[0]);
      }
    });
    server.start();
    try {
      final String url = "http://localhost:" + server.getAddress().getPort() + "/backup.zip";
      final int half = entryNames.size() / 2;

      for (final String key : new String[] { null, ENCRYPTION_KEY }) {
        served[0] = partialArchive(half, key);
        assertThatThrownBy(() -> restoreFromUrl(url, key)).as("key=%s", key).isInstanceOf(RestoreException.class)
            .rootCause().hasMessageContaining("is incomplete");
        assertThat(new File(RESTORED_PATH)).as("nothing a refused restore extracted is left behind").doesNotExist();

        served[0] = partialArchive(entryNames.size(), key);
        restoreFromUrl(url, key);
        assertThat(countRestored()).as("key=%s", key).isEqualTo(RECORDS);
      }
    } finally {
      server.stop(0);
    }
  }

  /** An archive whose central directory is intact but does not describe what the walk read is refused too. */
  @Test
  void anArchiveWhoseDirectoryDoesNotMatchItsEntriesIsRefused() throws Exception {
    final byte[] complete = Files.readAllBytes(new File(ARCHIVE).toPath());
    // AN EXTRA BYTE BETWEEN THE LAST ENTRY AND THE CENTRAL DIRECTORY: THE END RECORD NO LONGER LOCATES THE DIRECTORY IT ENDS
    final int eocd = lastIndexOf(complete, new byte[] { 0x50, 0x4b, 0x05, 0x06 });
    final long cdOffset = (complete[eocd + 16] & 0xFFL) | (complete[eocd + 17] & 0xFFL) << 8 | (complete[eocd + 18] & 0xFFL) << 16
        | (complete[eocd + 19] & 0xFFL) << 24;
    final ByteArrayOutputStream shifted = new ByteArrayOutputStream();
    shifted.write(complete, 0, (int) cdOffset);
    shifted.write(0);
    shifted.write(complete, (int) cdOffset, complete.length - (int) cdOffset);
    Files.write(new File(PARTIAL).toPath(), shifted.toByteArray());

    assertThatThrownBy(() -> restore(PARTIAL, "-restoreThreads", "0")).isInstanceOf(RestoreException.class)
        .rootCause().hasMessageContaining("is not a complete backup");
  }

  private void restore(final String file, final String option, final String value) {
    final Restore restore = new Restore(("-f " + file + " -d " + RESTORED_PATH + " -o " + option + " " + value).split(" "));
    restore.setLogger(new ConsoleLogger(0, message -> {
    }));
    restore.restoreDatabase();
  }

  private void restoreFromUrl(final String url, final String key) {
    if (key != null)
      restore(url, "-encryptionKey", key);
    else
      restore(url, "-restoreThreads", "0");
  }

  private long countRestored() {
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
      return restored.query("sql", "select count(*) as c from Doc").next().<Number>getProperty("c").longValue();
    }
  }

  /**
   * The first {@code entries} entries of the real backup, written by the backup's own writer and then aborted as a failed
   * backup aborts it: no central directory. With every entry and no abort it is a complete archive. Encrypted the way
   * {@code FullBackupFormat.encryptFile} encrypts, when {@code key} is set.
   */
  private byte[] partialArchive(final int entries, final String key) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    OutputStream out = bytes;
    if (key != null) {
      final SecureRandom random = new SecureRandom();
      final byte[] salt = new byte[16];
      final byte[] iv = new byte[16];
      random.nextBytes(salt);
      random.nextBytes(iv);
      final byte[] derived = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
          .generateSecret(new PBEKeySpec(key.toCharArray(), salt, 65536, 256)).getEncoded();
      final Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(derived, "AES"), new IvParameterSpec(iv));
      bytes.write(salt);
      bytes.write(iv);
      out = new CipherOutputStream(bytes, cipher);
    }

    final BackupArchiveWriter writer = new ZipStreamArchiveWriter(out, 9, new IoThrottler(0));
    try (final ZipFile zip = new ZipFile(ARCHIVE)) {
      for (int i = 0; i < entries; i++) {
        final ZipEntry entry = zip.getEntry(entryNames.get(i));
        try (final InputStream in = zip.getInputStream(entry)) {
          writer.addEntry(entry.getName(), entry.getTime(), in);
        }
      }
    }
    if (entries == entryNames.size())
      writer.close();
    else
      writer.abort();
    out.close();
    return bytes.toByteArray();
  }

  private static int lastIndexOf(final byte[] array, final byte[] pattern) {
    for (int i = array.length - pattern.length; i >= 0; --i) {
      boolean match = true;
      for (int j = 0; j < pattern.length && match; j++)
        match = array[i + j] == pattern[j];
      if (match)
        return i;
    }
    throw new IllegalStateException("pattern " + new String(pattern, StandardCharsets.ISO_8859_1) + " not found");
  }
}
