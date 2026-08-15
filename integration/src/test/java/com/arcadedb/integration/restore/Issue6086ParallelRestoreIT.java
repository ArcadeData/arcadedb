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
import com.arcadedb.integration.backup.IoThrottler;
import com.arcadedb.integration.backup.format.ParallelZipArchiveWriter;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end coverage of the parallel restore introduced by issue #6086. The contract being defended is that the
 * thread count changes nothing but the duration: whatever it is set to, and whichever of the three input sources the
 * archive comes from, the restored database is the same one - and the two sources that cannot be read at random,
 * http(s) and encrypted, keep working by falling back rather than by failing.
 */
class Issue6086ParallelRestoreIT {
  private static final String DATABASE_PATH  = "target/databases/parallel-restore";
  private static final String RESTORED_PATH  = "target/databases/parallel-restore-restored";
  private static final String BACKUP_FILE    = "target/parallel-restore.zip";
  private static final String ENCRYPTED_FILE = "target/parallel-restore-encrypted.zip";
  private static final String LEGACY_FILE    = "target/parallel-restore-legacy.zip";
  private static final String ENCRYPTION_KEY = "AnotherTestWithHard2GuessPassword";
  private static final int    RECORDS        = 20_000;

  /** What was logged, and by which thread - the thread is what tells the two restore paths apart. */
  private record LogLine(String thread, String message) {
  }

  @BeforeAll
  static void buildTheArchives() throws Exception {
    clean();
    try (final Database database = createDatabase()) {
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
      new Backup(database, ENCRYPTED_FILE).setVerboseLevel(0).setEncryptionKey(ENCRYPTION_KEY).backupDatabase();
      // THE SHAPE EVERY BACKUP HAD BEFORE #6072: THE SINGLE-THREADED WRITER AT LEVEL 9
      new Backup(database, LEGACY_FILE).setVerboseLevel(0).setCompressionThreads(0).setCompressionLevel(9).backupDatabase();
    }
    TestHelper.checkActiveDatabases();
  }

  @AfterAll
  static void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
    new File(ENCRYPTED_FILE).delete();
    new File(LEGACY_FILE).delete();
  }

  /**
   * The whole point of the issue, and the only assertion that can catch a restore that is fast and wrong: whatever the
   * thread count - the legacy sequential walk, one worker, several, or the automatic sizing - the database that comes
   * out is the one that went in.
   */
  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 8, -1 })
  void restoresTheSameDatabaseWhateverTheThreadCount(final int threads) throws Exception {
    FileUtils.deleteRecursively(new File(RESTORED_PATH));

    final List<LogLine> log = restore(BACKUP_FILE, RESTORED_PATH, threads, null);

    assertThat(header(log)).contains(threads == 0 ? "single threaded" : "threads");

    try (final Database source = new DatabaseFactory(DATABASE_PATH).open(ComponentFile.MODE.READ_ONLY);
         final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
      new DatabaseComparator().compare(source, restored);
      assertThat(restored.countType("Doc", true)).isEqualTo(RECORDS);
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * "The same database" as {@code DatabaseComparator} understands it is a strong claim, but it is a claim about
   * contents, not about bytes. The files the parallel path writes have to be identical to the ones the sequential path
   * writes, byte for byte, because that is what makes the two paths interchangeable rather than merely equivalent.
   */
  @Test
  void theParallelPathWritesTheSameBytesAsTheSequentialOne() throws Exception {
    final String sequentialPath = RESTORED_PATH + "_sequential";
    try {
      FileUtils.deleteRecursively(new File(sequentialPath));
      FileUtils.deleteRecursively(new File(RESTORED_PATH));

      final List<LogLine> sequentialLog = restore(BACKUP_FILE, sequentialPath, 0, null);
      final List<LogLine> parallelLog = restore(BACKUP_FILE, RESTORED_PATH, 8, null);

      // PROVE THE TWO RUNS REALLY TOOK DIFFERENT PATHS, OR THIS TEST WOULD BE COMPARING A PATH WITH ITSELF
      assertThat(header(sequentialLog)).contains("single threaded");
      assertThat(header(parallelLog)).contains("8 threads");
      assertThat(entryLines(parallelLog)).isNotEmpty()
          .allMatch(line -> line.thread().startsWith("arcadedb-restore-inflater-"));
      assertThat(entryLines(sequentialLog)).isEmpty();

      assertSameFiles(new File(sequentialPath), new File(RESTORED_PATH));
    } finally {
      FileUtils.deleteRecursively(new File(sequentialPath));
    }
  }

  /**
   * The two input sources that cannot be opened for random access. Neither may fail, and neither may silently take the
   * parallel path: a {@code CipherInputStream} only decrypts front to back and an http body is a one-shot stream, so
   * both have to be recognised before the archive is opened, not after.
   */
  @Test
  void anEncryptedArchiveFallsBackToTheSequentialWalk() throws Exception {
    FileUtils.deleteRecursively(new File(RESTORED_PATH));

    final List<LogLine> log = restore(ENCRYPTED_FILE, RESTORED_PATH, 8, ENCRYPTION_KEY);

    assertThat(header(log)).contains("single threaded");

    try (final Database source = new DatabaseFactory(DATABASE_PATH).open(ComponentFile.MODE.READ_ONLY);
         final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
      new DatabaseComparator().compare(source, restored);
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void anArchiveReadOverHttpFallsBackToTheSequentialWalk() throws Exception {
    FileUtils.deleteRecursively(new File(RESTORED_PATH));

    final HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    final byte[] archive = Files.readAllBytes(new File(BACKUP_FILE).toPath());
    server.createContext("/backup.zip", exchange -> {
      exchange.sendResponseHeaders(200, archive.length);
      try (final OutputStream out = exchange.getResponseBody()) {
        out.write(archive);
      }
    });
    server.start();
    try {
      final String url = "http://localhost:" + server.getAddress().getPort() + "/backup.zip";
      final List<LogLine> log = restore(url, RESTORED_PATH, 8, null);

      assertThat(header(log)).contains("single threaded");

      try (final Database source = new DatabaseFactory(DATABASE_PATH).open(ComponentFile.MODE.READ_ONLY);
           final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(source, restored);
      }
    } finally {
      server.stop(0);
    }
    TestHelper.checkActiveDatabases();
  }

  /** An archive written the way every archive was written before #6072 has to restore through the new path unchanged. */
  @Test
  void aLegacySingleThreadedArchiveRestoresThroughTheParallelPath() throws Exception {
    FileUtils.deleteRecursively(new File(RESTORED_PATH));

    final List<LogLine> log = restore(LEGACY_FILE, RESTORED_PATH, 8, null);
    assertThat(header(log)).contains("8 threads");

    try (final Database source = new DatabaseFactory(DATABASE_PATH).open(ComponentFile.MODE.READ_ONLY);
         final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
      new DatabaseComparator().compare(source, restored);
    }
    TestHelper.checkActiveDatabases();
  }

  @ParameterizedTest
  @CsvSource({ "0, single threaded", "3, 3 threads" })
  void theGlobalConfigurationDecidesWhenTheSettingIsNotGiven(final int configured, final String expected)
      throws Exception {
    final int old = GlobalConfiguration.RESTORE_THREADS.getValueAsInteger();
    try {
      GlobalConfiguration.RESTORE_THREADS.setValue(configured);
      FileUtils.deleteRecursively(new File(RESTORED_PATH));

      final List<LogLine> log = restore(BACKUP_FILE, RESTORED_PATH, null, null);
      assertThat(header(log)).contains(expected);

      try (final Database source = new DatabaseFactory(DATABASE_PATH).open(ComponentFile.MODE.READ_ONLY);
           final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(source, restored);
      }
    } finally {
      GlobalConfiguration.RESTORE_THREADS.setValue(old);
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * Zip-slip, on both paths. The parallel path can do strictly better than the sequential one and has to: it reads the
   * central directory before it starts, so it knows every entry name up front and can refuse the archive before a
   * single file has been written. The sequential walk only learns a name when it reaches it, so it stops at the bad
   * entry with the good ones already on disk - which is the pre-existing behaviour, asserted here so that the
   * difference is a recorded decision rather than an accident.
   */
  @ParameterizedTest
  @ValueSource(strings = { "../escaped.txt", "sub/escaped.txt", ".." })
  void aHostileEntryNameIsRefusedBeforeAnythingIsWritten(final String hostileName) throws Exception {
    final String hostileArchive = "target/parallel-restore-hostile.zip";
    final File destination = new File(RESTORED_PATH);
    try {
      writeArchive(hostileArchive, "innocent.txt", hostileName);

      FileUtils.deleteRecursively(destination);
      assertThatThrownBy(() -> restore(hostileArchive, RESTORED_PATH, 8, null)).isInstanceOf(RestoreException.class);
      assertThat(destination.list()).as("the parallel path refuses the archive before writing anything").isEmpty();

      FileUtils.deleteRecursively(destination);
      assertThatThrownBy(() -> restore(hostileArchive, RESTORED_PATH, 0, null)).isInstanceOf(RestoreException.class);
    } finally {
      new File(hostileArchive).delete();
      FileUtils.deleteRecursively(destination);
    }
  }

  /**
   * Two entries of the same name are harmless when they are written one after the other - the second wins, which is
   * what the sequential walk does - and are two threads writing one file on the parallel path. No ArcadeDB backup can
   * produce such an archive, so it is refused up front rather than given a locking rule of its own.
   */
  @Test
  void twoEntriesOfTheSameNameAreRefused() throws Exception {
    final String duplicateArchive = "target/parallel-restore-duplicate.zip";
    try {
      // ZipOutputStream REFUSES TO WRITE A DUPLICATE NAME, SO THE ARCHIVE IS BUILT WITH ARCADEDB'S OWN WRITER, WHICH
      // DOES NOT CHECK - WHICH IS ALSO WHY THE CHECK BELONGS ON THE READING SIDE
      try (final FileOutputStream out = new FileOutputStream(duplicateArchive)) {
        final ParallelZipArchiveWriter writer = new ParallelZipArchiveWriter(out, 1, 1, new IoThrottler(0));
        for (int i = 0; i < 2; i++)
          writer.addEntry("twice.txt", System.currentTimeMillis(),
              new ByteArrayInputStream(("copy " + i).getBytes(StandardCharsets.UTF_8)));
        writer.close();
      }

      FileUtils.deleteRecursively(new File(RESTORED_PATH));
      assertThatThrownBy(() -> restore(duplicateArchive, RESTORED_PATH, 8, null)).isInstanceOf(RestoreException.class)
          .hasRootCauseMessage("The backup archive contains two entries named 'twice.txt'");
      assertThat(new File(RESTORED_PATH).list()).isEmpty();
    } finally {
      new File(duplicateArchive).delete();
      FileUtils.deleteRecursively(new File(RESTORED_PATH));
    }
  }

  /** An archive with no entries at all is not a restore, on either path. */
  @ParameterizedTest
  @ValueSource(ints = { 0, 8 })
  void anEmptyArchiveIsRefused(final int threads) throws Exception {
    final String emptyArchive = "target/parallel-restore-empty.zip";
    try {
      writeArchive(emptyArchive);

      FileUtils.deleteRecursively(new File(RESTORED_PATH));
      assertThatThrownBy(() -> restore(emptyArchive, RESTORED_PATH, threads, null))
          .isInstanceOf(RestoreException.class).hasMessageContaining("Error during restore");
    } finally {
      new File(emptyArchive).delete();
      FileUtils.deleteRecursively(new File(RESTORED_PATH));
    }
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static List<LogLine> restore(final String archive, final String destination, final Integer threads,
      final String encryptionKey) {
    final List<LogLine> log = Collections.synchronizedList(new ArrayList<>());

    final Restore restore = encryptionKey != null ?
        new Restore(("-f " + archive + " -d " + destination + " -o -encryptionKey " + encryptionKey).split(" ")) :
        new Restore(archive, destination);
    if (threads != null)
      restore.setRestoreThreads(threads);

    restore.setLogger(new ConsoleLogger(2, message -> log.add(new LogLine(Thread.currentThread().getName(), message))));
    restore.restoreDatabase();
    return log;
  }

  /** The single level-0 line the restore opens with, which states which path it took. */
  private static String header(final List<LogLine> log) {
    return log.stream().map(LogLine::message).filter(message -> message.startsWith("Executing full restore")).findFirst()
        .orElseThrow(() -> new AssertionError("the restore logged no header line: " + log));
  }

  /** The per-entry lines, which only the parallel path emits as a whole line (and from a worker thread). */
  private static List<LogLine> entryLines(final List<LogLine> log) {
    return log.stream().filter(line -> line.message().startsWith("- File '")).toList();
  }

  private static void assertSameFiles(final File expected, final File actual) throws Exception {
    final String[] expectedNames = expected.list();
    final String[] actualNames = actual.list();
    assertThat(expectedNames).isNotNull();
    assertThat(actualNames).containsExactlyInAnyOrder(expectedNames);

    for (final String name : expectedNames)
      assertThat(Files.mismatch(new File(expected, name).toPath(), new File(actual, name).toPath())).as(name)
          .isEqualTo(-1L);
  }

  /** A hand-built archive: the restore has to defend itself against ZIPs no ArcadeDB backup would ever produce. */
  private static void writeArchive(final String path, final String... entryNames) throws Exception {
    try (final ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(path), StandardCharsets.UTF_8)) {
      for (final String entryName : entryNames) {
        zip.putNextEntry(new ZipEntry(entryName));
        zip.write(("content of " + entryName).getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
      }
    }
  }

  private static Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("payload", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    });

    database.begin();
    for (int i = 0; i < RECORDS; i++) {
      database.newVertex("Doc").set("id", i).set("payload", "record-" + i + "-" + "x".repeat(200)).save();
      if (i % 1000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    return database;
  }
}
