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
import com.arcadedb.integration.restore.format.ParallelZipExtractor;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
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

  /**
   * The failure that the up-front validation cannot catch: an entry that fails <b>while it is being extracted</b>,
   * with other workers mid-write beside it. The guarantee this pins is the one a caller needs in order to clean up -
   * the server's restore handler deletes the destination directory when a restore fails, and deleting a tree that
   * threads are still writing into is a race - so when {@code extract} throws, no worker may still be writing.
   * <p>
   * The failure is injected where it is deterministic and portable: a directory planted at the target path of the
   * largest entry, which makes that entry's {@code FileOutputStream} throw the moment it opens. Largest, because
   * entries are handed out largest first, so it fails while the others have only just started their megabytes.
   * The extractor is driven directly rather than through {@code Restore}, which would delete the planted directory
   * along with the rest of the destination before extracting.
   */
  @Test
  void aFailureMidExtractionLeavesNoWorkerStillWriting() throws Exception {
    final String archive = "target/parallel-restore-failing.zip";
    final File destination = new File(RESTORED_PATH);
    try {
      // THE PAYLOAD IS ALL ZEROES, WHICH IS THE POINT: IT COSTS A FEW HUNDRED KB OF ARCHIVE AND STILL MAKES THE
      // SURVIVING WORKER WRITE 192 MB, SO "IS ANYTHING STILL WRITING?" IS A QUESTION WITH A STABLE ANSWER RATHER
      // THAN A RACE THAT HAPPENS TO GO THE TEST'S WAY. THE POISONED ENTRY IS DECLARED LARGER SO IT IS HANDED OUT
      // FIRST AND FAILS WHILE THE OTHER WORKER IS AT THE START OF ITS OWN
      try (final FileOutputStream out = new FileOutputStream(archive)) {
        final ParallelZipArchiveWriter writer = new ParallelZipArchiveWriter(out, 1, 2, new IoThrottler(0));
        writer.addEntry("poisoned.bin", System.currentTimeMillis(), new ZeroInputStream(256L * 1024 * 1024));
        writer.addEntry("payload.bin", System.currentTimeMillis(), new ZeroInputStream(192L * 1024 * 1024));
        writer.close();
      }

      FileUtils.deleteRecursively(destination);
      assertThat(new File(destination, "poisoned.bin").mkdirs()).isTrue();

      assertThatThrownBy(() -> new ParallelZipExtractor(2, new ConsoleLogger(0)).extract(new File(archive), destination))
          .isInstanceOf(IOException.class);

      assertThat(liveRestoreThreads()).as("a worker was still running when extract() threw").isEmpty();
      // AND THE FILE IT WAS WRITING IS WHOLE, NOT A PREFIX A LATE WORKER WAS STILL EXTENDING
      assertThat(new File(destination, "payload.bin")).hasSize(192L * 1024 * 1024);
    } finally {
      new File(archive).delete();
      FileUtils.deleteRecursively(destination);
    }
  }

  /**
   * Two names that differ only in case are one file on a case-insensitive filesystem (the default on macOS and
   * Windows) and two files on a case-sensitive one, so the up-front name comparison cannot answer this and must not
   * try: guessing "collision" would refuse an archive that restores perfectly well on Linux, where two ArcadeDB types
   * named {@code Doc} and {@code doc} give exactly such a pair of bucket files. The filesystem is asked instead, at
   * the moment the file is created, so this test asserts whichever answer the filesystem it runs on gives.
   */
  @Test
  void twoNamesDifferingOnlyInCaseFollowTheFilesystem() throws Exception {
    final String archive = "target/parallel-restore-case.zip";
    final File destination = new File(RESTORED_PATH);
    try {
      writeArchive(archive, "Cased.bin", "cased.bin");
      FileUtils.deleteRecursively(destination);
      assertThat(destination.mkdirs()).isTrue();

      if (isCaseInsensitive(destination))
        assertThatThrownBy(() -> new ParallelZipExtractor(2, new ConsoleLogger(0)).extract(new File(archive), destination))
            .isInstanceOf(IOException.class).hasMessageContaining("already exists");
      else {
        assertThat(new ParallelZipExtractor(2, new ConsoleLogger(0)).extract(new File(archive), destination).files())
            .isEqualTo(2);
        assertThat(destination.list()).containsExactlyInAnyOrder("Cased.bin", "cased.bin");
      }
    } finally {
      new File(archive).delete();
      FileUtils.deleteRecursively(destination);
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

  /** A large entry that costs neither heap nor archive: a stream of zeroes, of a length the test picks. */
  private static final class ZeroInputStream extends InputStream {
    private long remaining;

    private ZeroInputStream(final long length) {
      this.remaining = length;
    }

    @Override
    public int read() {
      return remaining-- > 0 ? 0 : -1;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) {
      if (remaining <= 0)
        return -1;
      final int produced = (int) Math.min(len, remaining);
      Arrays.fill(b, off, off + produced, (byte) 0);
      remaining -= produced;
      return produced;
    }
  }

  /** Asks the filesystem under {@code directory} whether it distinguishes two names by case. */
  private static boolean isCaseInsensitive(final File directory) throws IOException {
    final File probe = new File(directory, "CaseProbe");
    try {
      assertThat(probe.createNewFile()).isTrue();
      return new File(directory, "caseprobe").exists();
    } finally {
      probe.delete();
    }
  }

  /** The extractor's own workers, by the name it gives them, that are still alive. */
  private static List<String> liveRestoreThreads() {
    return Thread.getAllStackTraces().keySet().stream().filter(Thread::isAlive)
        .map(Thread::getName).filter(name -> name.startsWith("arcadedb-restore-inflater-")).toList();
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
