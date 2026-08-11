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
package com.arcadedb.integration.backup.format;

import com.arcadedb.integration.backup.IoThrottler;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Format-level coverage for the parallel backup writer (issue #6072). The archive it produces is not a new format: it
 * has to be readable both the way the restore path reads it (a streaming {@code ZipInputStream} walk, which never
 * consults the central directory) and the way any unzip tool reads it (via the central directory, which
 * {@link ZipFile} parses), so every case below is verified through both readers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParallelZipArchiveWriterTest {
  private static final int SMALL_CHUNK = 4096;

  private File workDirectory;

  @BeforeEach
  void setUp() throws IOException {
    workDirectory = Files.createTempDirectory("arcadedb-zip-test").toFile();
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(workDirectory);
  }

  @ParameterizedTest
  @CsvSource({ "1,0", "1,1", "1,6", "1,9", "2,1", "4,1", "8,1", "8,9" })
  void roundTripAcrossThreadsAndLevels(final int threads, final int level) throws Exception {
    final Map<String, byte[]> sources = buildFixture();

    final File archive = writeArchive("archive-%d-%d.zip".formatted(threads, level), sources, level, threads, SMALL_CHUNK);

    assertReadableByStreamingReader(archive, sources);
    assertReadableByCentralDirectoryReader(archive, sources);
  }

  /**
   * The chunk boundary is the only place the concatenated-deflate construction can go wrong, so the sizes around it
   * are covered explicitly rather than left to the general fixture.
   */
  @ParameterizedTest
  @ValueSource(ints = { 0, 1, SMALL_CHUNK - 1, SMALL_CHUNK, SMALL_CHUNK + 1, 2 * SMALL_CHUNK, 2 * SMALL_CHUNK + 1,
      7 * SMALL_CHUNK + 123 })
  void chunkBoundarySizes(final int size) throws Exception {
    final Map<String, byte[]> sources = new LinkedHashMap<>();
    sources.put("boundary.bin", pseudoRandom(size, 42));

    final File archive = writeArchive("boundary-%d.zip".formatted(size), sources, 6, 4, SMALL_CHUNK);

    assertReadableByStreamingReader(archive, sources);
    assertReadableByCentralDirectoryReader(archive, sources);
  }

  /**
   * Whatever the thread count, the bytes handed to the restore path must be identical - the chunking may change the
   * archive, it must never change what comes out of it.
   */
  @Test
  void threadCountDoesNotChangeTheRestoredBytes() throws Exception {
    final Map<String, byte[]> sources = buildFixture();

    final File single = writeArchive("single.zip", sources, 6, 1, SMALL_CHUNK);
    final File parallel = writeArchive("parallel.zip", sources, 6, 8, SMALL_CHUNK);

    assertThat(readWithStreamingReader(parallel)).containsExactlyEntriesOf(readWithStreamingReader(single));
  }

  /**
   * The whole point of phase 1 is trading a little ratio for a lot of speed, so the trade has to stay small: chunking
   * costs ratio because each chunk starts from an empty dictionary, and that cost must not blow up.
   */
  @Test
  void chunkingCostsLittleRatio() throws Exception {
    final Map<String, byte[]> sources = new LinkedHashMap<>();
    sources.put("pagelike.bin", pageLike(16 * 1024 * 1024));

    final long oneChunk = writeArchive("ratio-single.zip", sources, 6, 1, 16 * 1024 * 1024).length();
    final long defaultChunks = writeArchive("ratio-many.zip", sources, 6, 8,
        ParallelZipArchiveWriter.DEFAULT_CHUNK_SIZE).length();

    assertThat(oneChunk).isGreaterThan(0);
    // EACH CHUNK RESTARTS FROM AN EMPTY DICTIONARY, WHICH IS THE PRICE OF THE PARALLELISM. AT THE DEFAULT CHUNK SIZE
    // IT HAS TO STAY IN THE NOISE, NOT SHOW UP AS A MATERIALLY BIGGER ARCHIVE
    assertThat(defaultChunks).isLessThan(oneChunk * 110 / 100);
  }

  @Test
  void producesTheSameEntriesAsTheLegacyWriter() throws Exception {
    final Map<String, byte[]> sources = buildFixture();
    materialise(sources);

    final File legacy = new File(workDirectory, "legacy.zip");
    try (final FileOutputStream out = new FileOutputStream(legacy);
        final BackupArchiveWriter writer = new ZipStreamArchiveWriter(out, 9, new IoThrottler(0))) {
      for (final String name : sources.keySet())
        writer.addFile(new File(workDirectory, name));
    }

    final File parallel = writeArchive("parallel-vs-legacy.zip", sources, 9, 4, SMALL_CHUNK);

    assertThat(readWithStreamingReader(parallel)).containsExactlyEntriesOf(readWithStreamingReader(legacy));
    assertThat(readWithCentralDirectoryReader(parallel)).containsExactlyInAnyOrderEntriesOf(
        readWithCentralDirectoryReader(legacy));
  }

  @Test
  void reportedSizesMatchTheArchive() throws Exception {
    final Map<String, byte[]> sources = buildFixture();
    materialise(sources);
    final File archive = new File(workDirectory, "sizes.zip");

    final Map<String, BackupArchiveWriter.EntryStats> reported = new LinkedHashMap<>();
    try (final FileOutputStream out = new FileOutputStream(archive);
        final BackupArchiveWriter writer = new ParallelZipArchiveWriter(out, 6, 4, new IoThrottler(0), SMALL_CHUNK)) {
      for (final String name : sources.keySet())
        reported.put(name, writer.addFile(new File(workDirectory, name)));
    }

    try (final ZipFile zip = new ZipFile(archive)) {
      for (final Map.Entry<String, BackupArchiveWriter.EntryStats> entry : reported.entrySet()) {
        final ZipEntry zipEntry = zip.getEntry(entry.getKey());
        assertThat(zipEntry).as(entry.getKey()).isNotNull();
        assertThat(zipEntry.getSize()).isEqualTo(entry.getValue().uncompressedSize());
        assertThat(zipEntry.getCompressedSize()).isEqualTo(entry.getValue().compressedSize());
      }
    }
  }

  @Test
  void throttlerCapsTheReadRate() throws Exception {
    final Map<String, byte[]> sources = new LinkedHashMap<>();
    sources.put("throttled.bin", compressible(4 * 1024 * 1024));
    materialise(sources);

    final File archive = new File(workDirectory, "throttled.zip");
    final long begin = System.currentTimeMillis();
    try (final FileOutputStream out = new FileOutputStream(archive);
        final BackupArchiveWriter writer = new ParallelZipArchiveWriter(out, 1, 4, new IoThrottler(4), SMALL_CHUNK)) {
      writer.addFile(new File(workDirectory, "throttled.bin"));
    }
    final long elapsed = System.currentTimeMillis() - begin;

    // 4MB AT 4MB/s CANNOT COMPLETE IN UNDER HALF A SECOND WHATEVER THE HARDWARE
    assertThat(elapsed).isGreaterThan(500);
    assertReadableByStreamingReader(archive, sources);
  }

  @Test
  void rejectsInvalidConfiguration() {
    assertThatThrownBy(() -> new ParallelZipArchiveWriter(OutputStream.nullOutputStream(), 6, 0, new IoThrottler(0)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ParallelZipArchiveWriter(OutputStream.nullOutputStream(), 6, 4, new IoThrottler(0), 8))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * An entry above 4 GB is where the ZIP64 extensions kick in, in the data descriptor as well as in the central
   * directory. It is the riskiest branch of the writer and the one no small fixture can reach, so it gets its own test
   * - deliberately left in the normal lane rather than tagged slow, because the slow lane only covers the engine
   * module and a ZIP64 bug that never runs in CI is a ZIP64 bug nobody finds.
   */
  @Test
  void entryLargerThan4GBUsesZip64() throws Exception {
    final long size = 4L * 1024 * 1024 * 1024 + 1024;

    // THE SOURCE IS A SPARSE FILE OF ZEROS, SO ON ANY MAINSTREAM FILESYSTEM IT COSTS NO DISK AT ALL, AND ZEROS DEFLATE
    // TO ALMOST NOTHING SO THE ARCHIVE STAYS TINY. THE SPACE CHECK IS INSURANCE FOR A FILESYSTEM THAT MATERIALISES IT
    assumeTrue(workDirectory.getUsableSpace() > size + 1024L * 1024 * 1024,
        "not enough free space to materialise a 4GB entry");

    final File source = new File(workDirectory, "huge.bin");
    try (final RandomAccessFile raf = new RandomAccessFile(source, "rw")) {
      raf.setLength(size);
    }

    final File archive = new File(workDirectory, "huge.zip");
    final BackupArchiveWriter.EntryStats stats;
    try (final FileOutputStream out = new FileOutputStream(archive);
        final BackupArchiveWriter writer = new ParallelZipArchiveWriter(out, 1, 4, new IoThrottler(0))) {
      stats = writer.addFile(source);
    }
    assertThat(stats.uncompressedSize()).isEqualTo(size);

    // STREAMING READER: EXERCISES THE ZIP64 DATA DESCRIPTOR
    long streamed = 0;
    final CRC32 crc = new CRC32();
    final byte[] buffer = new byte[1024 * 1024];
    try (final ZipInputStream in = new ZipInputStream(Files.newInputStream(archive.toPath()))) {
      final ZipEntry entry = in.getNextEntry();
      assertThat(entry).isNotNull();
      assertThat(entry.getName()).isEqualTo("huge.bin");
      int read;
      while ((read = in.read(buffer)) > 0) {
        crc.update(buffer, 0, read);
        streamed += read;
      }
      assertThat(in.getNextEntry()).isNull();
    }
    assertThat(streamed).isEqualTo(size);

    final CRC32 expected = new CRC32();
    for (long remaining = size; remaining > 0; remaining -= buffer.length)
      expected.update(buffer, 0, (int) Math.min(buffer.length, remaining));
    assertThat(crc.getValue()).isEqualTo(expected.getValue());

    // CENTRAL DIRECTORY READER: EXERCISES THE ZIP64 EXTRA FIELD
    try (final ZipFile zip = new ZipFile(archive)) {
      final ZipEntry entry = zip.getEntry("huge.bin");
      assertThat(entry).isNotNull();
      assertThat(entry.getSize()).isEqualTo(size);
    }

    source.delete();
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private File writeArchive(final String archiveName, final Map<String, byte[]> sources, final int level,
      final int threads, final int chunkSize) throws IOException {
    materialise(sources);

    final File archive = new File(workDirectory, archiveName);
    try (final FileOutputStream out = new FileOutputStream(archive);
        final BackupArchiveWriter writer = new ParallelZipArchiveWriter(out, level, threads, new IoThrottler(0),
            chunkSize)) {
      for (final String name : sources.keySet())
        writer.addFile(new File(workDirectory, name));
    }
    return archive;
  }

  private void materialise(final Map<String, byte[]> sources) throws IOException {
    for (final Map.Entry<String, byte[]> entry : sources.entrySet()) {
      final File file = new File(workDirectory, entry.getKey());
      if (!file.exists() || file.length() != entry.getValue().length)
        Files.write(file.toPath(), entry.getValue());
    }
  }

  private static Map<String, byte[]> buildFixture() {
    final Map<String, byte[]> sources = new LinkedHashMap<>();
    sources.put("empty.bin", new byte[0]);
    sources.put("tiny.bin", new byte[] { 42 });
    sources.put("exactly-one-chunk.bin", pseudoRandom(SMALL_CHUNK, 1));
    sources.put("one-chunk-plus-one.bin", pseudoRandom(SMALL_CHUNK + 1, 2));
    sources.put("incompressible.bin", pseudoRandom(11 * SMALL_CHUNK + 7, 3));
    sources.put("compressible.bin", compressible(13 * SMALL_CHUNK + 11));
    sources.put("zeros.bin", new byte[5 * SMALL_CHUNK]);
    return sources;
  }

  private static byte[] pseudoRandom(final int size, final long seed) {
    final byte[] data = new byte[size];
    new Random(seed).nextBytes(data);
    return data;
  }

  private static byte[] compressible(final int size) {
    final byte[] data = new byte[size];
    for (int i = 0; i < size; i++)
      data[i] = (byte) ('a' + (i / 37) % 7);
    return data;
  }

  /**
   * Stand-in for a paginated database file: fixed-size pages with a repeating record shape, a few varying bytes per
   * record and a run of unused zeros at the tail of every page. Compresses like real data rather than like a
   * degenerate best or worst case, which is what makes the ratio comparison meaningful.
   */
  private static byte[] pageLike(final int size) {
    final byte[] data = new byte[size];
    final Random random = new Random(7);
    final int pageSize = 65536;
    for (int page = 0; page * pageSize < size; page++) {
      final int base = page * pageSize;
      final int used = pageSize * 3 / 4;
      for (int offset = 0; offset + 64 <= used && base + offset + 64 <= size; offset += 64) {
        for (int i = 0; i < 48; i++)
          data[base + offset + i] = (byte) ('A' + i % 26);
        for (int i = 48; i < 64; i++)
          data[base + offset + i] = (byte) random.nextInt(256);
      }
    }
    return data;
  }

  private void assertReadableByStreamingReader(final File archive, final Map<String, byte[]> expected) throws IOException {
    assertThat(readWithStreamingReader(archive)).containsExactlyEntriesOf(expected);
  }

  private void assertReadableByCentralDirectoryReader(final File archive, final Map<String, byte[]> expected)
      throws IOException {
    assertThat(readWithCentralDirectoryReader(archive)).containsExactlyInAnyOrderEntriesOf(expected);
  }

  private static Map<String, byte[]> readWithStreamingReader(final File archive) throws IOException {
    final Map<String, byte[]> content = new LinkedHashMap<>();
    try (final ZipInputStream in = new ZipInputStream(Files.newInputStream(archive.toPath()))) {
      for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry())
        content.put(entry.getName(), in.readAllBytes());
    }
    return content;
  }

  private static Map<String, byte[]> readWithCentralDirectoryReader(final File archive) throws IOException {
    final Map<String, byte[]> content = new LinkedHashMap<>();
    try (final ZipFile zip = new ZipFile(archive)) {
      final Enumeration<? extends ZipEntry> entries = zip.entries();
      while (entries.hasMoreElements()) {
        final ZipEntry entry = entries.nextElement();
        content.put(entry.getName(), zip.getInputStream(entry).readAllBytes());
      }
    }
    return content;
  }
}
