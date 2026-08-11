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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
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

  /**
   * A writer that is aborted rather than closed must leave nothing a restore would accept. The partial file is
   * normally deleted, but a delete can fail, and an archive with a valid central directory listing only the entries
   * that made it is a backup that looks complete and is not.
   */
  @Test
  void abortLeavesNoValidArchive() throws Exception {
    final Map<String, byte[]> sources = buildFixture();
    materialise(sources);

    for (final boolean parallel : new boolean[] { true, false }) {
      final File archive = new File(workDirectory, "aborted-" + parallel + ".zip");
      try (final FileOutputStream out = new FileOutputStream(archive)) {
        final BackupArchiveWriter writer = parallel ?
            new ParallelZipArchiveWriter(out, 6, 4, new IoThrottler(0), SMALL_CHUNK) :
            new ZipStreamArchiveWriter(out, 6, new IoThrottler(0));
        writer.addFile(new File(workDirectory, "compressible.bin"));
        writer.abort();
      }

      assertThat(archive).as("parallel=" + parallel).exists();
      assertThatThrownBy(() -> new ZipFile(archive).close()).as("parallel=" + parallel).isInstanceOf(IOException.class);
    }
  }

  /**
   * The other ZIP64 trigger, and the one that actually fires in production: an <b>archive</b> past 4 GB, which any
   * backup of a database of a few tens of GB produces. It puts the central directory beyond the 32-bit offset field,
   * so the ZIP64 end-of-central-directory record and its locator have to be written - a different branch from the
   * >4 GB <i>entry</i> covered above, and one no small fixture reaches.
   * <p>
   * It also pins the field-by-field encoding, which is the subtle part. Only the fields that individually overflow
   * are replaced by their sentinel: here the central-directory offset does, while the entry count and the directory
   * size do not, so those two keep their real values. That is what {@code ZipOutputStream} itself emits - this test
   * asserts against the JDK's own bytes for the same logical archive rather than against a reading of the
   * specification, so the two can never drift.
   * <p>
   * Costs no disk: the source is sparse, the level is 0 so the 4 GB passes through as stored blocks, and both
   * archives are written into a sink that keeps only the tail.
   */
  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void archiveLargerThan4GBMatchesTheJdkEndOfCentralDirectory() throws Exception {
    final long size = 4L * 1024 * 1024 * 1024 + 4096;

    final File source = new File(workDirectory, "huge.bin");
    try (final RandomAccessFile raf = new RandomAccessFile(source, "rw")) {
      raf.setLength(size);
    }

    final TailSink ours = new TailSink();
    try (final BackupArchiveWriter writer = new ParallelZipArchiveWriter(ours, 0, 4, new IoThrottler(0))) {
      writer.addFile(source);
    }

    final TailSink jdk = new TailSink();
    try (final BackupArchiveWriter writer = new ZipStreamArchiveWriter(jdk, 0, new IoThrottler(0))) {
      writer.addFile(source);
    }

    assertThat(ours.total()).as("the archive itself must cross 4GB for this to test anything").isGreaterThan(0xFFFFFFFFL);

    final EndOfCentralDirectory oursEnd = EndOfCentralDirectory.parse(ours.tail());
    final EndOfCentralDirectory jdkEnd = EndOfCentralDirectory.parse(jdk.tail());

    assertThat(oursEnd.hasZip64Locator()).as("ZIP64 locator must be present past 4GB").isTrue();
    assertThat(oursEnd.hasZip64Record()).as("ZIP64 end record must be present past 4GB").isTrue();

    // ONLY THE OVERFLOWED FIELD CARRIES THE SENTINEL, EXACTLY AS THE JDK DOES IT
    assertThat(oursEnd.centralDirectoryOffset()).isEqualTo(0xFFFFFFFFL).isEqualTo(jdkEnd.centralDirectoryOffset());
    assertThat(oursEnd.entriesTotal()).isEqualTo(1).isEqualTo(jdkEnd.entriesTotal());
    assertThat(oursEnd.entriesOnDisk()).isEqualTo(1).isEqualTo(jdkEnd.entriesOnDisk());
    assertThat(oursEnd.centralDirectorySize()).isLessThan(0xFFFFFFFFL);
    assertThat(jdkEnd.centralDirectorySize()).isLessThan(0xFFFFFFFFL);

    // AND THE ZIP64 RECORD CARRIES THE REAL VALUES THE CLASSIC ONE COULD NOT HOLD
    assertThat(oursEnd.zip64EntriesTotal()).isEqualTo(1);
    assertThat(oursEnd.zip64CentralDirectoryOffset()).isGreaterThan(0xFFFFFFFFL);

    source.delete();
  }

  /**
   * Both JDK readers share one implementation of the format, so agreeing with them is weaker evidence than it looks.
   * Info-ZIP is a genuinely independent one, and {@code unzip -t} inflates and CRC-checks rather than just listing.
   * Skipped where the binary is not on PATH, which is why it complements rather than replaces the JDK-based tests.
   * <p>
   * Runs on the ordinary fixture rather than on a multi-GB archive: what is unusual here is the streaming data
   * descriptor, and every entry carries one, so a small archive exercises the interop risk just as well.
   */
  @Test
  void archiveIsAcceptedByAnIndependentUnzipImplementation() throws Exception {
    final File unzip = Stream.of("/usr/bin/unzip", "/bin/unzip", "/usr/local/bin/unzip").map(File::new)
        .filter(File::canExecute).findFirst().orElse(null);
    assumeTrue(unzip != null, "Info-ZIP unzip not available on this machine");

    final Map<String, byte[]> sources = buildFixture();
    final File archive = writeArchive("interop.zip", sources, 6, 4, SMALL_CHUNK);

    final Process process = new ProcessBuilder(unzip.getAbsolutePath(), "-t", archive.getAbsolutePath())
        .redirectErrorStream(true).start();
    final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(process.waitFor(2, TimeUnit.MINUTES)).as("unzip -t did not finish").isTrue();

    assertThat(process.exitValue()).as(output).isZero();
    assertThat(output).contains("No errors detected");
    for (final String name : sources.keySet())
      assertThat(output).contains(name);
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
   * <p>
   * It is cheap despite the size: the source is a sparse file of zeros, so it costs no disk and no real read, and
   * zeros deflate at multiple GB/s, so the archive is ~19 MB and the whole test is a couple of seconds on a developer
   * machine. The explicit timeout is what keeps that a fact rather than an assumption - the unit lane caps the whole
   * job at 60 minutes, and a test moving 4 GB is exactly the shape that could quietly eat that budget on a degraded
   * runner. Ten minutes is two orders of magnitude of headroom and still fails fast and legibly if it is ever wrong.
   */
  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
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
      assertThat(entry.getCompressedSize()).isEqualTo(stats.compressedSize());
    }

    // ONLY THE FIELD THAT ACTUALLY OVERFLOWED GOES INTO THE ZIP64 EXTRA FIELD. HERE THAT IS THE UNCOMPRESSED SIZE
    // ALONE - THE ENTRY COMPRESSES TO A FEW MB AND SITS AT OFFSET 0 - SO THE EXTRA FIELD HOLDS ONE 8-BYTE VALUE, NOT
    // THREE. THE SAME ARCHIVE WRITTEN BY java.util.zip IS THE REFERENCE, SO THE TWO CANNOT DRIFT
    final File jdkArchive = new File(workDirectory, "huge-jdk.zip");
    try (final FileOutputStream out = new FileOutputStream(jdkArchive);
        final BackupArchiveWriter writer = new ZipStreamArchiveWriter(out, 1, new IoThrottler(0))) {
      writer.addFile(source);
    }
    assertThat(zip64ExtraFieldLength(archive, "huge.bin")).isEqualTo(zip64ExtraFieldLength(jdkArchive, "huge.bin"))
        .isEqualTo(8);

    // THE LOCAL HEADER'S "VERSION NEEDED TO EXTRACT" STAYS 2.0 EVEN THOUGH THE ENTRY NEEDS ZIP64, AND THAT IS NOT AN
    // OVERSIGHT: WITH A DATA DESCRIPTOR THE SIZES ARE NOT KNOWN WHEN THAT HEADER IS WRITTEN, SO THERE IS NOTHING TO
    // BASE AN UPGRADE ON - ZipOutputStream.writeLOC TAKES THE SAME BRANCH AND WRITES version(e) FOR A STREAMED ENTRY.
    // ASSERTED AGAINST THE JDK'S OWN BYTES SO NOBODY HAS TO TAKE EITHER READING OF APPNOTE ON TRUST
    assertThat(localHeaderVersionNeeded(archive)).isEqualTo(localHeaderVersionNeeded(jdkArchive)).isEqualTo(20);

    source.delete();
  }

  /** The "version needed to extract" field of the first local file header, at offset 4. */
  private static int localHeaderVersionNeeded(final File archive) throws IOException {
    try (final InputStream in = Files.newInputStream(archive.toPath())) {
      final byte[] header = in.readNBytes(6);
      return (header[4] & 0xFF) | ((header[5] & 0xFF) << 8);
    }
  }

  /** Length in bytes of the ZIP64 extended-information extra field the central directory holds for an entry. */
  private static int zip64ExtraFieldLength(final File archive, final String entryName) throws IOException {
    try (final ZipFile zip = new ZipFile(archive)) {
      final byte[] extra = zip.getEntry(entryName).getExtra();
      if (extra == null)
        return 0;
      for (int i = 0; i + 4 <= extra.length; ) {
        final int id = (extra[i] & 0xFF) | ((extra[i + 1] & 0xFF) << 8);
        final int length = (extra[i + 2] & 0xFF) | ((extra[i + 3] & 0xFF) << 8);
        if (id == 0x0001)
          return length;
        i += 4 + length;
      }
      return 0;
    }
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  /** Swallows an arbitrarily large archive while keeping its last bytes, which is all the end records occupy. */
  private static final class TailSink extends OutputStream {
    private final byte[] tail = new byte[256];
    private       long   total;

    @Override
    public void write(final int b) {
      write(new byte[] { (byte) b }, 0, 1);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
      total += len;
      if (len >= tail.length)
        System.arraycopy(b, off + len - tail.length, tail, 0, tail.length);
      else {
        System.arraycopy(tail, len, tail, 0, tail.length - len);
        System.arraycopy(b, off, tail, tail.length - len, len);
      }
    }

    byte[] tail() {
      return tail;
    }

    long total() {
      return total;
    }
  }

  /**
   * The three end records a ZIP64 archive finishes with, read straight out of the tail bytes: the classic
   * end-of-central-directory (last 22 bytes when there is no comment), the ZIP64 locator (the 20 before it) and the
   * ZIP64 end record (the 56 before that).
   */
  private record EndOfCentralDirectory(long entriesOnDisk, long entriesTotal, long centralDirectorySize,
                                       long centralDirectoryOffset, boolean hasZip64Locator, boolean hasZip64Record,
                                       long zip64EntriesTotal, long zip64CentralDirectoryOffset) {
    static EndOfCentralDirectory parse(final byte[] tail) {
      final int end = tail.length - 22;
      if (u32(tail, end) != 0x06054b50L)
        throw new IllegalStateException("end-of-central-directory signature not at the expected offset");

      final int locator = end - 20;
      final boolean hasLocator = locator >= 0 && u32(tail, locator) == 0x07064b50L;
      final int zip64 = locator - 56;
      final boolean hasRecord = hasLocator && zip64 >= 0 && u32(tail, zip64) == 0x06064b50L;

      return new EndOfCentralDirectory(u16(tail, end + 8), u16(tail, end + 10), u32(tail, end + 12), u32(tail, end + 16),
          hasLocator, hasRecord, hasRecord ? u64(tail, zip64 + 32) : -1, hasRecord ? u64(tail, zip64 + 48) : -1);
    }

    private static long u16(final byte[] b, final int o) {
      return (b[o] & 0xFFL) | ((b[o + 1] & 0xFFL) << 8);
    }

    private static long u32(final byte[] b, final int o) {
      return u16(b, o) | (u16(b, o + 2) << 16);
    }

    private static long u64(final byte[] b, final int o) {
      return u32(b, o) | (u32(b, o + 4) << 32);
    }
  }

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
