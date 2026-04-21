/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SnapshotInstaller.CountingInputStream} byte tracking
 * and the compression ratio check in {@link SnapshotInstaller#copyWithLimit}.
 */
class SnapshotCompressionRatioTest {

  // -- CountingInputStream tests --

  @Test
  void countingInputStream_tracksReadBytes() throws Exception {
    final byte[] data = new byte[1024];
    Arrays.fill(data, (byte) 0x42);

    final SnapshotInstaller.CountingInputStream counter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(data));

    assertThat(counter.getCount()).isEqualTo(0);

    final byte[] buf = new byte[256];
    counter.read(buf);
    assertThat(counter.getCount()).isEqualTo(256);

    counter.read(buf);
    assertThat(counter.getCount()).isEqualTo(512);
  }

  @Test
  void countingInputStream_tracksReadAllBytes() throws Exception {
    final byte[] data = new byte[4096];
    Arrays.fill(data, (byte) 0xAB);

    final SnapshotInstaller.CountingInputStream counter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(data));

    counter.readAllBytes();
    assertThat(counter.getCount()).isEqualTo(4096);
  }

  @Test
  void countingInputStream_tracksSingleByteRead() throws Exception {
    final byte[] data = { 0x01, 0x02, 0x03 };
    final SnapshotInstaller.CountingInputStream counter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(data));

    counter.read(); // read one byte
    assertThat(counter.getCount()).isEqualTo(1);

    counter.read();
    assertThat(counter.getCount()).isEqualTo(2);
  }

  @Test
  void countingInputStream_tracksAcrossMultipleReads() throws Exception {
    final byte[] data = new byte[512];
    final SnapshotInstaller.CountingInputStream counter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(data));

    final byte[] small = new byte[100];
    counter.read(small, 0, 100);
    assertThat(counter.getCount()).isEqualTo(100);

    counter.read(small, 0, 100);
    assertThat(counter.getCount()).isEqualTo(200);

    counter.readAllBytes();
    assertThat(counter.getCount()).isEqualTo(512);
  }

  @Test
  void countingInputStream_eofDoesNotIncrementCount() throws Exception {
    final SnapshotInstaller.CountingInputStream counter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(new byte[0]));

    final int result = counter.read();
    assertThat(result).isEqualTo(-1);
    assertThat(counter.getCount()).isEqualTo(0);
  }

  @Test
  void countingInputStream_markNotSupported() throws Exception {
    final SnapshotInstaller.CountingInputStream counter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(new byte[10]));
    assertThat(counter.markSupported()).isFalse();
  }

  // -- copyWithLimit tests --

  @Test
  void copyWithLimit_returnsExactByteCopied() throws Exception {
    final byte[] data = new byte[1000];
    Arrays.fill(data, (byte) 0x55);

    final long copied = SnapshotInstaller.copyWithLimit(
        new ByteArrayInputStream(data),
        new ByteArrayOutputStream(),
        Long.MAX_VALUE,
        "test-entry");

    assertThat(copied).isEqualTo(1000);
  }

  @Test
  void copyWithLimit_throwsWhenLimitExceeded() {
    final byte[] data = new byte[1001];

    assertThatThrownBy(() -> SnapshotInstaller.copyWithLimit(
        new ByteArrayInputStream(data),
        new ByteArrayOutputStream(),
        1000L,
        "big-entry"))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("big-entry")
        .hasMessageContaining("size limit");
  }

  @Test
  void copyWithLimit_exactlyAtLimitSucceeds() throws Exception {
    final byte[] data = new byte[100];

    final long copied = SnapshotInstaller.copyWithLimit(
        new ByteArrayInputStream(data),
        new ByteArrayOutputStream(),
        100L,
        "exact-entry");

    assertThat(copied).isEqualTo(100);
  }

  // -- Compression ratio constant tests --

  @Test
  void compressionRatioConstants_haveExpectedValues() {
    assertThat(SnapshotInstaller.MAX_COMPRESSION_RATIO).isEqualTo(100_000);
    assertThat(SnapshotInstaller.MIN_RATIO_CHECK_BYTES).isEqualTo(64L * 1024L);
  }

  /**
   * Regression: a freshly-initialised ArcadeDB dictionary page (327 680 bytes, mostly zeros)
   * compresses at ~946:1 with DEFLATE. The ratio guard must not reject it.
   */
  @Test
  void sparsePageSizedEntry_doesNotExceedRatioLimit() throws Exception {
    // Dictionary.DEF_PAGE_SIZE = 65536 * 5 = 327 680 bytes, freshly initialised → mostly zeros
    final int dictionaryPageSize = 65_536 * 5;
    final byte[] sparseData = new byte[dictionaryPageSize]; // all-zeros: highly compressible

    final byte[] zipped;
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(new ZipEntry("dictionary.0.327680.v0.dict"));
      zos.write(sparseData);
      zos.closeEntry();
      zos.finish();
      zipped = baos.toByteArray();
    }

    final SnapshotInstaller.CountingInputStream rawCounter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(zipped));

    try (final ZipInputStream zipIn = new ZipInputStream(rawCounter)) {
      final ZipEntry entry = zipIn.getNextEntry();
      assertThat(entry).isNotNull();

      final long compressedStart = rawCounter.getCount();
      final long extracted = SnapshotInstaller.copyWithLimit(zipIn, OutputStream.nullOutputStream(), Long.MAX_VALUE, entry.getName());
      zipIn.closeEntry();

      final long compressedBytes = Math.max(1L, rawCounter.getCount() - compressedStart);
      final long ratio = extracted / compressedBytes;

      // Confirm this is genuinely a high-ratio entry (must exceed old too-tight threshold of 200)
      assertThat(ratio).isGreaterThan(200L);
      // Confirm it stays within the updated limit
      assertThat(ratio).isLessThan(SnapshotInstaller.MAX_COMPRESSION_RATIO);
    }
  }

  // -- Integration: CountingInputStream with ZipInputStream --

  @Test
  void countingInputStream_measuresCompressedBytesPerEntry() throws Exception {
    // Build a small ZIP in memory
    final byte[] uncompressedData = new byte[10_000];
    Arrays.fill(uncompressedData, (byte) 0xCC); // highly compressible

    final byte[] zipped;
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(new ZipEntry("data.bin"));
      zos.write(uncompressedData);
      zos.closeEntry();
      zos.finish();
      zipped = baos.toByteArray();
    }

    final SnapshotInstaller.CountingInputStream rawCounter = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(zipped));

    try (final ZipInputStream zipIn = new ZipInputStream(rawCounter)) {
      final ZipEntry entry = zipIn.getNextEntry();
      assertThat(entry).isNotNull();

      final long compressedStart = rawCounter.getCount();
      final long extracted = SnapshotInstaller.copyWithLimit(zipIn, OutputStream.nullOutputStream(), Long.MAX_VALUE, entry.getName());
      zipIn.closeEntry();

      final long compressedBytes = Math.max(1L, rawCounter.getCount() - compressedStart);

      assertThat(extracted).isEqualTo(10_000);
      // The compressed size must be less than the uncompressed size for highly compressible data
      assertThat(compressedBytes).isLessThan(extracted);
      // Ratio should be well below the bomb threshold for this legitimate data
      assertThat(extracted / compressedBytes).isLessThan(SnapshotInstaller.MAX_COMPRESSION_RATIO);
    }
  }
}
