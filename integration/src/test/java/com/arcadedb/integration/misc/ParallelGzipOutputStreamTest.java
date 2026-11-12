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
package com.arcadedb.integration.misc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11539: the gzip stream the export writes through, compressed by a pool. What it must keep from
 * {@link GZIPOutputStream}: the bytes read back, ONE gzip member (header, one deflate stream, one trailer over the whole
 * input), the same header, and about the same ratio at the same level.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParallelGzipOutputStreamTest {
  private static final int SMALL_CHUNK = 1024;

  @Test
  void everySizeLevelAndThreadCountReadsBackAsOneMember() throws Exception {
    for (final int size : new int[] { 0, 1, SMALL_CHUNK - 1, SMALL_CHUNK, SMALL_CHUNK + 1, 5 * SMALL_CHUNK + 17, 64 * SMALL_CHUNK })
      for (final int level : new int[] { 0, 1, 6, 9 })
        for (final int threads : new int[] { 1, 3 }) {
          final byte[] input = text(size, size);
          final byte[] gzip = parallelGzip(input, level, threads, SMALL_CHUNK);
          final String as = "size %d, level %d, %d thread(s)".formatted(size, level, threads);

          assertThat(gunzip(gzip)).as(as).isEqualTo(input);
          assertSingleMember(gzip, input, as);
        }
  }

  @Test
  void writingOneByteAtATimeWritesTheSameFileAsBulkWrites() throws Exception {
    final byte[] input = text(7 * SMALL_CHUNK + 5, 1);

    final ByteArrayOutputStream single = new ByteArrayOutputStream();
    try (final OutputStream out = new ParallelGzipOutputStream(single, 6, 2, SMALL_CHUNK)) {
      for (final byte b : input)
        out.write(b);
    }

    final ByteArrayOutputStream mixed = new ByteArrayOutputStream();
    try (final OutputStream out = new ParallelGzipOutputStream(mixed, 6, 2, SMALL_CHUNK)) {
      // WRITES THAT STRADDLE CHUNK BOUNDARIES, ONE LARGER THAN A CHUNK
      out.write(input, 0, 100);
      out.write(input, 100, 3 * SMALL_CHUNK);
      out.write(input[100 + 3 * SMALL_CHUNK]);
      out.write(input, 101 + 3 * SMALL_CHUNK, input.length - 101 - 3 * SMALL_CHUNK);
    }

    assertThat(single.toByteArray()).isEqualTo(mixed.toByteArray());
    assertThat(gunzip(single.toByteArray())).isEqualTo(input);
  }

  @Test
  void theHeaderIsTheOneGzipOutputStreamWrites() throws Exception {
    final byte[] input = text(10_000, 3);
    final byte[] legacy = legacyGzip(input, 6);
    final byte[] parallel = parallelGzip(input, 6, 2, SMALL_CHUNK);

    assertThat(Arrays.copyOf(parallel, 10)).isEqualTo(Arrays.copyOf(legacy, 10));
  }

  /**
   * The cost of the parallelism is that each chunk starts with an empty dictionary. At the default 1 MB chunk it has to stay
   * within a fraction of a percent of the single-threaded stream at the same level: the export's default level keeps the size
   * an export always had.
   */
  @Test
  void theRatioAtTheDefaultChunkSizeIsTheSingleThreadedOne() throws Exception {
    final byte[] input = text(8 * ParallelDeflater.DEFAULT_CHUNK_SIZE, 4);

    final int legacy = legacyGzip(input, 6).length;
    final byte[] parallel = parallelGzip(input, 6, 4, ParallelDeflater.DEFAULT_CHUNK_SIZE);

    assertThat(gunzip(parallel)).isEqualTo(input);
    assertThat((double) parallel.length).isLessThan(legacy * 1.01);
  }

  @Test
  void closeIsIdempotentAndAWriteAfterItIsRefused() throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final OutputStream out = new ParallelGzipOutputStream(bytes, 6, 2, SMALL_CHUNK);
    out.write(text(3000, 5));
    out.close();
    final byte[] closed = bytes.toByteArray();
    out.close();

    assertThat(bytes.toByteArray()).isEqualTo(closed);
    assertThatThrownBy(() -> out.write(1)).isInstanceOf(IOException.class).hasMessageContaining("closed");
    assertThatThrownBy(() -> out.write(new byte[10])).isInstanceOf(IOException.class).hasMessageContaining("closed");
  }

  /**
   * A write that failed leaves the stream missing data. Closing it must release the pool and close the target, and must not
   * terminate the deflate stream and write a trailer, which would make an incomplete file look like a complete one.
   */
  @Test
  void aFailedWriteLeavesNoTerminatedStreamAndNoThreadBehind() throws Exception {
    final ByteArrayOutputStream written = new ByteArrayOutputStream();
    final boolean[] closed = { false };
    final OutputStream failing = new OutputStream() {
      @Override
      public void write(final int b) throws IOException {
        write(new byte[] { (byte) b }, 0, 1);
      }

      @Override
      public void write(final byte[] b, final int off, final int len) throws IOException {
        if (written.size() > 2000)
          throw new IOException("disk full");
        written.write(b, off, len);
      }

      @Override
      public void close() {
        closed[0] = true;
      }
    };

    final OutputStream out = new ParallelGzipOutputStream(failing, 0, 2, SMALL_CHUNK);
    assertThatThrownBy(() -> {
      final byte[] input = text(64 * SMALL_CHUNK, 6);
      out.write(input);
    }).isInstanceOf(IOException.class).hasMessageContaining("disk full");
    assertThatThrownBy(() -> out.write(1)).isInstanceOf(IOException.class).hasMessageContaining("previous write");

    final int beforeClose = written.size();
    out.close();

    assertThat(closed[0]).isTrue();
    assertThat(written.size()).as("nothing written by the close of a failed stream").isEqualTo(beforeClose);
    assertThatThrownBy(() -> gunzip(written.toByteArray())).isInstanceOf(EOFException.class);
    assertThat(Thread.getAllStackTraces().keySet()).noneMatch(t -> t.getName().startsWith("arcadedb-gzip-compressor") && t.isAlive());
  }

  /**
   * {@code flush()} writes the chunks whose compression is complete (without waiting for the others), and leaves the chunk
   * being filled where it is. The stream still reads back whole after it.
   */
  @Test
  @Timeout(60)
  void flushWritesTheCompletedChunksAndKeepsTheOneBeingFilled() throws Exception {
    final byte[] input = text(SMALL_CHUNK + 100, 8);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final OutputStream out = new ParallelGzipOutputStream(bytes, 6, 2, SMALL_CHUNK)) {
      out.write(input);
      // ONE FULL CHUNK IS SUBMITTED, FAR BELOW THE IN-FLIGHT BOUND, SO NOTHING HAS DRAINED IT: ONLY flush() WRITES IT. THE
      // LOOP WAITS FOR THE WORKER, THE @Timeout IS ITS HANG DETECTOR
      while (bytes.size() <= 10) {
        out.flush();
        Thread.onSpinWait();
      }
      final int flushed = bytes.size();
      out.flush();
      assertThat(bytes.size()).as("the partial chunk is not forced out").isEqualTo(flushed);
    }
    assertThat(gunzip(bytes.toByteArray())).isEqualTo(input);
  }

  /** A failed flush leaves the stream missing what it could not write: closing it must not terminate it. */
  @Test
  void aFailedFlushLeavesNoTerminatedStream() throws Exception {
    final ByteArrayOutputStream written = new ByteArrayOutputStream();
    final OutputStream failingFlush = new OutputStream() {
      @Override
      public void write(final int b) {
        written.write(b);
      }

      @Override
      public void write(final byte[] b, final int off, final int len) {
        written.write(b, off, len);
      }

      @Override
      public void flush() throws IOException {
        throw new IOException("flush failed");
      }
    };

    final OutputStream out = new ParallelGzipOutputStream(failingFlush, 6, 2, SMALL_CHUNK);
    out.write(text(3000, 9));
    assertThatThrownBy(out::flush).isInstanceOf(IOException.class).hasMessageContaining("flush failed");
    assertThatThrownBy(() -> out.write(1)).isInstanceOf(IOException.class).hasMessageContaining("previous write");

    final int beforeClose = written.size();
    out.close();
    assertThat(written.size()).as("nothing written by the close of a failed stream").isEqualTo(beforeClose);
  }

  @Test
  void openPicksTheStreamByThreadCountAndHonoursTheLevel() throws Exception {
    final byte[] input = text(200_000, 7);

    final ByteArrayOutputStream stored = new ByteArrayOutputStream();
    try (final OutputStream out = ParallelGzipOutputStream.open(stored, 0, 0)) {
      assertThat(out).isInstanceOf(GZIPOutputStream.class);
      out.write(input);
    }
    final ByteArrayOutputStream smallest = new ByteArrayOutputStream();
    try (final OutputStream out = ParallelGzipOutputStream.open(smallest, 9, 0)) {
      out.write(input);
    }
    final ByteArrayOutputStream parallel = new ByteArrayOutputStream();
    try (final OutputStream out = ParallelGzipOutputStream.open(parallel, 0, 2)) {
      assertThat(out).isInstanceOf(ParallelGzipOutputStream.class);
      out.write(input);
    }

    assertThat(gunzip(stored.toByteArray())).isEqualTo(input);
    assertThat(gunzip(smallest.toByteArray())).isEqualTo(input);
    assertThat(gunzip(parallel.toByteArray())).isEqualTo(input);
    // LEVEL 0 STORES: THE FILE IS AT LEAST THE INPUT, LEVEL 9 A FRACTION OF IT
    assertThat(stored.size()).isGreaterThan(input.length);
    assertThat(parallel.size()).isGreaterThan(input.length);
    assertThat(smallest.size()).isLessThan(input.length / 2);

    assertThatThrownBy(() -> ParallelGzipOutputStream.open(OutputStream.nullOutputStream(), 10, 1)).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> ParallelGzipOutputStream.open(OutputStream.nullOutputStream(), -1, 1)).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> ParallelGzipOutputStream.open(OutputStream.nullOutputStream(), 6, -1)).isInstanceOf(
        IllegalArgumentException.class);
  }

  /**
   * Inflates {@code gzip} as ONE member: past the 10-byte header a single raw deflate stream must end, and exactly the 8-byte
   * trailer must follow it, carrying the CRC-32 and the size of the whole input. A multi-member file fails here: bytes would
   * follow the first member's trailer.
   */
  private static void assertSingleMember(final byte[] gzip, final byte[] input, final String as) throws DataFormatException {
    final Inflater inflater = new Inflater(true);
    try {
      inflater.setInput(gzip, 10, gzip.length - 10);
      final ByteArrayOutputStream inflated = new ByteArrayOutputStream();
      final byte[] buffer = new byte[8192];
      while (!inflater.finished()) {
        final int n = inflater.inflate(buffer);
        assertThat(n > 0 || !inflater.needsInput()).as(as + ": truncated deflate stream").isTrue();
        inflated.write(buffer, 0, n);
      }
      assertThat(inflated.toByteArray()).as(as).isEqualTo(input);
      assertThat(inflater.getRemaining()).as(as + ": bytes after the deflate stream").isEqualTo(8);

      final int trailer = gzip.length - 8;
      final CRC32 crc = new CRC32();
      crc.update(input);
      assertThat(readIntLE(gzip, trailer)).as(as + ": CRC-32").isEqualTo(crc.getValue());
      assertThat(readIntLE(gzip, trailer + 4)).as(as + ": ISIZE").isEqualTo(input.length & 0xFFFFFFFFL);
    } finally {
      inflater.end();
    }
  }

  private static long readIntLE(final byte[] b, final int offset) {
    return (b[offset] & 0xFFL) | (b[offset + 1] & 0xFFL) << 8 | (b[offset + 2] & 0xFFL) << 16 | (b[offset + 3] & 0xFFL) << 24;
  }

  private static byte[] parallelGzip(final byte[] input, final int level, final int threads, final int chunkSize)
      throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final OutputStream out = new ParallelGzipOutputStream(bytes, level, threads, chunkSize)) {
      out.write(input);
    }
    return bytes.toByteArray();
  }

  private static byte[] legacyGzip(final byte[] input, final int level) throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final OutputStream out = ParallelGzipOutputStream.open(bytes, level, 0)) {
      out.write(input);
    }
    return bytes.toByteArray();
  }

  private static byte[] gunzip(final byte[] gzip) throws IOException {
    try (final InputStream in = new GZIPInputStream(new ByteArrayInputStream(gzip))) {
      return in.readAllBytes();
    }
  }

  /** JSON-like lines with random numbers: compressible like an export, not trivially. */
  private static byte[] text(final int size, final long seed) {
    final Random random = new Random(seed);
    final StringBuilder builder = new StringBuilder(size + 64);
    while (builder.length() < size)
      builder.append("{\"t\":\"r\",\"c\":\"Item\",\"p\":{\"id\":").append(random.nextInt(1_000_000)).append(",\"v\":")
          .append(random.nextFloat()).append("}}\n");
    return builder.substring(0, size).getBytes(StandardCharsets.US_ASCII);
  }
}
