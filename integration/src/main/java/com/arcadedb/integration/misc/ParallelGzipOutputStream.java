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

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * A gzip stream compressed by several threads (issue #11539): the export's counterpart of the backup's
 * {@code ParallelZipArchiveWriter}, on the same {@link ParallelDeflater}.
 * <p>
 * {@link GZIPOutputStream} deflates on the thread that writes, and an export formats its records on that same thread, so
 * the two add up: at the default level the compression was about 90% of an {@code EXPORT DATABASE}. Here the writing
 * thread only fills chunks and updates the CRC; the chunks are deflated by a pool while it goes on formatting.
 * <p>
 * <b>The output is one gzip member</b> (RFC 1952): one header, one deflate stream made of the chunks concatenated in order
 * (pigz's construction, see {@link ParallelDeflater}), one trailer with the CRC-32 and the size of the whole input. It is
 * not a series of independently compressed members, which some readers stop after the first of, so it is read by every
 * gzip reader exactly like {@link GZIPOutputStream}'s output, and its ratio is within a fraction of a percent of it at the
 * same level.
 * <p>
 * {@link #flush()} does not force out the chunk being filled: that would end a chunk early and cost ratio on every call,
 * which is also why {@link GZIPOutputStream} does not sync-flush by default. Not thread-safe, like any {@link OutputStream}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ParallelGzipOutputStream extends OutputStream {
  /**
   * The header {@link GZIPOutputStream} writes: magic, deflate, no flags, no modification time, no extra flags, OS 255
   * ("unknown", RFC 1952).
   */
  private static final byte[] HEADER = { 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, (byte) 0xFF };
  /** {@link GZIPOutputStream}'s own deflate output buffer is 512 bytes, so each one would otherwise be a write to the file. */
  private static final int    LEGACY_BUFFER_SIZE = 64 * 1024;

  private final OutputStream     out;
  private final ParallelDeflater deflater;
  private final CRC32            crc     = new CRC32();
  private final byte[]           trailer = new byte[8];
  private       byte[]           chunk;
  private       int              position;
  private       long             totalIn;
  private       boolean          closed;
  /** A write failed: the stream is missing data, so closing it must not terminate it as if it were complete. */
  private       boolean          failed;

  public ParallelGzipOutputStream(final OutputStream out, final int compressionLevel, final int threads) throws IOException {
    this(out, compressionLevel, threads, ParallelDeflater.DEFAULT_CHUNK_SIZE);
  }

  public ParallelGzipOutputStream(final OutputStream out, final int compressionLevel, final int threads, final int chunkSize)
      throws IOException {
    this.out = out;
    this.deflater = new ParallelDeflater(compressionLevel, threads, chunkSize, "arcadedb-gzip-compressor", "the gzip stream");
    try {
      out.write(HEADER);
    } catch (final IOException | RuntimeException e) {
      deflater.close();
      throw e;
    }
  }

  /**
   * A gzip stream over {@code out} at {@code compressionLevel} (0 stored to 9 smallest): {@code threads} 0 is the JDK's
   * single-threaded {@link GZIPOutputStream}, kept as an escape hatch like the backup's {@code compressionThreads = 0}, and
   * any positive count is a {@link ParallelGzipOutputStream} with a pool of that size.
   */
  public static OutputStream open(final OutputStream out, final int compressionLevel, final int threads) throws IOException {
    if (compressionLevel < Deflater.NO_COMPRESSION || compressionLevel > Deflater.BEST_COMPRESSION)
      throw new IllegalArgumentException("Compression level must be between 0 and 9, found " + compressionLevel);
    if (threads < 0)
      throw new IllegalArgumentException("Compression threads cannot be negative, found " + threads);

    if (threads > 0)
      return new ParallelGzipOutputStream(out, compressionLevel, threads);

    return new GZIPOutputStream(out, LEGACY_BUFFER_SIZE) {
      {
        def.setLevel(compressionLevel);
      }
    };
  }

  @Override
  public void write(final int b) throws IOException {
    ensureOpen();
    if (chunk == null)
      chunk = deflater.acquireInputBuffer();
    chunk[position++] = (byte) b;
    if (position == chunk.length)
      submitChunk();
  }

  @Override
  public void write(final byte[] b, int off, int len) throws IOException {
    ensureOpen();
    if (off < 0 || len < 0 || len > b.length - off)
      throw new IndexOutOfBoundsException("Range [" + off + ", " + off + " + " + len + ") out of bounds for length " + b.length);

    while (len > 0) {
      if (chunk == null)
        chunk = deflater.acquireInputBuffer();
      final int n = Math.min(len, chunk.length - position);
      System.arraycopy(b, off, chunk, position, n);
      position += n;
      off += n;
      len -= n;
      if (position == chunk.length)
        submitChunk();
    }
  }

  /** Flushes what is already compressed to the underlying stream; the chunk being filled stays where it is (see above). */
  @Override
  public void flush() throws IOException {
    ensureOpen();
    out.flush();
  }

  /**
   * Compresses what is left, terminates the deflate stream, writes the trailer and closes the underlying stream. After a
   * failed write the stream is only released and closed: terminating it would make an incomplete file look complete to a
   * reader that does not check the CRC.
   */
  @Override
  public void close() throws IOException {
    if (closed)
      return;
    closed = true;

    try {
      if (!failed)
        finish();
    } finally {
      try {
        deflater.close();
      } finally {
        out.close();
      }
    }
  }

  private void finish() throws IOException {
    if (position > 0)
      submitChunk();
    else if (chunk != null)
      deflater.recycle(chunk);
    chunk = null;

    deflater.drainAll(out);
    out.write(totalIn == 0 ? ParallelDeflater.EMPTY_DEFLATE_STREAM : ParallelDeflater.FINAL_EMPTY_BLOCK);

    // RFC 1952: CRC-32, THEN THE INPUT SIZE MODULO 2^32, BOTH LITTLE ENDIAN
    final long crcValue = crc.getValue();
    for (int i = 0; i < 4; i++) {
      trailer[i] = (byte) (crcValue >>> (i * 8));
      trailer[4 + i] = (byte) (totalIn >>> (i * 8));
    }
    out.write(trailer);
    out.flush();
  }

  private void submitChunk() throws IOException {
    final byte[] full = chunk;
    final int length = position;
    chunk = null;
    position = 0;

    crc.update(full, 0, length);
    totalIn += length;
    try {
      deflater.submit(full, length, out);
    } catch (final IOException | RuntimeException | Error e) {
      failed = true;
      throw e;
    }
  }

  private void ensureOpen() throws IOException {
    if (closed)
      throw new IOException("Stream closed");
    if (failed)
      throw new IOException("A previous write to the gzip stream failed");
  }
}
