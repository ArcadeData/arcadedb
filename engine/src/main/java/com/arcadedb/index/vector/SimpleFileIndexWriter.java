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
package com.arcadedb.index.vector;

import com.arcadedb.log.LogManager;
import io.github.jbellis.jvector.disk.IndexWriter;

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;

/**
 * Simple IndexWriter implementation for writing to a regular file.
 * Used for PQ data serialization where ArcadeDB page management is not needed.
 * <p>
 * The writer opens its target with {@code TRUNCATE_EXISTING}, so it must be pointed at a file nobody reads yet: a
 * caller that replaces a live file writes a temporary one and renames it over the target (see
 * {@link LSMVectorIndexPQFile#writePQ}).
 * <p>
 * <b>Interrupts</b> (issue #9796). An interrupt that lands on a thread doing I/O on a {@link FileChannel} closes the
 * channel ({@link ClosedByInterruptException}), and the thread that streams PQ data is one the engine interrupts on
 * purpose: the graph build pool is cancelled with {@code shutdownNow()}. Like the page file (#9306), the WAL (#7768),
 * the snapshot shadow (#9444) and the sealed store (#9621), the writer reopens its channel and carries on, a bounded
 * number of times, so a close landing in the middle of a stream does not abandon it half written. The interrupt is
 * not swallowed: it is cleared while the stream is written, because a thread that still has it set would close every
 * reopened channel at once, and asserted again by {@link #close()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SimpleFileIndexWriter implements IndexWriter {
  /** Reopen-and-retry rounds of one flush or force whose channel an interrupt closed under it. */
  private static final int MAX_REOPENS = 5;

  private final Path        path;
  private final ByteBuffer  buffer;
  private       FileChannel channel;
  private       long        position;
  /** Bytes already handed to the channel: the file offset the buffer's first byte lands on. */
  private       long        flushed;
  /** An interrupt absorbed while writing, asserted again by {@link #close()}. */
  private       boolean     interrupted;

  /**
   * Create a writer for the given file path.
   * The file will be created or truncated if it exists.
   */
  public SimpleFileIndexWriter(final Path path) throws IOException {
    this.path = path;
    this.channel = FileChannel.open(path,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING);
    // 8KB buffer for efficient writing
    this.buffer = ByteBuffer.allocate(8192);
    this.position = 0;
  }

  /**
   * Flushes the buffered bytes and forces them, and the file's metadata, to the storage device. A caller that renames
   * the file over a live one calls this first: the rename is atomic, but without it the new name can survive a crash
   * that the new content does not.
   */
  public void force() throws IOException {
    flush();
    for (int reopens = 0; ; ) {
      try {
        channel.force(true);
        return;
      } catch (final ClosedByInterruptException e) {
        if (++reopens > MAX_REOPENS)
          throw e;
        reopenAfterInterrupt();
      }
    }
  }

  @Override
  public long position() throws IOException {
    return position;
  }

  @Override
  public void write(final int b) throws IOException {
    ensureCapacity(1);
    buffer.put((byte) b);
    position++;
    flushIfNeeded();
  }

  @Override
  public void write(final byte[] bytes) throws IOException {
    write(bytes, 0, bytes.length);
  }

  @Override
  public void write(final byte[] bytes, final int off, final int len) throws IOException {
    int remaining = len;
    int offset = off;
    while (remaining > 0) {
      final int toWrite = Math.min(remaining, buffer.remaining());
      buffer.put(bytes, offset, toWrite);
      position += toWrite;
      offset += toWrite;
      remaining -= toWrite;
      flushIfNeeded();
    }
  }

  @Override
  public void writeBoolean(final boolean v) throws IOException {
    write(v ? 1 : 0);
  }

  @Override
  public void writeByte(final int v) throws IOException {
    write(v);
  }

  @Override
  public void writeShort(final int v) throws IOException {
    ensureCapacity(2);
    buffer.putShort((short) v);
    position += 2;
    flushIfNeeded();
  }

  @Override
  public void writeChar(final int v) throws IOException {
    writeShort(v);
  }

  @Override
  public void writeInt(final int v) throws IOException {
    ensureCapacity(4);
    buffer.putInt(v);
    position += 4;
    flushIfNeeded();
  }

  @Override
  public void writeLong(final long v) throws IOException {
    ensureCapacity(8);
    buffer.putLong(v);
    position += 8;
    flushIfNeeded();
  }

  @Override
  public void writeFloat(final float v) throws IOException {
    ensureCapacity(4);
    buffer.putFloat(v);
    position += 4;
    flushIfNeeded();
  }

  @Override
  public void writeDouble(final double v) throws IOException {
    ensureCapacity(8);
    buffer.putDouble(v);
    position += 8;
    flushIfNeeded();
  }

  @Override
  public void writeBytes(final String s) throws IOException {
    final int len = s.length();
    for (int i = 0; i < len; i++) {
      write((byte) s.charAt(i));
    }
  }

  @Override
  public void writeChars(final String s) throws IOException {
    final int len = s.length();
    for (int i = 0; i < len; i++) {
      writeChar(s.charAt(i));
    }
  }

  @Override
  public void writeUTF(final String s) throws IOException {
    // Standard DataOutputStream UTF implementation
    final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > 65535) {
      throw new UTFDataFormatException("String too long: " + bytes.length);
    }
    writeShort(bytes.length);
    write(bytes);
  }

  @Override
  public void close() throws IOException {
    try {
      flush();
    } finally {
      channel.close();
      if (interrupted) {
        // Absorbed while the stream was written; the caller is still entitled to see it.
        interrupted = false;
        Thread.currentThread().interrupt();
      }
    }
  }

  private void ensureCapacity(final int needed) throws IOException {
    if (buffer.remaining() < needed) {
      flush();
    }
  }

  private void flushIfNeeded() throws IOException {
    if (buffer.remaining() == 0) {
      flush();
    }
  }

  private void flush() throws IOException {
    if (buffer.position() == 0)
      return;

    buffer.flip();
    // Writes are strictly sequential and the file starts empty, so what the file holds past this offset is exactly the
    // part of this buffer a closed channel got through, which is what lets a retry resume from the file's size
    final long base = flushed;
    for (int reopens = 0; ; ) {
      try {
        // Positional, so a retry on a reopened channel never depends on where the closed one left its own position
        while (buffer.hasRemaining())
          channel.write(buffer, base + buffer.position());
        break;
      } catch (final ClosedByInterruptException e) {
        if (++reopens > MAX_REOPENS)
          throw e;
        reopenAfterInterrupt();
        // The interrupted write may have landed some of its bytes before the channel noticed: the file says how many
        buffer.position((int) Math.min(buffer.limit(), Math.max(0L, channel.size() - base)));
      }
    }
    flushed = base + buffer.limit();
    buffer.clear();
  }

  /**
   * Replaces the channel an interrupt closed. Never creates or truncates: the file is the one this writer made.
   */
  private void reopenAfterInterrupt() throws IOException {
    // The flag stays set after ClosedByInterruptException, which would close the reopened channel on its first I/O
    interrupted |= Thread.interrupted();
    LogManager.instance().log(this, Level.FINE, "File '%s' was closed (thread interrupted?). Reopen it and retry...", null, path);
    channel = FileChannel.open(path, StandardOpenOption.WRITE);
  }
}
