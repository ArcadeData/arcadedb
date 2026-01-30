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

import io.github.jbellis.jvector.disk.IndexWriter;

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Simple IndexWriter implementation for writing to a regular file.
 * Used for PQ data serialization where ArcadeDB page management is not needed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SimpleFileIndexWriter implements IndexWriter {
  private final FileChannel channel;
  private final ByteBuffer  buffer;
  private       long        position;

  /**
   * Create a writer for the given file path.
   * The file will be created or truncated if it exists.
   */
  public SimpleFileIndexWriter(final Path path) throws IOException {
    this.channel = FileChannel.open(path,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING);
    // 8KB buffer for efficient writing
    this.buffer = ByteBuffer.allocate(8192);
    this.position = 0;
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
    final byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    if (bytes.length > 65535) {
      throw new UTFDataFormatException("String too long: " + bytes.length);
    }
    writeShort(bytes.length);
    write(bytes);
  }

  @Override
  public void close() throws IOException {
    flush();
    channel.close();
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
    if (buffer.position() > 0) {
      buffer.flip();
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      buffer.clear();
    }
  }
}
