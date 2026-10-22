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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An append that fails after writing part of its record must not leave the torn prefix in the WAL (#10509 review): the
 * record is written again (a group commit re-queues it for the next flush pass), and a complete record behind a torn one is
 * what recovery reads as a corruption in the middle of the WAL.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WALFileTornAppendTest extends TestHelper {

  @Test
  void aFailedAppendLeavesNoTornRecordBehind() throws Exception {
    final File path = new File(database.getDatabasePath(), "txlog_torn_append.wal");
    final WALFile walFile = new WALFile(path.getAbsolutePath());
    final Field channelField = WALFile.class.getDeclaredField("channel");
    channelField.setAccessible(true);
    final FileChannel real = (FileChannel) channelField.get(walFile);
    try {
      final byte[] first = record(100, 1);
      final byte[] torn = record(200, 2);
      final byte[] next = record(50, 3);

      walFile.append(ByteBuffer.wrap(first));

      channelField.set(walFile, new TearingChannel(real, 10));
      assertThatThrownBy(() -> walFile.append(ByteBuffer.wrap(torn))).isInstanceOf(IOException.class);
      channelField.set(walFile, real);

      assertThat(walFile.getSize()).as("the 10 bytes the failed write put down must be gone").isEqualTo(first.length);

      walFile.append(ByteBuffer.wrap(next));
      final byte[] expected = new byte[first.length + next.length];
      System.arraycopy(first, 0, expected, 0, first.length);
      System.arraycopy(next, 0, expected, first.length, next.length);
      assertThat(Files.readAllBytes(path.toPath())).isEqualTo(expected);
    } finally {
      channelField.set(walFile, real);
      walFile.close();
    }
  }

  private static byte[] record(final int size, final int seed) {
    final byte[] record = new byte[size];
    for (int i = 0; i < size; ++i)
      record[i] = (byte) (i * seed % 251);
    return record;
  }

  /** Writes the first {@code tornBytes} of the first positional write, then fails the next one like a full disk. */
  private static class TearingChannel extends FileChannel {
    private final FileChannel delegate;
    private final int         tornBytes;
    private       boolean     torn;

    TearingChannel(final FileChannel delegate, final int tornBytes) {
      this.delegate = delegate;
      this.tornBytes = tornBytes;
    }

    @Override
    public int write(final ByteBuffer src, final long position) throws IOException {
      if (torn)
        throw new IOException("No space left on device");
      torn = true;
      final ByteBuffer part = src.slice();
      part.limit(Math.min(tornBytes, src.remaining()));
      final int written = delegate.write(part, position);
      src.position(src.position() + written);
      return written;
    }

    @Override
    public long size() throws IOException {
      return delegate.size();
    }

    @Override
    public FileChannel truncate(final long size) throws IOException {
      delegate.truncate(size);
      return this;
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
      return delegate.read(dst);
    }

    @Override
    public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
      return delegate.read(dsts, offset, length);
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
      return delegate.write(src);
    }

    @Override
    public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
      return delegate.write(srcs, offset, length);
    }

    @Override
    public long position() throws IOException {
      return delegate.position();
    }

    @Override
    public FileChannel position(final long newPosition) throws IOException {
      delegate.position(newPosition);
      return this;
    }

    @Override
    public void force(final boolean metaData) throws IOException {
      delegate.force(metaData);
    }

    @Override
    public long transferTo(final long position, final long count, final WritableByteChannel target) throws IOException {
      return delegate.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(final ReadableByteChannel src, final long position, final long count) throws IOException {
      return delegate.transferFrom(src, position, count);
    }

    @Override
    public int read(final ByteBuffer dst, final long position) throws IOException {
      return delegate.read(dst, position);
    }

    @Override
    public MappedByteBuffer map(final MapMode mode, final long position, final long size) throws IOException {
      return delegate.map(mode, position, size);
    }

    @Override
    public FileLock lock(final long position, final long size, final boolean shared) throws IOException {
      return delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(final long position, final long size, final boolean shared) throws IOException {
      return delegate.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() {
      // the test closes the real channel through the WALFile
    }
  }
}
