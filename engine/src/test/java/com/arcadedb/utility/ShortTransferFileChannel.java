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
package com.arcadedb.utility;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A real {@link FileChannel} that never transfers more than {@code maxBytesPerCall} bytes in one read or write -
 * which the {@link FileChannel} contract allows and a local-disk channel essentially never does, so code that takes
 * one call's return value on faith only shows its bug against a channel like this one (issue #9640). Everything
 * else is delegated untouched, so the bytes still land in, and come from, the real file.
 */
public class ShortTransferFileChannel extends FileChannel {
  private final FileChannel   delegate;
  private final int           maxBytesPerCall;
  /** Calls that transferred fewer bytes than the caller asked for: proof the short path was exercised. */
  public final  AtomicInteger shortTransfers = new AtomicInteger();

  public ShortTransferFileChannel(final FileChannel delegate, final int maxBytesPerCall) {
    this.delegate = delegate;
    this.maxBytesPerCall = maxBytesPerCall;
  }

  private interface Transfer {
    int run(ByteBuffer capped) throws IOException;
  }

  private int capped(final ByteBuffer buffer, final Transfer transfer) throws IOException {
    final int limit = buffer.limit();
    final int allowed = Math.min(maxBytesPerCall, buffer.remaining());
    if (allowed < buffer.remaining())
      shortTransfers.incrementAndGet();
    buffer.limit(buffer.position() + allowed);
    try {
      return transfer.run(buffer);
    } finally {
      buffer.limit(limit);
    }
  }

  @Override
  public int read(final ByteBuffer dst) throws IOException {
    return capped(dst, delegate::read);
  }

  @Override
  public int read(final ByteBuffer dst, final long position) throws IOException {
    return capped(dst, b -> delegate.read(b, position));
  }

  @Override
  public int write(final ByteBuffer src) throws IOException {
    return capped(src, delegate::write);
  }

  @Override
  public int write(final ByteBuffer src, final long position) throws IOException {
    return capped(src, b -> delegate.write(b, position));
  }

  @Override
  public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
    return delegate.read(dsts, offset, length);
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
  public long size() throws IOException {
    return delegate.size();
  }

  @Override
  public FileChannel truncate(final long size) throws IOException {
    delegate.truncate(size);
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
  protected void implCloseChannel() throws IOException {
    delegate.close();
  }
}
