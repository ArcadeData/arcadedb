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

import com.arcadedb.database.Binary;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * The one view of a {@link CachedPage} that every thread shares ({@link CachedPage#getSharedImmutableView()}, #10654), for a
 * reader that reads the page at absolute positions: {@code readInt}, {@code readLong}, {@code readShort}, {@code readByte},
 * a variable-length number ({@code readNumber}, {@code readNumberSize}, {@code readNumberAndSize}, #10930), {@link #slice()},
 * {@link #getImmutableView} and the page metadata never touch the position of the buffer nor keep any state on the page, so
 * many threads can use them on this one instance at once, and a lookup no longer builds an {@link ImmutablePage}, a
 * {@link Binary} and a {@link ByteBuffer} per page it reads.
 * <p>
 * A fixed-length byte array ({@code readByteArray}) is copied straight out of the backing array (#10820). The reads of
 * {@link BasePage} that DO move the position (a length-prefixed byte array) are answered here on a private buffer
 * built for the call, so a reader that reaches one still gets its own position, at the price of the allocation the shared view
 * otherwise saves. A position kept across calls cannot be offered by a shared instance at all:
 * {@link #getBufferPosition()} and {@link #setBufferPosition(int)} throw, and {@link #getContent()} hands out a buffer of the
 * caller's own. The metadata is the cached page's and is not reloaded under the readers ({@link #loadMetadata()} throws).
 * <p>
 * Every public method of {@link BasePage} and {@link ImmutablePage} is either position-free or overridden here, and
 * {@code SharedPageViewTest.SharedPageViewVarInt} holds that: a new public page method that is neither overridden here nor listed
 * there as position-free, or a new field a read could keep a result in for a later call, fails it until this class answers it.
 */
final class SharedImmutablePage extends ImmutablePage {
  SharedImmutablePage(final PageId pageId, final int size, final byte[] content, final int version, final int contentSize) {
    super(pageId, size, content, version, contentSize);
  }

  /** A buffer over the same bytes, with a position of the caller's own. */
  private Binary privateContent() {
    return new Binary(content.getContent(), content.size());
  }

  /**
   * Copied straight out of the backing array, which every view wraps whole: no private buffer per call, so a hash index entry
   * read through the shared view allocates only the array it asked for (issue #10820).
   */
  @Override
  public void readByteArray(final int index, final byte[] buffer) {
    readByteArray(index, buffer, 0, buffer.length);
  }

  @Override
  public void readByteArray(final int index, final byte[] buffer, final int offset, final int length) {
    final byte[] bytes = content.getContent();
    final int from = PAGE_HEADER_SIZE + index;
    // THE BOUNDS A ByteBuffer.position() + get() ENFORCED: THE WHOLE ARRAY, NOT THE CONTENT SIZE
    if (from < 0 || from > bytes.length)
      throw new IllegalArgumentException("Position " + from + " is outside the page of " + bytes.length + " bytes");
    if (length > bytes.length - from)
      throw new BufferUnderflowException();
    System.arraycopy(bytes, from, buffer, offset, length);
  }

  @Override
  public byte[] readBytes(final int index) {
    return privateContent().getBytes(PAGE_HEADER_SIZE + index);
  }

  @Override
  public byte[] readBytes() {
    // a per-call view starts at position 0, and so does this
    return privateContent().getBytes();
  }

  @Override
  public ByteBuffer getContent() {
    return content.getByteBuffer().duplicate();
  }

  @Override
  public int getBufferPosition() {
    throw new UnsupportedOperationException("A shared page view has no position of its own: read it at absolute positions");
  }

  @Override
  public void setBufferPosition(final int newPos) {
    throw new UnsupportedOperationException("A shared page view has no position of its own: read it at absolute positions");
  }

  @Override
  public void loadMetadata() {
    throw new UnsupportedOperationException("A shared page view carries the metadata of its cached page");
  }
}
