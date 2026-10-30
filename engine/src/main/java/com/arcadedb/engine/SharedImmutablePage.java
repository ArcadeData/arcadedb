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

import java.nio.ByteBuffer;

/**
 * The one view of a {@link CachedPage} that every thread shares ({@link CachedPage#getSharedImmutableView()}, #10654), for a
 * reader that reads the page at absolute positions: {@code readInt}, {@code readLong}, {@code readShort}, {@code readByte},
 * a variable-length number ({@code readNumber}, {@code readNumberSize}, {@code readNumberAndSize}, #10930), {@link #slice()},
 * {@link #getImmutableView} and the page metadata never touch the position of the buffer nor keep any state on the page, so
 * many threads can use them on this one instance at once, and a lookup no longer builds an {@link ImmutablePage}, a
 * {@link Binary} and a {@link ByteBuffer} per page it reads.
 * <p>
 * The reads of {@link BasePage} that DO move the position (a length-prefixed byte array) are answered here on a private buffer
 * built for the call, so a reader that reaches one still gets its own position, at the price of the allocation the shared view
 * otherwise saves. A position kept across calls cannot be offered by a shared instance at all:
 * {@link #getBufferPosition()} and {@link #setBufferPosition(int)} throw, and {@link #getContent()} hands out a buffer of the
 * caller's own. The metadata is the cached page's and is not reloaded under the readers ({@link #loadMetadata()} throws).
 * <p>
 * Every public method of {@link BasePage} and {@link ImmutablePage} is either position-free or overridden here, and
 * {@code SharedImmutablePageCoverageTest} holds that: a new page read that moves the position, or keeps a result on the page
 * for a later call, fails it until this class answers it.
 */
final class SharedImmutablePage extends ImmutablePage {
  SharedImmutablePage(final PageId pageId, final int size, final byte[] content, final int version, final int contentSize) {
    super(pageId, size, content, version, contentSize);
  }

  /** A buffer over the same bytes, with a position of the caller's own. */
  private Binary privateContent() {
    return new Binary(content.getContent(), content.size());
  }

  @Override
  public void readByteArray(final int index, final byte[] buffer) {
    privateContent().getByteArray(PAGE_HEADER_SIZE + index, buffer);
  }

  @Override
  public void readByteArray(final int index, final byte[] buffer, final int offset, final int length) {
    privateContent().getByteArray(PAGE_HEADER_SIZE + index, buffer, offset, length);
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
