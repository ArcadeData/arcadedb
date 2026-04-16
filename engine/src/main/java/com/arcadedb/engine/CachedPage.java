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
import com.arcadedb.log.LogManager;

import java.nio.*;
import java.util.*;
import java.util.logging.*;

/**
 * Contains the page content to be shared across threads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CachedPage {
  private final PageId pageId;
  private final Binary content;
  private final int    size;
  private       int    version;
  private       long   lastAccessed = System.currentTimeMillis();

  public CachedPage(final MutablePage page, final boolean copyBuffer) {
    this.pageId = page.pageId;
    if (copyBuffer) {
      // Deep copy: duplicate the full backing array so the cached copy is completely independent
      // from the original MutablePage. Binary.copy() only creates a new ByteBuffer view over the
      // SAME array, which is not safe when the original page is still reachable (e.g. from the
      // async flush thread's queue).
      final byte[] srcArray = page.content.getContent();
      final byte[] copied = java.util.Arrays.copyOf(srcArray, srcArray.length);
      this.content = new Binary(copied, page.content.size());
    } else {
      this.content = page.content;
    }
    this.size = page.size;
    this.version = page.version;
  }

  public CachedPage(final PageManager pageManager, final PageId pageId, final int size) {
    this.pageId = pageId;
    this.content = new Binary(size).setAutoResizable(false);
    this.size = size;
  }

  public void loadMetadata() {
    version = content.getInt(BasePage.PAGE_VERSION_OFFSET);
    int contentSize = content.getInt(BasePage.PAGE_CONTENTSIZE_OFFSET);
    if (contentSize < 0 || contentSize > size) {
      // Corrupted page metadata - clamp to physical page size and log a warning
      LogManager.instance().log(this, Level.WARNING,
          "Page %s has invalid content size %d (physical size %d), clamping to physical size", pageId, contentSize, size);
      contentSize = size;
    }
    content.size(contentSize);
  }

  public ImmutablePage useAsImmutable() {
    return new ImmutablePage(pageId, size, content.getContent(), version, content.size());
  }

  public MutablePage useAsMutable() {
    final byte[] array = this.content.getByteBuffer().array();
    // COPY THE CONTENT, SO CHANGES DOES NOT AFFECT IMMUTABLE COPY
    return new MutablePage(pageId, size, Arrays.copyOf(array, array.length), version, content.size());
  }

  public long getLastAccessed() {
    return lastAccessed;
  }

  public void updateLastAccesses() {
    lastAccessed = System.currentTimeMillis();
  }

  public PageId getPageId() {
    return pageId;
  }

  public long getPhysicalSize() {
    return size;
  }

  public ByteBuffer getByteBuffer() {
    return content.getByteBuffer();
  }

  public int getVersion() {
    return version;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final CachedPage other = (CachedPage) o;

    if (!Objects.equals(pageId, other.pageId))
      return false;

    return version == other.version;
  }

  @Override
  public int hashCode() {
    return pageId.hashCode();
  }

  @Override
  public String toString() {
//    final byte[] c = content.getByteBuffer().array();
//    final Checksum crc32 = new CRC32();
//    crc32.update(c, 0, c.length);
//    return pageId.toString() + " v=" + version + " crc=" + crc32.getValue() + " records=" + content.getShort(8);
    return pageId.toString() + " v=" + version;
  }
}
