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

import java.nio.*;
import java.util.*;
import java.util.zip.*;

/**
 * Contains the page content to be shared across threads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CachedPage {
  private final PageManager pageManager;
  private final PageId      pageId;
  private final Binary      content;
  private final int         size;
  private       int         version;
  private       long        lastAccessed = System.currentTimeMillis();

  public CachedPage(final MutablePage page) {
    this.pageManager = page.manager;
    this.pageId = page.pageId;
    this.content = page.content;
    this.size = page.size;
    this.version = page.version;
  }

  public CachedPage(final PageManager pageManager, final PageId pageId, final int size) {
    this.pageManager = pageManager;
    this.pageId = pageId;
    this.content = new Binary(size).setAutoResizable(false);
    this.size = size;
  }

  public void loadMetadata() {
    version = content.getInt(BasePage.PAGE_VERSION_OFFSET);
    content.size(content.getInt(BasePage.PAGE_CONTENTSIZE_OFFSET));
  }

  public ImmutablePage use() {
    return new ImmutablePage(pageManager, pageId, size, content.getContent(), version, content.size());
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
    final byte[] c = content.getByteBuffer().array();
    final Checksum crc32 = new CRC32();
    crc32.update(c, 0, c.length);

    return pageId.toString() + " v=" + version + " crc=" + crc32.getValue();
  }
}
