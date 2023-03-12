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

/**
 * Low level immutable (read-only) page implementation of 65536 bytes (2 exp 16 = 65Kb). The first 8 bytes (the header) are reserved
 * to store the page version (MVCC). The maximum content is 65528.
 * <p>
 * Immutable pages are shared among threads and can be cached by the Page Manager with a LRU mechanism. In order to modify the page use the {@link #modify()}
 * to return a {@link MutablePage} (not shared and local to the current thread).
 * <br>
 * NOTE: This class is not thread safe and must be not used by multiple threads at the same time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ImmutablePage extends BasePage {
  public ImmutablePage(final PageManager manager, final PageId pageId, final int size, final byte[] content, final int version, final int contentSize) {
    super(manager, pageId, size, content, version, contentSize);
  }

  /**
   * Creates a copy of the ByteBuffer without copying the array[].
   *
   * @param index The starting position to copy
   */
  @Override
  public Binary getImmutableView(final int index, final int length) {
    return content.slice(index + PAGE_HEADER_SIZE, length);
  }
}
