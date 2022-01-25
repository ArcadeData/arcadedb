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

/**
 * Low level immutable (read-only) page implementation of 65536 bytes (2 exp 16 = 65Kb). The first 8 bytes (the header) are reserved
 * to store the page version (MVCC). The maximum content is 65528.
 */
public class ImmutablePage extends BasePage {
  public ImmutablePage(final PageManager manager, final PageId pageId, final int size) {
    this(manager, pageId, size, new byte[size], 0, 0);
  }

  public ImmutablePage(final PageManager manager, final PageId pageId, final int size, final byte[] content, final int version, final int contentSize) {
    super(manager, pageId, size, content, version, contentSize);
  }
}
