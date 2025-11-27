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

import com.arcadedb.database.DatabaseInternal;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;

import java.io.IOException;

/**
 * Implements JVector's ReaderSupplier interface to provide RandomAccessReader instances
 * for lazy-loading graph topology from ArcadeDB pages.
 *
 * Thread-safe: Creates a new reader per thread (JVector requirement).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadePageReaderSupplier implements ReaderSupplier {
  private final DatabaseInternal database;
  private final int              fileId;
  private final int              pageSize;
  private final long             totalBytes;

  public ArcadePageReaderSupplier(final DatabaseInternal database, final int fileId,
                                  final int pageSize, final long totalBytes) {
    this.database = database;
    this.fileId = fileId;
    this.pageSize = pageSize;
    this.totalBytes = totalBytes;
  }

  @Override
  public RandomAccessReader get() throws IOException {
    // Create a new reader for each request (JVector uses readers per-thread)
    return new ArcadePageGraphReader(database, fileId, pageSize, totalBytes);
  }

  @Override
  public void close() {
    // No-op: pages are managed by PageManager, readers are lightweight
  }
}
