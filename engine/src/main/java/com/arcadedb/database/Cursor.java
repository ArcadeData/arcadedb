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
package com.arcadedb.database;

import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.Iterator;

/**
 * Cursor to browse a result set.
 * <p>
 * <b>Closing (#5662).</b> A cursor is {@link AutoCloseable} so that an abandoned one is a static-analysis finding
 * rather than something only a careful read can spot. Closing matters: a scan over an LSM index holds one underlying
 * cursor per compacted series, each registered with its file, and
 * {@code LSMTreeIndex.dropRetiredCompactedIndexes} will not physically drop a file that still has one. Draining a
 * cursor releases those registrations as each series is exhausted, so only a cursor abandoned <i>partway</i> leaks -
 * which is exactly the shape a {@code LIMIT}, an {@code exists()} or an early {@code break} produces. Prefer
 * try-with-resources; {@link #close()} is idempotent on every implementation.
 * <p>
 * {@code close()} is declared without a checked exception (unlike {@link AutoCloseable#close()}) so that a
 * try-with-resources over a cursor does not force the caller into a {@code catch (Exception)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface Cursor extends Iterable<Identifiable>, Iterator<Identifiable>, AutoCloseable {
  long estimateSize();

  /**
   * Releases whatever the cursor holds. The default is a no-op for the implementations backed by an in-memory
   * collection, which hold nothing - so an implementation that DOES hold something must remember to override it. The
   * weak-reference net in {@code LSMTreeIndexCompacted} covers only the compacted-series cursors; nothing catches a
   * new cursor type that acquires a resource and inherits this default.
   */
  @Override
  default void close() {
    // NO ACTIONS
  }
}
