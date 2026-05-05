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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.RID;

import java.io.IOException;

/**
 * Forward-only cursor over the postings of one dim within a single source (sealed segment or
 * memtable). Used by {@link DimCursor} to merge multiple sources transparently.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>Caller invokes {@link #start()} once.</li>
 *   <li>Caller invokes {@link #advance()} repeatedly until it returns {@code false}.</li>
 *   <li>Optional: {@link #seekTo(RID)} skips forward; the cursor lands at the first posting
 *       whose RID is &gt;= the target.</li>
 *   <li>{@link #close()} releases backing resources. Idempotent.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface SourceCursor extends AutoCloseable {

  void start() throws IOException;

  boolean advance() throws IOException;

  /** Seek forward to the first posting whose RID is &gt;= {@code target}. Backwards seeks are no-ops. */
  boolean seekTo(RID target) throws IOException;

  RID currentRid();

  float currentWeight();

  boolean isTombstone();

  boolean isExhausted();

  /** Upper bound on the contribution of any remaining posting in this source for this dim. */
  float upperBoundRemaining();

  @Override
  void close();
}
