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
package com.arcadedb.index;

import com.arcadedb.database.RID;

/**
 * Test seam shared by every path that indexes the records of a bucket by scanning them: the record-at-a-time builds of the LSM,
 * hash, full-text and geospatial indexes, the sorted build of a type index and the in-place rebuild. A test sets
 * {@link #onRecordIndexed} to pause a build at the same logical point whichever path the build takes, the drop-and-create one or
 * the in-place one, and to compare what the database does meanwhile.
 * <p>
 * The field is read once per record and is null outside tests, so the cost on a build is one volatile read per record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IndexBuildTestHooks {
  /** Called by a build after the entries of one record were produced. */
  @FunctionalInterface
  public interface RecordIndexedHook {
    /**
     * @param index the index being built: a bucket index, or the type index of a sorted build
     * @param rid   the record just indexed
     */
    void onRecordIndexed(Index index, RID rid);
  }

  /** Null outside tests. A test sets it and MUST reset it to null in a finally block. */
  public static volatile RecordIndexedHook onRecordIndexed;

  private IndexBuildTestHooks() {
  }

  /** Runs the hook if one is set. */
  public static void recordIndexed(final Index index, final RID rid) {
    final RecordIndexedHook hook = onRecordIndexed;
    if (hook != null)
      hook.onRecordIndexed(index, rid);
  }
}
