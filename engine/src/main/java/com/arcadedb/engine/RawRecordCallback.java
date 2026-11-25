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
import com.arcadedb.database.RID;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

@ExcludeFromJacocoGeneratedReport
public interface RawRecordCallback {
  boolean onRecord(RID rid, Binary view);

  /**
   * {@link #onRecord(RID, Binary)} for a scan that knows whether the view was read from the transaction's own image of its page
   * (#12320): under {@code READ_COMMITTED} that image misses what others committed to the page after the transaction's first
   * write to it, so a vertex built from it carries edge-list heads a concurrent transaction may have moved.
   */
  default boolean onRecord(final RID rid, final Binary view, final boolean fromOwnImage) {
    return onRecord(rid, view);
  }

  /**
   * {@link #onRecord(RID, Binary, boolean)} for a scan that also knows the transaction the view was read in (#12575): the begin
   * sequence of the transaction that read the page, {@code -1} outside a transaction. A record built from the view carries it as
   * its read marker, as one built by {@code BucketIterator} or {@code lookupByRID} does, so a delete or a property write that
   * finds a concurrent commit in between is told it is a retryable conflict. Taken once per page by the scan, not per record.
   */
  default boolean onRecord(final RID rid, final Binary view, final boolean fromOwnImage, final long readInTransaction) {
    return onRecord(rid, view, fromOwnImage);
  }
}
