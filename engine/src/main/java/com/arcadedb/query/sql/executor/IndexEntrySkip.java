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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.facet.FacetFallbackScanner;
import com.arcadedb.schema.DocumentType;

/**
 * Tells whether a SKIP can pass over the row of an index entry without loading its record (issue #11954). The row is thrown
 * away, so the load matters only for what the load itself decides:
 * <ul>
 *   <li>whether the record is there: the scans skip or reconcile an entry whose record is gone;</li>
 *   <li>whether the user may read it: the load fails on a bucket the user may not read;</li>
 *   <li>what a read listener or trigger makes of it: it can hide the record or hand back another one.</li>
 * </ul>
 * The first two are answered from the record's slot by {@link Bucket#existsRecord}, which asks the permission the load asks and
 * reads the slot through the transaction. The third is answered only by the load, so the entries of a type that fires read events
 * are loaded as before. One instance serves one scan: the answers of the last bucket are kept, the entries of an index coming
 * bucket after bucket.
 */
public final class IndexEntrySkip {
  /** What the row of an entry is to a SKIP. */
  public enum Verdict {
    /** A row: the SKIP counts it. */
    ROW,
    /** No row: the record is of a type the rows are not, and a filter after the scan drops it. */
    NOT_A_ROW,
    /** Only the load of the record tells: it is loaded and served as without the SKIP. */
    LOAD
  }

  private final DatabaseInternal database;
  private final String           typeName;
  private       int              bucketId = -1;
  // null when the entries of the bucket have to be loaded
  private       Bucket           bucket;
  private       boolean          ofType;

  /**
   * @param typeName the type the rows must be an instance of, as a filter after the scan checks, or null when every record the index
   *                 names is a row
   */
  public IndexEntrySkip(final DatabaseInternal database, final String typeName) {
    this.database = database;
    this.typeName = typeName;
  }

  public Verdict verdict(final RID rid) {
    if (rid.getBucketId() != bucketId) {
      bucketId = rid.getBucketId();
      final DocumentType type = database.getSchema().getTypeByBucketId(bucketId);
      bucket = type == null || FacetFallbackScanner.firesReadEvents(database, type) ?
          null :
          database.getSchema().getBucketByIdIfExists(bucketId);
      ofType = typeName == null || (type != null && type.isSubTypeOf(typeName));
    }
    if (bucket == null || !bucket.existsRecord(rid))
      return Verdict.LOAD;
    return ofType ? Verdict.ROW : Verdict.NOT_A_ROW;
  }
}
