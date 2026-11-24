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

import java.util.Arrays;

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
 * are loaded as before. One instance serves one scan: what the schema says of a bucket is asked once per bucket and kept, since
 * the entries of an index that spans several buckets (a type with more than one bucket, or a super type) come interleaved, in key
 * order and not bucket after bucket.
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

  // What is known of a bucket, by its id. A bucket id is a file id, which the database hands out one after the other: the arrays
  // grow to the highest bucket the entries name, and a lookup is an array read. A bucket id past MAX_BUCKET_ID is not cached,
  // its entries are loaded: nothing a well-formed index holds
  private static final int  MAX_BUCKET_ID = 1 << 16;
  private static final byte UNKNOWN       = 0;
  private static final byte MUST_LOAD     = 1;
  private static final byte IS_ROW        = 2;
  private static final byte IS_NOT_A_ROW  = 3;

  private final DatabaseInternal database;
  private final String           typeName;
  private       byte[]           kinds   = new byte[32];
  // the bucket of the ids whose kind is IS_ROW or IS_NOT_A_ROW
  private       Bucket[]         buckets = new Bucket[32];

  /**
   * @param typeName the type the rows must be an instance of, as a filter after the scan checks, or null when every record the index
   *                 names is a row
   */
  public IndexEntrySkip(final DatabaseInternal database, final String typeName) {
    this.database = database;
    this.typeName = typeName;
  }

  public Verdict verdict(final RID rid) {
    final int bucketId = rid.getBucketId();
    if (bucketId < 0 || bucketId > MAX_BUCKET_ID)
      return Verdict.LOAD;
    if (bucketId >= kinds.length)
      grow(bucketId);
    byte kind = kinds[bucketId];
    if (kind == UNKNOWN)
      kind = resolve(bucketId);
    if (kind == MUST_LOAD || !buckets[bucketId].existsRecord(rid))
      return Verdict.LOAD;
    return kind == IS_ROW ? Verdict.ROW : Verdict.NOT_A_ROW;
  }

  private byte resolve(final int bucketId) {
    final DocumentType type = database.getSchema().getTypeByBucketId(bucketId);
    final Bucket bucket = type == null || FacetFallbackScanner.firesReadEvents(database, type) ?
        null :
        database.getSchema().getBucketByIdIfExists(bucketId);
    if (bucket == null)
      return kinds[bucketId] = MUST_LOAD;
    buckets[bucketId] = bucket;
    return kinds[bucketId] = typeName == null || type.isSubTypeOf(typeName) ? IS_ROW : IS_NOT_A_ROW;
  }

  private void grow(final int bucketId) {
    final int capacity = Math.max(bucketId + 1, kinds.length << 1);
    kinds = Arrays.copyOf(kinds, capacity);
    buckets = Arrays.copyOf(buckets, capacity);
  }
}
