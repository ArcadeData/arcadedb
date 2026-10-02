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
package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.EdgeType;

import java.util.Arrays;
import java.util.List;

/**
 * The set of edge buckets a type-filtered edge-list walk accepts (issue #8417). A filtered walk builds one per hop (one
 * per stripe on a super-node) and consults it once per entry of the list, so it has to be cheap on both counts.
 * <p>
 * Two shapes, picked by the span between the lowest and highest requested bucket id:
 * <ul>
 * <li>a span up to {@link #MAX_DENSE_SPAN}: a {@code boolean[]} over that span, offset by the lowest id - one array
 * read per entry. This is the common case: a type's own buckets are created together and get adjacent ids.</li>
 * <li>a wider span: the sorted bucket ids, binary-searched. Bucket ids are never reused, so a polymorphic type whose
 * subtype was created much later can span thousands of unrelated buckets, and a dense mask over that would cost a
 * multi-kilobyte allocation on every hop for a handful of buckets.</li>
 * </ul>
 */
public final class EdgeBucketMask {
  /** Widest span, in bucket ids, still served by the dense mask: at most this many bytes allocated per hop. */
  static final int MAX_DENSE_SPAN = 1024;

  private final int       firstBucketId;
  private final int       lastBucketId;
  private final boolean[] dense;
  private final int[]     sorted;

  private EdgeBucketMask(final int firstBucketId, final int lastBucketId, final boolean[] dense, final int[] sorted) {
    this.firstBucketId = firstBucketId;
    this.lastBucketId = lastBucketId;
    this.dense = dense;
    this.sorted = sorted;
  }

  /**
   * Builds the mask of the buckets of the given edge types, subtypes included. Names that do not resolve to an edge
   * type are skipped. Returns {@code null} when nothing is left to match.
   */
  public static EdgeBucketMask of(final DatabaseInternal database, final String[] edgeTypes) {
    // ONE SCHEMA LOOKUP PER TYPE: THE (CACHED) BUCKET LISTS ARE KEPT FROM THE FIRST PASS AND UNBOXED ONCE
    final List<?>[] perType = new List<?>[edgeTypes.length];
    int total = 0;
    for (int t = 0; t < edgeTypes.length; t++) {
      final List<Integer> bucketIds = bucketIdsOf(database, edgeTypes[t]);
      perType[t] = bucketIds;
      if (bucketIds != null)
        total += bucketIds.size();
    }

    if (total == 0)
      return null;

    final int[] ids = new int[total];
    int count = 0;
    for (final List<?> bucketIds : perType)
      if (bucketIds != null)
        for (final Object bucketId : bucketIds)
          ids[count++] = (Integer) bucketId;
    return ofBucketIds(ids);
  }

  /** Builds the mask over the given bucket ids, which it sorts in place. Returns {@code null} for none or a negative id. */
  static EdgeBucketMask ofBucketIds(final int[] ids) {
    if (ids.length == 0)
      return null;
    Arrays.sort(ids);

    final int min = ids[0];
    final int max = ids[ids.length - 1];
    if (min < 0)
      return null;

    if (max - min < MAX_DENSE_SPAN) {
      final boolean[] dense = new boolean[max - min + 1];
      for (final int id : ids)
        dense[id - min] = true;
      return new EdgeBucketMask(min, max, dense, null);
    }
    return new EdgeBucketMask(min, max, null, ids);
  }

  /**
   * True if an entry whose edge lives in this bucket belongs to one of the requested types. Takes a {@code long}
   * because that is what the segment's VLQ decoding returns: narrowing it to {@code int} before the range check would
   * let a corrupted, out-of-range number wrap into a valid bucket id.
   */
  public boolean matches(final long bucketId) {
    if (bucketId < firstBucketId || bucketId > lastBucketId)
      return false;
    if (dense != null)
      return dense[(int) (bucketId - firstBucketId)];
    return Arrays.binarySearch(sorted, (int) bucketId) >= 0;
  }

  /** True for the one-array-read shape, false for the binary-searched one. */
  boolean isDense() {
    return dense != null;
  }

  private static List<Integer> bucketIdsOf(final DatabaseInternal database, final String typeName) {
    if (!database.getSchema().existsType(typeName))
      return null;

    // A vertex or document type sharing the name of the requested edge type cannot match any edge:
    // skip it like a non-existent type instead of failing with a ClassCastException (issue #5194)
    if (!(database.getSchema().getType(typeName) instanceof EdgeType type))
      return null;

    // CACHED BY THE TYPE, NOT BUILT: CALLING IT ONCE PER PASS COSTS NOTHING
    return type.getBucketIds(true);
  }
}
