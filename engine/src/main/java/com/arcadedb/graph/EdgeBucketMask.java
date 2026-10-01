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

import java.util.List;

/**
 * The set of edge buckets a type-filtered edge-list walk accepts, as a primitive mask over the bucket-id range the
 * requested types span (issue #8417).
 * <p>
 * The mask is offset by the lowest bucket id instead of being indexed from zero, so its size follows the buckets of the
 * requested types and not the highest bucket id of the whole schema: a filtered walk builds one per hop (one per stripe
 * on a super-node), and a schema with thousands of buckets must not turn every hop into a multi-kilobyte allocation.
 */
public final class EdgeBucketMask {
  private final int       firstBucketId;
  private final boolean[] mask;

  private EdgeBucketMask(final int firstBucketId, final boolean[] mask) {
    this.firstBucketId = firstBucketId;
    this.mask = mask;
  }

  /**
   * Builds the mask of the buckets of the given edge types, subtypes included. Names that do not resolve to an edge
   * type are skipped. Returns {@code null} when nothing is left to match.
   */
  public static EdgeBucketMask of(final DatabaseInternal database, final String[] edgeTypes) {
    int min = Integer.MAX_VALUE;
    int max = -1;
    for (final String e : edgeTypes) {
      final List<Integer> bucketIds = bucketIdsOf(database, e);
      if (bucketIds != null)
        for (final Integer bucketId : bucketIds) {
          if (bucketId < min)
            min = bucketId;
          if (bucketId > max)
            max = bucketId;
        }
    }

    if (max < 0 || min < 0)
      return null;

    final boolean[] mask = new boolean[max - min + 1];
    for (final String e : edgeTypes) {
      final List<Integer> bucketIds = bucketIdsOf(database, e);
      if (bucketIds != null)
        for (final Integer bucketId : bucketIds)
          mask[bucketId - min] = true;
    }
    return new EdgeBucketMask(min, mask);
  }

  /** True if an entry whose edge lives in this bucket belongs to one of the requested types. */
  public boolean matches(final long bucketId) {
    final long index = bucketId - firstBucketId;
    return index >= 0 && index < mask.length && mask[(int) index];
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
