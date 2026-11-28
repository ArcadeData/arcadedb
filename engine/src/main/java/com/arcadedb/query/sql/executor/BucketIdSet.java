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

import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.Schema;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.IntPredicate;

/**
 * The buckets a statement on a type reads, in the shape the planner hands them around: a set of bucket names (issue #12758).
 * It is held by bucket file id, which a bucket keeps for its whole life, and answers with names only when asked for them.
 * <p>
 * {@code ALTER TYPE ... NAME} moves the buckets of the type under their new names one at a time, while statements on the type
 * are planned and run. A set of the names read when planning started no longer matched the names the planner and the steps
 * read a moment later, so a bucket renamed in between was left out of the plan, and a statement on a type that holds the row
 * answered no rows. {@link #includes} decides by id, so a rename cannot take a bucket out of the set.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class BucketIdSet extends AbstractSet<String> {
  private final Schema schema;
  private final int[]  ids;

  /**
   * @param ids the file ids of the buckets, in any order, repetitions allowed
   */
  public BucketIdSet(final Schema schema, final int[] ids) {
    this.schema = schema;
    this.ids = Arrays.stream(ids).distinct().sorted().toArray();
  }

  /**
   * Whether the bucket is one of {@code buckets}, by its id when they are a {@link BucketIdSet}, otherwise by its current name.
   * {@code null} stands for every bucket, as does {@code "*"} in a set of names.
   */
  public static boolean includes(final Set<String> buckets, final Bucket bucket) {
    if (buckets == null)
      return true;
    if (buckets instanceof BucketIdSet byId)
      return byId.containsId(bucket.getFileId());
    return buckets.contains(bucket.getName()) || buckets.contains("*");
  }

  /** The file ids of {@code buckets}: their own when they are a {@link BucketIdSet}, otherwise those of the buckets by the names. */
  public static List<Integer> idsOf(final Set<String> buckets, final Schema schema) {
    if (buckets instanceof BucketIdSet byId)
      return byId.idList();
    final List<Integer> result = new ArrayList<>(buckets.size());
    for (final String name : buckets)
      result.add(schema.getBucketByName(name).getFileId());
    return result;
  }

  public boolean containsId(final int bucketId) {
    return Arrays.binarySearch(ids, bucketId) >= 0;
  }

  /** The buckets of this set that pass {@code keep}, by id. */
  public BucketIdSet filter(final IntPredicate keep) {
    return new BucketIdSet(schema, Arrays.stream(ids).filter(keep).toArray());
  }

  public List<Integer> idList() {
    final List<Integer> result = new ArrayList<>(ids.length);
    for (final int id : ids)
      result.add(id);
    return result;
  }

  /**
   * By the bucket the name resolves to now. Best effort for the callers that only have a name: one that holds the bucket asks
   * {@link #includes}, which a concurrent rename cannot fool.
   */
  @Override
  public boolean contains(final Object o) {
    if (!(o instanceof String name))
      return false;
    final Bucket bucket = schema.getBucketByNameIfExists(name);
    return bucket != null && containsId(bucket.getFileId());
  }

  /** The names the buckets have now, leaving out a bucket dropped since. */
  @Override
  public Iterator<String> iterator() {
    return new Iterator<>() {
      private int    next = 0;
      private String name = advance();

      private String advance() {
        while (next < ids.length) {
          final Bucket bucket = schema.getBucketByIdIfExists(ids[next++]);
          if (bucket != null)
            return bucket.getName();
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return name != null;
      }

      @Override
      public String next() {
        if (name == null)
          throw new NoSuchElementException();
        final String current = name;
        name = advance();
        return current;
      }
    };
  }

  @Override
  public int size() {
    int size = 0;
    for (final int id : ids)
      if (schema.getBucketByIdIfExists(id) != null)
        ++size;
    return size;
  }
}
