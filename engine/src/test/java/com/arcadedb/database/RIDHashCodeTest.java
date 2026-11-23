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

import com.arcadedb.graph.LightEdgeRID;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.function.IntSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The hash codes of the keys the engine puts in its hash tables must spread over a table's bins. A {@code HashMap} picks the bin
 * from the low bits of the hash, so a hash that only adds the parts of a key (bucket * 31 + offset) leaves most bins empty when
 * the offsets are what a bucket really assigns: page * 2048 + slot, with a few dozen slots in use per page. The bins that remain
 * hold dozens of keys and the table turns them into trees; a full-text search builds one such map of RIDs per bucket and term.
 * <p>
 * The hash of a {@code PageId} has the same shape and its test is in {@code PageIdHashCodeTest}.
 */
class RIDHashCodeTest {
  private static final int TABLE = 1 << 16;
  /** {@code HashMap.TREEIFY_THRESHOLD}: a bin this long becomes a tree. */
  private static final int TREEIFY = 8;

  /** How many bins of a {@code HashMap} table hold {@link #TREEIFY} keys or more, and how many bins are used at all. */
  private record Spread(int treeBins, int usedBins) {
  }

  private static Spread spread(final IntSupplier[] hashes) {
    final int[] bins = new int[TABLE];
    for (final IntSupplier hash : hashes) {
      final int h = hash.getAsInt();
      ++bins[(h ^ (h >>> 16)) & (TABLE - 1)];
    }
    int treeBins = 0;
    int used = 0;
    for (final int count : bins) {
      if (count > 0)
        ++used;
      if (count >= TREEIFY)
        ++treeBins;
    }
    return new Spread(treeBins, used);
  }

  @Test
  void theRidsOfOneBucketSpreadOverTheBinsOfAHashMap() {
    // 1,024 pages of 44 records, as a bucket assigns the offsets
    final IntSupplier[] hashes = new IntSupplier[1024 * 44];
    int i = 0;
    for (int page = 0; page < 1024; page++)
      for (int slot = 0; slot < 44; slot++) {
        final RID rid = new RID(3, page * 2048L + slot);
        hashes[i++] = rid::hashCode;
      }

    final Spread spread = spread(hashes);

    assertThat(spread.treeBins()).as("bins that would become trees").isLessThan(32);
    assertThat(spread.usedBins()).as("bins in use out of %d for %d keys", TABLE, hashes.length).isGreaterThan(TABLE / 3);
  }

  @Test
  void theRidsOfDifferentBucketsDoNotCollide() {
    final IntSupplier[] hashes = new IntSupplier[16 * 3000];
    int i = 0;
    for (int bucket = 40; bucket < 56; bucket++)
      for (int offset = 0; offset < 3000; offset++) {
        final RID rid = new RID(bucket, offset);
        hashes[i++] = rid::hashCode;
      }

    assertThat(spread(hashes).treeBins()).isLessThan(32);
  }

  @Test
  void theHashOfAnEqualRidIsTheSame() {
    assertThat(new RID(7, 123456L).hashCode()).isEqualTo(new RID(7, 123456L).hashCode());
    assertThat(new RID(7, 123456L)).isEqualTo(new RID(7, 123456L)).isNotEqualTo(new RID(8, 123456L));
  }

  /**
   * A record-less RID (a lightweight edge) has the same bucket and offset for every edge of its type: the endpoints are what
   * tells two of them apart, in {@code equals()} and so in the hash too.
   */
  @Test
  void aLightweightEdgeHashesByItsEndpoints() {
    final int edgeBucket = 9;
    final Set<Integer> hashes = new HashSet<>();
    final int vertices = 60;
    for (int out = 0; out < vertices; out++)
      for (int in = 0; in < vertices; in++) {
        final LightEdgeRID edge = new LightEdgeRID(null, edgeBucket, new RID(4, out * 2048L + 5), new RID(5, in * 2048L + 5));
        final LightEdgeRID same = new LightEdgeRID(null, edgeBucket, new RID(4, out * 2048L + 5), new RID(5, in * 2048L + 5));

        assertThat(edge).isEqualTo(same);
        assertThat(edge.hashCode()).as("the same endpoints hash the same").isEqualTo(same.hashCode());
        hashes.add(edge.hashCode());
      }

    assertThat(hashes.size()).as("distinct hashes of %d lightweight edges that share one bucket and offset", vertices * vertices)
        .isGreaterThan(vertices * vertices * 99 / 100);
  }

  @Test
  void aLightweightEdgeDoesNotHashLikeItsReverse() {
    final RID a = new RID(4, 2048L * 3 + 1);
    final RID b = new RID(5, 2048L * 7 + 2);

    final LightEdgeRID forward = new LightEdgeRID(null, 9, a, b);
    final LightEdgeRID reverse = new LightEdgeRID(null, 9, b, a);

    assertThat(forward).isNotEqualTo(reverse);
    assertThat(forward.hashCode()).isNotEqualTo(reverse.hashCode());
  }

  @Test
  void theLightweightEdgesOfOneTypeSpreadOverTheBinsOfAHashMap() {
    final IntSupplier[] hashes = new IntSupplier[100 * 100];
    int i = 0;
    for (int out = 0; out < 100; out++)
      for (int in = 0; in < 100; in++) {
        final LightEdgeRID edge = new LightEdgeRID(null, 9, new RID(4, out * 2048L + 3), new RID(4, in * 2048L + 3));
        hashes[i++] = edge::hashCode;
      }

    final Spread spread = spread(hashes);

    assertThat(spread.treeBins()).as("bins that would become trees").isLessThan(32);
    assertThat(spread.usedBins()).as("bins in use out of %d for %d keys", TABLE, hashes.length).isGreaterThan(TABLE / 8);
  }
}
