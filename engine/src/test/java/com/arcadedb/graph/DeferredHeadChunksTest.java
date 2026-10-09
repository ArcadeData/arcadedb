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

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The map {@link GraphBatch} keeps its deferred edge-list heads in (issue #9575): it must answer exactly what the two
 * {@code LongObjectHashMap<RID>} it replaced answered, through growth and through the backward-shift deletes the undo
 * logs drive.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeferredHeadChunksTest {

  @Test
  void bothDirectionsOfAVertexShareOneEntry() {
    final DeferredHeadChunks heads = new DeferredHeadChunks();
    final long key = key(3, 42);

    assertThat(heads.getOut(key)).isNull();
    assertThat(heads.getIn(key)).isNull();
    assertThat(heads.isEmpty()).isTrue();

    heads.putOut(key, new RID(7, 100));
    assertThat(heads.getOut(key)).isEqualTo(new RID(7, 100));
    assertThat(heads.getIn(key)).isNull();

    heads.putIn(key, new RID(8, 200));
    assertThat(heads.getIn(key)).isEqualTo(new RID(8, 200));
    assertThat(heads.size()).isEqualTo(1);
    assertThat(heads.outSize()).isEqualTo(1);
    assertThat(heads.inSize()).isEqualTo(1);

    // overwriting a head is not a new head
    heads.putOut(key, new RID(7, 101));
    assertThat(heads.getOut(key)).isEqualTo(new RID(7, 101));
    assertThat(heads.outSize()).isEqualTo(1);

    // the entry outlives one direction and goes with the second
    heads.removeOut(key);
    assertThat(heads.getOut(key)).isNull();
    assertThat(heads.getIn(key)).isEqualTo(new RID(8, 200));
    assertThat(heads.size()).isEqualTo(1);
    heads.removeIn(key);
    assertThat(heads.isEmpty()).isTrue();
    assertThat(heads.outSize()).isZero();
    assertThat(heads.inSize()).isZero();

    // removing what is not there changes nothing
    heads.removeIn(key);
    heads.removeOut(key(1, 1));
    assertThat(heads.isEmpty()).isTrue();
  }

  @Test
  void theExtremesOfThePackedLayoutRoundTrip() {
    final DeferredHeadChunks heads = new DeferredHeadChunks();
    final RID highest = new RID((1 << 23) - 1, 0xFFFFFFFFFFL);
    heads.putOut(0L, new RID(0, 0));
    heads.putIn(0L, highest);
    assertThat(heads.getOut(0L)).isEqualTo(new RID(0, 0));
    assertThat(heads.getIn(0L)).isEqualTo(highest);

    assertThatThrownBy(() -> heads.putOut(1L, new RID(1 << 23, 0))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> heads.putOut(1L, new RID(0, 1L << 40))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> heads.putIn(1L, new RID(-1, -1))).isInstanceOf(IllegalArgumentException.class);
    // a refused put leaves no half-made entry behind
    assertThat(heads.size()).isEqualTo(1);
  }

  /**
   * Random puts and removes in both directions against two plain maps, through many growths and with deletes that land
   * in the middle of probe runs, which is where a backward shift that moves the wrong entry loses one.
   */
  @Test
  void matchesTwoPlainMapsUnderRandomPutsAndRemoves() {
    final DeferredHeadChunks heads = new DeferredHeadChunks();
    final Map<Long, RID> out = new HashMap<>();
    final Map<Long, RID> in = new HashMap<>();
    final Random random = new Random(9575);

    // Dense positions in few buckets: the real shape of vertex keys, and the one a weak hash clusters
    final long[] universe = new long[50_000];
    for (int i = 0; i < universe.length; i++)
      universe[i] = key(random.nextInt(4), random.nextInt(40_000));

    for (int step = 0; step < 400_000; step++) {
      final long key = universe[random.nextInt(universe.length)];
      final int op = random.nextInt(10);
      final RID rid = new RID(random.nextInt(1000), random.nextInt(1_000_000));
      switch (op) {
      case 0, 1, 2 -> {
        heads.putOut(key, rid);
        out.put(key, rid);
      }
      case 3, 4, 5 -> {
        heads.putIn(key, rid);
        in.put(key, rid);
      }
      case 6, 7 -> {
        heads.removeOut(key);
        out.remove(key);
      }
      default -> {
        heads.removeIn(key);
        in.remove(key);
      }
      }

      if (step % 50_000 == 0)
        assertSame(heads, out, in, universe);
    }
    assertSame(heads, out, in, universe);

    final Set<Long> expectedKeys = new HashSet<>(out.keySet());
    expectedKeys.addAll(in.keySet());
    final Set<Long> actualKeys = new HashSet<>();
    for (final long k : heads.keys())
      actualKeys.add(k);
    assertThat(actualKeys).isEqualTo(expectedKeys);
    assertThat(heads.keys()).hasSize(expectedKeys.size());

    heads.clear();
    assertThat(heads.isEmpty()).isTrue();
    assertThat(heads.outSize()).isZero();
    assertThat(heads.inSize()).isZero();
    for (final long k : universe)
      assertThat(heads.getOut(k)).isNull();
  }

  private static void assertSame(final DeferredHeadChunks heads, final Map<Long, RID> out, final Map<Long, RID> in,
      final long[] universe) {
    for (final long k : universe) {
      assertThat(heads.getOut(k)).isEqualTo(out.get(k));
      assertThat(heads.getIn(k)).isEqualTo(in.get(k));
    }
    final Set<Long> union = new HashSet<>(out.keySet());
    union.addAll(in.keySet());
    assertThat(heads.size()).isEqualTo(union.size());
    assertThat(heads.outSize()).isEqualTo(out.size());
    assertThat(heads.inSize()).isEqualTo(in.size());
  }

  private static long key(final int bucketId, final long position) {
    return ((long) bucketId << 40) | position;
  }
}
