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

package com.arcadedb.containers.ha.chaos;

import java.util.Arrays;
import java.util.BitSet;

/**
 * The {@code ChaosOp} rows one node holds, as per-writer bit sets indexed by sequence number. Rows that match no ledger
 * entry, appear twice, or carry more than one {@code NEXT} edge are kept aside for the checker.
 */
public final class NodeSnapshot {
  private final Ledger   ledger;
  private final BitSet[] present;
  private final BitSet[] withEdge;
  private final Keys     phantoms   = new Keys();
  private final Keys     duplicates = new Keys();
  private final Keys     multiEdge  = new Keys();
  private       long     rows;

  public NodeSnapshot(final Ledger ledger) {
    this.ledger = ledger;
    present = new BitSet[ledger.writers()];
    withEdge = new BitSet[ledger.writers()];
    for (int i = 0; i < present.length; i++) {
      present[i] = new BitSet();
      withEdge[i] = new BitSet();
    }
  }

  public void add(final long key, final int edges) {
    ++rows;
    final int writer = Ledger.writerOf(key);
    final long seq = Ledger.seqOf(key);
    if (key < 0 || writer >= present.length || seq >= ledger.size(writer)) {
      phantoms.add(key);
      return;
    }
    final int index = (int) seq;
    if (present[writer].get(index)) {
      duplicates.add(key);
      return;
    }
    present[writer].set(index);
    if (edges > 0)
      withEdge[writer].set(index);
    if (edges > 1)
      multiEdge.add(key);
  }

  public boolean present(final int writer, final int seq) {
    return present[writer].get(seq);
  }

  public boolean hasEdge(final int writer, final int seq) {
    return withEdge[writer].get(seq);
  }

  public long rows() {
    return rows;
  }

  public long[] phantoms() {
    return phantoms.toArray();
  }

  public long[] duplicates() {
    return duplicates.toArray();
  }

  public long[] multiEdge() {
    return multiEdge.toArray();
  }

  /**
   * Keys whose presence or edge state differs between the two snapshots, up to {@code limit}.
   */
  public long[] diff(final NodeSnapshot other, final int limit) {
    final Keys out = new Keys();
    for (int w = 0; w < present.length && out.size() < limit; w++) {
      final BitSet differing = (BitSet) present[w].clone();
      differing.xor(other.present[w]);
      final BitSet edges = (BitSet) withEdge[w].clone();
      edges.xor(other.withEdge[w]);
      differing.or(edges);
      for (int s = differing.nextSetBit(0); s >= 0 && out.size() < limit; s = differing.nextSetBit(s + 1))
        out.add(Ledger.key(w, s));
    }
    final long[] mine = phantoms.sorted();
    final long[] theirs = other.phantoms.sorted();
    for (final long key : mine)
      if (out.size() < limit && Arrays.binarySearch(theirs, key) < 0)
        out.add(key);
    for (final long key : theirs)
      if (out.size() < limit && Arrays.binarySearch(mine, key) < 0)
        out.add(key);
    return out.toArray();
  }

  private static final class Keys {
    private long[] values = new long[16];
    private int    size;

    void add(final long key) {
      if (size == values.length)
        values = Arrays.copyOf(values, size * 2);
      values[size++] = key;
    }

    int size() {
      return size;
    }

    long[] toArray() {
      return Arrays.copyOf(values, size);
    }

    long[] sorted() {
      final long[] copy = toArray();
      Arrays.sort(copy);
      return copy;
    }
  }
}
