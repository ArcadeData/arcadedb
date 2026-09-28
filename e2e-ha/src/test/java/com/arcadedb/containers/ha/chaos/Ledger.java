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
import java.util.Random;

/**
 * Client-side record of every write the workload attempted and what the cluster answered. Keys pack the writer index
 * and its sequence number into one {@code long}; outcomes are one byte per operation in per-writer arrays, so a soak
 * run with millions of operations stays light on the garbage collector.
 */
public final class Ledger {
  public static final byte IN_FLIGHT    = 0;
  public static final byte ACKED        = 1;
  public static final byte FAILED       = 2;
  public static final byte UNKNOWN      = 3;
  public static final byte ACKED_LATE   = 4;
  public static final byte LOST_UNKNOWN = 5;

  static final         int  SEQ_BITS     = 40;
  static final         long SEQ_MASK     = (1L << SEQ_BITS) - 1;
  private static final byte PAIR_FLAG    = 0x10;
  private static final byte OUTCOME_MASK = 0x0F;
  private static final int  PICK_TRIES   = 8;

  private final Lane[] lanes;

  public Ledger(final int writers) {
    lanes = new Lane[writers];
    for (int i = 0; i < writers; i++)
      lanes[i] = new Lane();
  }

  public int writers() {
    return lanes.length;
  }

  public static long key(final int writer, final long seq) {
    return ((long) writer << SEQ_BITS) | seq;
  }

  public static int writerOf(final long key) {
    return (int) (key >>> SEQ_BITS);
  }

  public static long seqOf(final long key) {
    return key & SEQ_MASK;
  }

  public static String format(final long key) {
    return "w" + writerOf(key) + "-" + seqOf(key);
  }

  public static String name(final byte outcome) {
    return switch (outcome) {
      case IN_FLIGHT -> "IN_FLIGHT";
      case ACKED -> "ACKED";
      case FAILED -> "FAILED";
      case UNKNOWN -> "UNKNOWN";
      case ACKED_LATE -> "ACKED_LATE";
      case LOST_UNKNOWN -> "LOST_UNKNOWN";
      default -> "?" + outcome;
    };
  }

  /**
   * Reserves the writer's next sequence number. The operation stays {@link #IN_FLIGHT} until {@link #record}.
   */
  public long reserve(final int writer, final boolean pair) {
    return key(writer, lanes[writer].reserve(pair));
  }

  public void record(final long key, final byte outcome) {
    lane(key).set((int) seqOf(key), outcome);
  }

  public byte outcome(final long key) {
    return (byte) (lane(key).get((int) seqOf(key)) & OUTCOME_MASK);
  }

  public boolean isPair(final long key) {
    return (lane(key).get((int) seqOf(key)) & PAIR_FLAG) != 0;
  }

  public int size(final int writer) {
    return lanes[writer].size();
  }

  public long count(final byte outcome) {
    long total = 0;
    for (final Lane lane : lanes)
      total += lane.count(outcome);
    return total;
  }

  /**
   * Picks a random acknowledged key of the writer, or -1 when a few random probes find none.
   */
  public long randomAckedKey(final int writer, final Random random) {
    final Lane lane = lanes[writer];
    final int size = lane.size();
    if (size == 0)
      return -1;
    for (int i = 0; i < PICK_TRIES; i++) {
      final int seq = random.nextInt(size);
      final int outcome = lane.get(seq) & OUTCOME_MASK;
      if (outcome == ACKED || outcome == ACKED_LATE)
        return key(writer, seq);
    }
    return -1;
  }

  private Lane lane(final long key) {
    final int writer = writerOf(key);
    if (key < 0 || writer >= lanes.length)
      throw new IllegalArgumentException("Unknown writer in key " + format(key));
    return lanes[writer];
  }

  private static final class Lane {
    private byte[] ops = new byte[1024];
    private int    size;

    synchronized int reserve(final boolean pair) {
      if (size == ops.length)
        ops = Arrays.copyOf(ops, size * 2);
      ops[size] = pair ? PAIR_FLAG : 0;
      return size++;
    }

    synchronized void set(final int seq, final byte outcome) {
      check(seq);
      ops[seq] = (byte) ((ops[seq] & PAIR_FLAG) | outcome);
    }

    synchronized byte get(final int seq) {
      check(seq);
      return ops[seq];
    }

    synchronized int size() {
      return size;
    }

    synchronized long count(final byte outcome) {
      long count = 0;
      for (int i = 0; i < size; i++)
        if ((ops[i] & OUTCOME_MASK) == outcome)
          ++count;
      return count;
    }

    private void check(final int seq) {
      if (seq < 0 || seq >= size)
        throw new IllegalArgumentException("Sequence " + seq + " was never reserved");
    }
  }
}
