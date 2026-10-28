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
package com.arcadedb.utility;

/**
 * HyperLogLog cardinality estimator: HLL with HLL++'s 64-bit hash and small-range correction. Dense registers (one byte each, only the
 * low 6 bits used), no sparse encoding and no empirical bias tables, so the standard error is about 1.04 / sqrt(m), i.e. 0.81% at the
 * fixed precision of 14. The hash is a fixed function of the value (no run-time seed), so the same input always gives the same estimate.
 * Not thread safe: one instance per thread, combined with {@link #merge(HyperLogLog)}.
 */
public final class HyperLogLog {
  public static final int PRECISION = 14;

  private static final int    REGISTERS = 1 << PRECISION;
  private static final double ALPHA     = 0.7213 / (1 + 1.079 / REGISTERS);

  private final byte[] registers = new byte[REGISTERS];

  public HyperLogLog() {
  }

  public void addLong(final long value) {
    final long hash = hash(value);
    final int index = (int) (hash >>> (64 - PRECISION));
    // the guard bit caps the rank when the remaining bits are all zero
    final byte rank = (byte) (Long.numberOfLeadingZeros((hash << PRECISION) | (1L << (PRECISION - 1))) + 1);
    if (rank > registers[index])
      registers[index] = rank;
  }

  public void merge(final HyperLogLog other) {
    final byte[] o = other.registers;
    for (int i = 0; i < REGISTERS; i++)
      if (o[i] > registers[i])
        registers[i] = o[i];
  }

  public long estimate() {
    double sum = 0;
    int empty = 0;
    for (int i = 0; i < REGISTERS; i++) {
      final byte r = registers[i];
      sum += 1.0 / (1L << r);
      if (r == 0)
        empty++;
    }
    final double raw = ALPHA * REGISTERS * (double) REGISTERS / sum;
    if (raw <= 2.5 * REGISTERS && empty > 0)
      return Math.round(REGISTERS * Math.log((double) REGISTERS / empty));
    return Math.round(raw);
  }

  /**
   * The splitmix64 finalizer.
   */
  public static long hash(long value) {
    value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
    value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
    return value ^ (value >>> 31);
  }
}
