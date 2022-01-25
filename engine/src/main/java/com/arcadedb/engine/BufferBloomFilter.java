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
package com.arcadedb.engine;

import com.arcadedb.database.Binary;

public class BufferBloomFilter {
  private final Binary buffer;
  private final int    hashSeed;
  private final int    capacity;

  public BufferBloomFilter(final Binary buffer, final int slots, final int hashSeed) {
    if (slots % 8 > 0)
      throw new IllegalArgumentException("Slots must be a multiplier of 8");
    this.buffer = buffer;
    this.hashSeed = hashSeed;
    this.capacity = slots;
  }

  public void add(final int value) {
    final byte[] b = new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    final int hash = MurmurHash.hash32(b, 4, hashSeed);
    final int h = hash != Integer.MIN_VALUE ? Math.abs(hash) : Integer.MAX_VALUE;

    final int bit2change = h > capacity ? h % capacity : h;
    final int byte2change = bit2change / 8;
    final int bitInByte2change = bit2change % 8;

    final byte v = buffer.getByte(byte2change);
    buffer.putByte(byte2change, (byte) (v | (1 << bitInByte2change)));
  }

  public boolean mightContain(final int value) {
    final byte[] b = new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    final int hash = MurmurHash.hash32(b, 4, hashSeed);
    final int h = hash != Integer.MIN_VALUE ? Math.abs(hash) : Integer.MAX_VALUE;

    final int bit2change = h > capacity ? h % capacity : h;
    final int byte2change = bit2change / 8;
    final int bitInByte2change = bit2change % 8;

    final byte v = buffer.getByte(byte2change);
    return ((v >> bitInByte2change) & 1) == 1;
  }
}
