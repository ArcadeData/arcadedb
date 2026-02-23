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
package com.arcadedb.engine.timeseries.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Dictionary encoding for low-cardinality string columns (e.g., tags).
 * Builds a per-block dictionary (String → int16), emits dictionary + int16[] indices.
 * <p>
 * Format:
 * - 4 bytes: value count
 * - 2 bytes: dictionary size
 * - For each dictionary entry: 2 bytes length + UTF-8 bytes
 * - For each value: 2 bytes dictionary index
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DictionaryCodec {

  public static final int MAX_DICTIONARY_SIZE = 65535;

  private DictionaryCodec() {
  }

  public static byte[] encode(final String[] values) {
    if (values == null || values.length == 0)
      return new byte[0];

    // Build dictionary (use int counter to avoid short overflow)
    final Map<String, Integer> dict = new HashMap<>();
    final String[] dictEntries = new String[Math.min(values.length, MAX_DICTIONARY_SIZE)];
    int nextIndex = 0;

    final int[] indices = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      Integer idx = dict.get(values[i]);
      if (idx == null) {
        if (nextIndex >= MAX_DICTIONARY_SIZE)
          throw new IllegalArgumentException("Dictionary overflow: more than " + MAX_DICTIONARY_SIZE + " unique values");
        idx = nextIndex;
        dict.put(values[i], idx);
        dictEntries[nextIndex] = values[i];
        nextIndex++;
      }
      indices[i] = idx;
    }

    // Calculate buffer size
    int size = 4 + 2; // count + dict size
    for (int i = 0; i < nextIndex; i++) {
      final byte[] utf8 = dictEntries[i].getBytes(StandardCharsets.UTF_8);
      size += 2 + utf8.length;
    }
    size += values.length * 2; // indices

    final ByteBuffer buf = ByteBuffer.allocate(size);
    buf.putInt(values.length);
    buf.putShort((short) nextIndex);

    for (int i = 0; i < nextIndex; i++) {
      final byte[] utf8 = dictEntries[i].getBytes(StandardCharsets.UTF_8);
      buf.putShort((short) utf8.length);
      buf.put(utf8);
    }

    for (final int index : indices)
      buf.putShort((short) index);

    return buf.array();
  }

  public static String[] decode(final byte[] data) {
    if (data == null || data.length == 0)
      return new String[0];

    final ByteBuffer buf = ByteBuffer.wrap(data);
    final int count = buf.getInt();
    final int dictSize = buf.getShort() & 0xFFFF;

    final String[] dictEntries = new String[dictSize];
    for (int i = 0; i < dictSize; i++) {
      final int len = buf.getShort() & 0xFFFF;
      final byte[] utf8 = new byte[len];
      buf.get(utf8);
      dictEntries[i] = new String(utf8, StandardCharsets.UTF_8);
    }

    final String[] result = new String[count];
    for (int i = 0; i < count; i++) {
      final int idx = buf.getShort() & 0xFFFF;
      result[i] = dictEntries[idx];
    }
    return result;
  }
}
