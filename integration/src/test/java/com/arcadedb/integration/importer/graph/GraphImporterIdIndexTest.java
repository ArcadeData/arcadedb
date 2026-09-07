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
package com.arcadedb.integration.importer.graph;

import com.arcadedb.integration.importer.graph.GraphImporter.IdIndex;
import com.arcadedb.integration.importer.graph.GraphImporter.LongIntMap;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.arcadedb.integration.importer.graph.GraphImporter.NOT_CANONICAL_LONG;
import static com.arcadedb.integration.importer.graph.GraphImporter.canonicalLong;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The identity key space {@link GraphImporter} resolves edges through: which spellings become a
 * primitive key, which stay text, and that the promotion from the {@code int} table to the
 * {@code long} one loses nothing (issue #7244).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphImporterIdIndexTest {

  @Test
  void canonicalDecimalTextBecomesANumericKey() {
    assertThat(canonicalLong("0")).isZero();
    assertThat(canonicalLong("7")).isEqualTo(7);
    assertThat(canonicalLong("-5")).isEqualTo(-5);
    assertThat(canonicalLong("2257721487")).isEqualTo(2257721487L);
    assertThat(canonicalLong(String.valueOf(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
    assertThat(canonicalLong(String.valueOf(Long.MIN_VALUE + 1))).isEqualTo(Long.MIN_VALUE + 1);
    assertThat(canonicalLong(String.valueOf(Integer.MIN_VALUE))).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  void everythingElseStaysText() {
    assertThat(canonicalLong(null)).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("-")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("W13696992")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("12a")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("1.0")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong(" 1")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("1 ")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("+1")).isEqualTo(NOT_CANONICAL_LONG);
    // a value the source spelled differently from Long.toString is a distinct identity
    assertThat(canonicalLong("007")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("00")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("-0")).isEqualTo(NOT_CANONICAL_LONG);
    // out of range, and the one in-range value reserved as the sentinel
    assertThat(canonicalLong("9223372036854775808")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("-9223372036854775809")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong("99999999999999999999")).isEqualTo(NOT_CANONICAL_LONG);
    assertThat(canonicalLong(String.valueOf(Long.MIN_VALUE))).isEqualTo(NOT_CANONICAL_LONG);
  }

  /**
   * Even the two keys the primitive tables cannot hold - {@code Long.MIN_VALUE}, which is the
   * sentinel, and {@code Integer.MIN_VALUE}, which marks an empty slot in the {@code int} table -
   * resolve, because the index falls back rather than storing them where they would be read as
   * "nothing here".
   */
  @Test
  void reservedKeyValuesStillResolve() {
    final IdIndex index = new IdIndex();
    index.put(String.valueOf(Long.MIN_VALUE), 1);
    index.put(String.valueOf(Integer.MIN_VALUE), 2);
    index.put("0", 3);

    assertThat(index.get(String.valueOf(Long.MIN_VALUE))).isEqualTo(1);
    assertThat(index.get(String.valueOf(Integer.MIN_VALUE))).isEqualTo(2);
    assertThat(index.get("0")).isEqualTo(3);
  }

  @Test
  void anAbsentOrUnknownKeyResolvesToMinusOne() {
    final IdIndex index = new IdIndex();
    assertThat(index.get(null)).isEqualTo(-1);
    assertThat(index.get("1")).isEqualTo(-1);
    assertThat(index.get("nope")).isEqualTo(-1);

    index.put("1", 0);
    index.put(null, 9);
    assertThat(index.get("2")).isEqualTo(-1);
    assertThat(index.get("nope")).isEqualTo(-1);
  }

  /**
   * The index starts on the {@code int} table and widens once. Keys registered before the widening
   * have to survive it, and a later key must not be looked up in the table that no longer holds
   * anything.
   */
  @Test
  void wideningToLongKeysPreservesEveryKey() {
    final IdIndex index = new IdIndex();
    for (int i = 0; i < 10_000; i++)
      index.put(String.valueOf(i), i);
    index.put("-1", 10_000);

    // forces the promotion
    index.put("2257721487", 10_001);

    for (int i = 0; i < 10_000; i++)
      assertThat(index.get(String.valueOf(i))).isEqualTo(i);
    assertThat(index.get("-1")).isEqualTo(10_000);
    assertThat(index.get("2257721487")).isEqualTo(10_001);

    // and a key registered after the promotion, on either side of the int boundary
    index.put("42", 10_002);
    index.put("-9000000000", 10_003);
    assertThat(index.get("42")).isEqualTo(10_002);
    assertThat(index.get("-9000000000")).isEqualTo(10_003);
  }

  @Test
  void numericAndTextualKeysCoexistWithoutColliding() {
    final IdIndex index = new IdIndex();
    index.put("7", 1);
    index.put("007", 2);
    index.put("W7", 3);
    index.put("2257721487", 4);

    assertThat(index.get("7")).isEqualTo(1);
    assertThat(index.get("007")).isEqualTo(2);
    assertThat(index.get("W7")).isEqualTo(3);
    assertThat(index.get("2257721487")).isEqualTo(4);
    assertThat(index.get("0007")).isEqualTo(-1);
  }

  @Test
  void aRepeatedKeyKeepsTheLastRegistration() {
    final IdIndex index = new IdIndex();
    index.put("1", 10);
    index.put("1", 11);
    index.put("W1", 20);
    index.put("W1", 21);
    index.put("9000000000", 30);
    index.put("9000000000", 31);

    assertThat(index.get("1")).isEqualTo(11);
    assertThat(index.get("W1")).isEqualTo(21);
    assertThat(index.get("9000000000")).isEqualTo(31);
  }

  /**
   * Keys that differ only above the table's width - ids allocated in blocks, or carrying a fixed
   * stride - must still spread. Masking the low bits of {@code key * odd} makes the slot depend on
   * the key's low bits alone, which sends every one of these to the same slot and turns the lookup
   * into a linear scan of the whole table.
   */
  @Test
  void stridedKeysDoNotAllLandInOneSlot() {
    final IdIndex index = new IdIndex();
    final int stride = 1 << 16;
    for (int i = 0; i < 2_000; i++)
      index.put(String.valueOf((long) i * stride), i);
    for (int i = 0; i < 2_000; i++)
      assertThat(index.get(String.valueOf((long) i * stride))).isEqualTo(i);

    // and past the int boundary, so the widened table is exercised the same way
    final IdIndex wide = new IdIndex();
    for (int i = 0; i < 2_000; i++)
      wide.put(String.valueOf(Integer.MAX_VALUE + (long) i * stride), i);
    for (int i = 0; i < 2_000; i++)
      assertThat(wide.get(String.valueOf(Integer.MAX_VALUE + (long) i * stride))).isEqualTo(i);
  }

  /**
   * The widened map is new code on the hot path, so it is checked against {@link HashMap} over
   * enough keys to resize several times and to collide.
   */
  @Test
  void longIntMapAgreesWithAReferenceMap() {
    final LongIntMap map = new LongIntMap(16);
    final Map<Long, Integer> reference = new HashMap<>();
    final Random random = new Random(7244);

    for (int i = 0; i < 50_000; i++) {
      final long key = random.nextInt(4) == 0 ? random.nextInt(100) : random.nextLong();
      if (key == Long.MIN_VALUE)
        continue;
      map.put(key, i);
      reference.put(key, i);
    }

    for (final Map.Entry<Long, Integer> entry : reference.entrySet())
      assertThat(map.get(entry.getKey(), -1)).isEqualTo(entry.getValue());

    for (int i = 0; i < 1_000; i++) {
      final long key = random.nextLong();
      if (!reference.containsKey(key) && key != Long.MIN_VALUE)
        assertThat(map.get(key, -1)).isEqualTo(-1);
    }
  }
}
