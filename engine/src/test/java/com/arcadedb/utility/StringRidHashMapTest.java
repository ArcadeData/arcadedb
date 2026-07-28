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

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The temporary-id map of a streaming bulk load has to behave exactly like the {@code HashMap<String, RID>} it
 * replaces - exact key matching included - while holding no object per entry (issue #5470).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StringRidHashMapTest {

  @Test
  void storesAndResolvesKeys() {
    final StringRidHashMap map = new StringRidHashMap();

    map.put("a", new RID(3, 7));
    map.put("__dk/address/63dafeed291357804558b10cdd19fc8c", new RID(12, 4_294_967_296L));

    assertThat(map.size()).isEqualTo(2);
    assertThat(map.isEmpty()).isFalse();
    assertThat(map.get("a")).isEqualTo(new RID(3, 7));
    assertThat(map.get("__dk/address/63dafeed291357804558b10cdd19fc8c")).isEqualTo(new RID(12, 4_294_967_296L));
    assertThat(map.get("missing")).isNull();
    assertThat(new StringRidHashMap().isEmpty()).isTrue();
  }

  @Test
  void storingAKeyTwiceOverwritesItLikeHashMap() {
    final StringRidHashMap map = new StringRidHashMap();

    map.put("dup", new RID(1, 1));
    map.put("dup", new RID(2, 2));

    assertThat(map.size()).isEqualTo(1);
    assertThat(map.get("dup")).isEqualTo(new RID(2, 2));
  }

  @Test
  void emptyAndUnicodeKeysRoundTrip() {
    final StringRidHashMap map = new StringRidHashMap();

    map.put("", new RID(1, 1));
    map.put("città-Ω-日本語-😀", new RID(2, 2));
    map.put("città-Ω-日本語-😀 ", new RID(3, 3));

    assertThat(map.get("")).isEqualTo(new RID(1, 1));
    assertThat(map.get("città-Ω-日本語-😀")).isEqualTo(new RID(2, 2));
    assertThat(map.get("città-Ω-日本語-😀 ")).isEqualTo(new RID(3, 3));
    assertThat(map.get("citta-Ω-日本語-😀")).isNull();
  }

  /**
   * Keys sharing a long prefix are the normal case for bulk-load ids, and they are also the case a weak hash or a
   * sloppy comparison would get wrong.
   */
  @Test
  void matchesExactlyAcrossManyEntriesAndSeveralResizes() {
    final int entries = 200_000;
    final StringRidHashMap map = new StringRidHashMap();

    for (int i = 0; i < entries; i++)
      map.put("__dk/address/" + i, new RID(i % 16, i));

    assertThat(map.size()).isEqualTo(entries);

    for (int i = 0; i < entries; i++)
      assertThat(map.get("__dk/address/" + i)).as("key %d", i).isEqualTo(new RID(i % 16, i));

    assertThat(map.get("__dk/address/" + entries)).isNull();
    assertThat(map.get("__dk/address/")).isNull();
  }

  @Test
  void iteratesOverEveryEntry() {
    final Map<String, RID> expected = new HashMap<>();
    final StringRidHashMap map = new StringRidHashMap();

    for (int i = 0; i < 5_000; i++) {
      final String key = UUID.randomUUID().toString();
      final RID rid = new RID(i % 8, i);
      expected.put(key, rid);
      map.put(key, rid);
    }

    final Map<String, RID> seen = new HashMap<>();
    map.forEach(seen::put);

    assertThat(seen).isEqualTo(expected);
  }

  /**
   * The reason this class exists: a {@code HashMap<String, RID>} costs ~170 bytes for a key of this length (and four
   * objects), and the load that hit issue #5470 keeps one entry per vertex alive for the whole request.
   */
  @Test
  void holdsFarLessThanAHashMapWould() {
    final int entries = 100_000;
    final StringRidHashMap map = new StringRidHashMap();

    for (int i = 0; i < entries; i++)
      map.put("__dk/address/63dafeed291357804558b10cdd19fc" + String.format("%03d", i % 1000) + i, new RID(1, i));

    assertThat(map.retainedBytes() / entries).isLessThan(110);
  }
}
