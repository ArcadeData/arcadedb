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
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Focused tests for the linked-list chain traversal in the in-memory
 * Compressed*Index classes. Each `while (true)` chain-walk in those classes is
 * guarded by an `assert nextPos > lastChainPos` invariant; these tests exercise
 * the chain explicitly by forcing many hash collisions onto a single bucket.
 */
class CompressedIndexChainTest extends TestHelper {

  /** Small bucket count forces every put() onto the same hash bucket, exercising the chain walk. */
  private static final int FORCE_COLLISIONS = 1;

  @Test
  void rid2ridGetReturnsNullOnEmptyIndex() throws Exception {
    final CompressedRID2RIDIndex index = new CompressedRID2RIDIndex(database, 16, 16);
    assertThat(index.get(new RID(3, 1))).isNull();
  }

  @Test
  void rid2ridChainWalkFindsAllCollidingKeys() throws Exception {
    final CompressedRID2RIDIndex index = new CompressedRID2RIDIndex(database, FORCE_COLLISIONS, 64);

    for (int i = 0; i < 50; i++)
      index.put(new RID(3, i), new RID(4, i));

    for (int i = 0; i < 50; i++)
      assertThat(index.get(new RID(3, i))).isEqualTo(new RID(4, i));

    assertThat(index.get(new RID(3, 9999))).isNull();
  }

  @Test
  void rid2ridDuplicateKeyRejected() throws Exception {
    final CompressedRID2RIDIndex index = new CompressedRID2RIDIndex(database, FORCE_COLLISIONS, 64);
    index.put(new RID(3, 1), new RID(4, 1));
    assertThatThrownBy(() -> index.put(new RID(3, 1), new RID(4, 2)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rid2ridsGetReturnsNullOnEmptyIndex() throws Exception {
    final CompressedRID2RIDsIndex index = new CompressedRID2RIDsIndex(database, 16, 16);
    assertThat(index.get(new RID(3, 1))).isNull();
  }

  @Test
  void rid2ridsChainWalkFindsAllCollidingKeysWithMultipleValues() throws Exception {
    final CompressedRID2RIDsIndex index = new CompressedRID2RIDsIndex(database, FORCE_COLLISIONS, 64);

    // 20 distinct keys all in the same bucket; each key has 3 values.
    for (int i = 0; i < 20; i++) {
      index.put(new RID(3, i), new RID(4, i * 10), new RID(5, i * 10));
      index.put(new RID(3, i), new RID(4, i * 10 + 1), new RID(5, i * 10 + 1));
      index.put(new RID(3, i), new RID(4, i * 10 + 2), new RID(5, i * 10 + 2));
    }

    for (int i = 0; i < 20; i++) {
      final List<Pair<RID, RID>> values = index.get(new RID(3, i));
      assertThat(values).hasSize(3);
    }

    assertThat(index.get(new RID(3, 9999))).isNull();
  }

  @Test
  void any2ridGetReturnsNullOnEmptyIndex() throws Exception {
    final CompressedAny2RIDIndex<String> index = new CompressedAny2RIDIndex<>(database, com.arcadedb.schema.Type.STRING, 16);
    assertThat(index.get("missing")).isNull();
  }

  @Test
  void any2ridChainWalkFindsAllCollidingKeys() throws Exception {
    final CompressedAny2RIDIndex<String> index = new CompressedAny2RIDIndex<>(database, com.arcadedb.schema.Type.STRING, FORCE_COLLISIONS);

    for (int i = 0; i < 50; i++)
      index.put("key-" + i, new RID(4, i));

    for (int i = 0; i < 50; i++)
      assertThat(index.get("key-" + i)).isEqualTo(new RID(4, i));

    assertThat(index.get("absent")).isNull();
  }

  @Test
  void any2ridDuplicateKeyRejected() throws Exception {
    final CompressedAny2RIDIndex<String> index = new CompressedAny2RIDIndex<>(database, com.arcadedb.schema.Type.STRING, FORCE_COLLISIONS);
    index.put("k", new RID(4, 1));
    assertThatThrownBy(() -> index.put("k", new RID(4, 2)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
