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
package com.arcadedb.bench.workload.ha;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ContentDigestTest {
  private static JSONObject record(final String rid, final long id, final String payload) {
    return new JSONObject().put("@rid", rid).put("id", id).put("writer", 0).put("payload", payload);
  }

  @Test
  void keyOrderDoesNotChangeTheDigest() {
    final JSONObject a = new JSONObject().put("@rid", "#1:0").put("id", 1L).put("payload", "p");
    final JSONObject b = new JSONObject().put("payload", "p").put("id", 1L).put("@rid", "#1:0");
    final String digest = ContentDigest.of(List.of(a).iterator());
    assertThat(digest).hasSize(64).matches("[0-9a-f]+");
    assertThat(ContentDigest.of(List.of(b).iterator())).isEqualTo(digest);
  }

  @Test
  void oneChangedValueChangesIt() {
    final String digest = ContentDigest.of(List.of(record("#1:0", 1, "p"), record("#1:1", 2, "q")).iterator());
    assertThat(ContentDigest.of(List.of(record("#1:0", 1, "p"), record("#1:1", 2, "r")).iterator())).isNotEqualTo(digest);
    // the same contents at another rid is another node state
    assertThat(ContentDigest.of(List.of(record("#1:0", 1, "p"), record("#1:2", 2, "q")).iterator())).isNotEqualTo(digest);
    // a missing record too
    assertThat(ContentDigest.of(List.of(record("#1:0", 1, "p")).iterator())).isNotEqualTo(digest);
  }

  @Test
  void differencesNameTheNodeAndType() {
    assertThat(ContentDigest.differences("HaRecord", new String[] { "aa", "aa", "aa" })).isEmpty();
    // ha-0 is the leader here: the digests are by node, the leader's is the first one compared against
    assertThat(ContentDigest.differences("HaRecord", new String[] { "aa", "bb", "aa" }, 0))
        .containsExactly("ha-1 differs from the leader on HaRecord");
    // the leader is ha-2: ha-0 and ha-1 agree with each other but not with it
    assertThat(ContentDigest.differences("HaRecord", new String[] { "aa", "aa", "cc" }, 2))
        .containsExactly("ha-0 differs from the leader on HaRecord", "ha-1 differs from the leader on HaRecord");
  }
}
