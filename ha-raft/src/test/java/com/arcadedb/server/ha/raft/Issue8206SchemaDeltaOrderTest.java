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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8206: a follower writes the document {@link SchemaDelta#apply} returns to its {@code schema.json} verbatim,
 * so the merged document must carry the leader's key ORDER as well as its key set. Before the fix a child the delta
 * added was appended after the receiver's own children, and a follower's schema file listed a new type last while
 * the leader's listed it in its own place.
 */
class Issue8206SchemaDeltaOrderTest {

  private static JSONObject schema(final long version, final String... typeNames) {
    final JSONObject root = new JSONObject();
    root.put("schemaVersion", version);
    root.put("dbmsVersion", "26.10.1");

    final JSONObject types = new JSONObject();
    for (final String name : typeNames)
      types.put(name, new JSONObject().put("type", "v").put("name", name));
    root.put("types", types);

    root.put("triggers", new JSONObject());
    root.put("functions", new JSONObject());
    return root;
  }

  @Test
  void aTypeAddedInTheMiddleKeepsTheLeadersPosition() {
    final JSONObject base = schema(1, "E1", "Person", "V1");
    final JSONObject updated = schema(2, "E1", "Person", "RaftIndexedVertex0", "V1");

    final JSONObject merged = SchemaDelta.apply(base, SchemaDelta.compute(base, updated));

    assertThat(merged.toString()).isEqualTo(updated.toString());
  }

  @Test
  void aDropFollowedByACreateKeepsTheLeadersOrder() {
    final JSONObject base = schema(1, "E1", "RaftRuntimeVertex0", "V1");
    final JSONObject updated = schema(2, "E1", "RaftIndexedVertex0", "V1");

    final JSONObject merged = SchemaDelta.apply(base, SchemaDelta.compute(base, updated));

    assertThat(merged.toString()).isEqualTo(updated.toString());
  }

  @Test
  void aRootSectionAddedBeforeAnExistingOneKeepsTheLeadersPosition() {
    final JSONObject base = schema(1, "V1");
    final JSONObject updated = new JSONObject();
    updated.put("schemaVersion", 2L);
    updated.put("dbmsVersion", "26.10.1");
    updated.put("extensions", new JSONObject().put("module", new JSONObject().put("on", true)));
    updated.put("types", base.getJSONObject("types").copy());
    updated.put("triggers", new JSONObject());
    updated.put("functions", new JSONObject());

    final JSONObject merged = SchemaDelta.apply(base, SchemaDelta.compute(base, updated));

    assertThat(merged.toString()).isEqualTo(updated.toString());
  }
}
