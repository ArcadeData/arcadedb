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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An edge list must render every entry it holds, and an edge segment must honour {@code includeMetadata} (issue #8554).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8554EdgeSegmentToJsonTest extends TestHelper {

  @Test
  void edgeListSerializesItsEntries() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");

    final Set<String> expectedEdges = new HashSet<>();
    final RID[] source = new RID[1];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      source[0] = a.getIdentity();
      for (int i = 0; i < 3; i++) {
        final MutableVertex b = database.newVertex("V").save();
        expectedEdges.add(a.newEdge("E", b).getIdentity().toString());
      }
    });

    final VertexInternal vertex = (VertexInternal) database.lookupByRID(source[0], true);
    final EdgeLinkedList out = ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
    assertThat(out.count()).isEqualTo(3);

    final JSONArray json = out.toJSON();
    assertThat(json.length()).isEqualTo(3);
    final Set<String> edges = new HashSet<>();
    for (int i = 0; i < json.length(); i++) {
      final JSONObject entry = json.getJSONObject(i);
      edges.add(entry.getString("edge"));
      assertThat(entry.getString("vertex")).startsWith("#");
    }
    assertThat(edges).isEqualTo(expectedEdges);
  }

  @Test
  void segmentHonoursIncludeMetadata() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    final RID[] source = new RID[1];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      source[0] = a.getIdentity();
      a.newEdge("E", database.newVertex("V").save());
    });

    final VertexInternal vertex = (VertexInternal) database.lookupByRID(source[0], true);
    final EdgeSegment segment = (EdgeSegment) database.lookupByRID(vertex.getOutEdgesHeadChunk(), true);
    assertThat(segment.toJSON(false).has("@rid")).isFalse();
    assertThat(segment.toJSON(true).has("@rid")).isTrue();
    assertThat(segment.toJSON(false).getJSONArray("entries").length()).isEqualTo(1);
  }
}
