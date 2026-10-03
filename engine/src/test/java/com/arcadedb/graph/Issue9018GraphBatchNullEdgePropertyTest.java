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
import com.arcadedb.database.RID;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.schema.EdgeType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #9018: {@link GraphBatch#newEdge} given {@code null} for a property the edge type declares wrote the
 * declared type tag and no value bytes, so the reader took the following bytes as the value or ran past the record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9018GraphBatchNullEdgePropertyTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("N9018");
      final EdgeType e = database.getSchema().createEdgeType("E9018");
      e.createProperty("weight", Integer.class);
      e.createProperty("since", Long.class);
      e.createProperty("label", String.class);
    });
  }

  @Test
  void nullDeclaredPropertyReadsBackNull() {
    final RID[] v = new RID[2];
    database.transaction(() -> {
      for (int i = 0; i < v.length; i++)
        v[i] = database.newVertex("N9018").set("id", i).save().getIdentity();
    });

    try (final GraphBatch batch = GraphBatch.builder(database).withLightEdges(false).build()) {
      batch.newEdge(v[0], "E9018", v[1], "weight", null, "note", "hello");
      batch.newEdge(v[0], "E9018", v[1], "since", null, "weight", 7);
      batch.newEdge(v[0], "E9018", v[1], "weight", 7, "label", null);
    }

    final List<String> reported = WarningCapture.captureWarnings(() -> database.transaction(() -> {
      int checked = 0;
      for (final Edge edge : database.lookupByRID(v[0], true).asVertex().getEdges(Vertex.DIRECTION.OUT, "E9018")) {
        if (edge.has("note")) {
          assertThat(edge.get("weight")).isNull();
          assertThat(edge.getString("note")).isEqualTo("hello");
        } else if (edge.has("since")) {
          assertThat(edge.get("since")).isNull();
          assertThat(edge.getInteger("weight")).isEqualTo(7);
        } else {
          assertThat(edge.getInteger("weight")).isEqualTo(7);
          assertThat(edge.get("label")).isNull();
        }
        ++checked;
      }
      assertThat(checked).isEqualTo(3);
    }));
    assertThat(reported).isEmpty();
  }
}
