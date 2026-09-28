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
package com.arcadedb.gremlin;

import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8439: {@code has(key, value)} must not be pushed down to a FULL_TEXT index, which answers by analyzer token and
 * finds nothing for a value without tokens: {@code has('k', '--')} returned no vertex although one carries it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinHasFullTextIndexExactTest {
  private static final String[] VALUES = { "a b", "a", "-a", "x:a", "--" };

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-8439-fulltext");
    final DocumentType node = graph.getDatabase().getSchema().createVertexType("Node");
    node.createProperty("k", Type.STRING);
    node.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "k");
    graph.getDatabase().transaction(() -> {
      for (final String v : VALUES)
        graph.addVertex("Node").property("k", v);
    });
  }

  @AfterEach
  void teardown() {
    graph.drop();
  }

  @Test
  void hasIsExactOnAFullTextIndexedProperty() {
    for (final String v : VALUES)
      assertThat(graph.traversal().V().hasLabel("Node").has("k", v).count().next()).as("has('k', '%s')", v).isEqualTo(1L);
  }
}
