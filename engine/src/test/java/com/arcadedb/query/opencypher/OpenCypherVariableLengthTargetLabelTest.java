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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The node pattern at the far end of a variable-length relationship is filtered by the expansion
 * step itself, not by a scan step, so it needs the same label semantics a scanned node gets:
 * disjunction {@code (n:A|B)} matches any label, conjunction {@code (n:A:B)} matches all of them,
 * and a Cypher 25 dynamic {@code $(expression)} label is resolved per row.
 * <p>
 * Both written orientations are covered, because the indexed-anchor bridge reverses the traversal
 * and then evaluates the written source node as the expansion target.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherVariableLengthTargetLabelTest {
  private static final String DATABASE_PATH = "./target/databases/cypher-vlp-target-label";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
    database.getSchema().createVertexType("Special").addSuperType("Node");
    database.getSchema().createVertexType("Other").addSuperType("Node");
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Node").set("id", "hub").save();
      for (final String type : List.of("Node", "Special", "Other")) {
        final MutableVertex leaf = database.newVertex(type).set("id", type.toLowerCase()).save();
        leaf.newEdge("LINK", hub, true, (Object[]) null).save();
      }
      for (int i = 0; i < 128; i++)
        database.newVertex("Node").set("id", "decoy-" + i).save();
    });

    database.transaction(() -> database.getSchema().createTypeIndex(
        Schema.INDEX_TYPE.LSM_TREE, true, "Node", "id"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void labelDisjunctionOnExpansionTargetMatchesAnyLabel() {
    assertThat(ids("MATCH (hub:Node)<-[:LINK*1..2]-(n:Special|Other) WHERE hub.id = 'hub' RETURN n.id AS id ORDER BY id"))
        .containsExactly("other", "special");
  }

  @Test
  void labelDisjunctionOnReversedExpansionTargetMatchesAnyLabel() {
    assertThat(ids("MATCH (n:Special|Other)-[:LINK*1..2]->(hub:Node) WHERE hub.id = 'hub' RETURN n.id AS id ORDER BY id"))
        .containsExactly("other", "special");
  }

  @Test
  void labelConjunctionOnExpansionTargetRequiresEveryLabel() {
    assertThat(ids("MATCH (hub:Node)<-[:LINK*1..2]-(n:Node) WHERE hub.id = 'hub' RETURN n.id AS id ORDER BY id"))
        .containsExactly("node", "other", "special");
    assertThat(ids("MATCH (hub:Node)<-[:LINK*1..2]-(n:Special) WHERE hub.id = 'hub' RETURN n.id AS id ORDER BY id"))
        .containsExactly("special");
  }

  @Test
  void dynamicLabelOnExpansionTargetIsResolved() {
    assertThat(ids("MATCH (hub:Node)<-[:LINK*1..2]-(n:Node:$('Special')) WHERE hub.id = 'hub' RETURN n.id AS id ORDER BY id"))
        .containsExactly("special");
  }

  @Test
  void dynamicLabelOnReversedExpansionTargetIsResolved() {
    assertThat(ids("MATCH (n:Node:$('Special'))-[:LINK*1..2]->(hub:Node) WHERE hub.id = 'hub' RETURN n.id AS id ORDER BY id"))
        .containsExactly("special");
  }

  private List<String> ids(final String query) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        ids.add(resultSet.next().getProperty("id"));
    }
    return ids;
  }
}
