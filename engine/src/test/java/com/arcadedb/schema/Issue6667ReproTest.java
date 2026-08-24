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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Standalone reproduction of the exact scenario reported in https://github.com/ArcadeData/arcadedb/issues/6667:
 * a vertex type rename must not corrupt the type's edge-chunk bucket names, or an edge insert on the renamed
 * type afterward fails with SchemaException. Kept separate from {@link TypeRenameComponentNamingTest} because it
 * mirrors the issue's own reproducer verbatim, including the restart step the issue's suggested fix #4 calls
 * out ("insert an edge, restart, insert another edge, and assert the bucket names follow the convention").
 * <p>
 * Extends {@link TestHelper} so {@code afterTest()} runs {@code CHECK DATABASE} on teardown: the failure mode
 * this test guards against is bucket/file corruption, which a cached record count would not necessarily surface.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6667ReproTest extends TestHelper {

  @Test
  void renamedVertexTypeCanStillInsertEdges() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("Knows");

    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("Person").set("uid", "a").save();
      final MutableVertex v2 = database.newVertex("Person").set("uid", "b").save();
      v1.newEdge("Knows", v2).save();
    });

    database.getSchema().getType("Person").rename("Human");

    database.transaction(() -> {
      final MutableVertex v3 = database.newVertex("Human").set("uid", "c").save();
      final MutableVertex v4 = database.newVertex("Human").set("uid", "d").save();
      v3.newEdge("Knows", v4).save();
    });

    assertThat(database.query("sql", "select from Human").stream().count()).isEqualTo(4L);

    final String databasePath = database.getDatabasePath();
    database.close();
    database = new DatabaseFactory(databasePath).open();

    database.transaction(() -> {
      final MutableVertex v5 = database.newVertex("Human").set("uid", "e").save();
      final MutableVertex v6 = database.newVertex("Human").set("uid", "f").save();
      v5.newEdge("Knows", v6).save();
    });

    assertThat(database.query("sql", "select from Human").stream().count()).isEqualTo(6L);

    final var edgeBuckets = database.getSchema().getType("Human").getInvolvedBuckets().stream()
        .map(b -> b.getName()).filter(n -> n.endsWith("_out_edges") || n.endsWith("_in_edges")).toList();
    assertThat(edgeBuckets).containsExactlyInAnyOrder("Human_0_out_edges", "Human_0_in_edges");
  }
}
