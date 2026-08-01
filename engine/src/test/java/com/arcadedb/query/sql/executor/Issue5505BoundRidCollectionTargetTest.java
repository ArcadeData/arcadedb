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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A collection of RIDs bound as a query parameter must be usable as a target for both SELECT and
 * TRAVERSE. Both planners used to build the per-element {@code Rid} AST node without flagging it as
 * legacy, so {@code Rid.toRecordId} dereferenced a null expression instead of reading bucket/position.
 * A singleton bound RID always worked, so the collection form is what the tests bind here.
 */
class Issue5505BoundRidCollectionTargetTest extends TestHelper {

  @Test
  void selectFromBoundRidCollection() {
    database.transaction(() -> {
      final List<RID> seeds = createChain();

      final ResultSet result = database.query("sql", "SELECT FROM :seeds", Map.of("seeds", List.of(seeds.get(0), seeds.get(1))));

      final Set<RID> returned = collectRids(result);
      assertThat(returned).containsExactlyInAnyOrder(seeds.get(0), seeds.get(1));
    });
  }

  @Test
  void traverseFromBoundRidCollection() {
    database.transaction(() -> {
      final List<RID> seeds = createChain();

      final ResultSet result = database.query("sql", "SELECT FROM (TRAVERSE out('" + EDGE + "') FROM :seeds MAXDEPTH 1)",
          Map.of("seeds", List.of(seeds.get(0), seeds.get(1))));

      // depth 0: the two seeds; depth 1: v1 (from v0) and v2 (from v1)
      final Set<RID> returned = collectRids(result);
      assertThat(returned).containsExactlyInAnyOrder(seeds.get(0), seeds.get(1), seeds.get(2));
    });
  }

  @Test
  void selectFromSingleElementBoundRidCollection() {
    database.transaction(() -> {
      final List<RID> seeds = createChain();

      final ResultSet result = database.query("sql", "SELECT FROM :seeds", Map.of("seeds", List.of(seeds.getFirst())));

      final Set<RID> returned = collectRids(result);
      assertThat(returned).containsExactly(seeds.getFirst());
    });
  }

  private static final String VERTEX = "Issue5505V";
  private static final String EDGE    = "Issue5505E";

  /**
   * Builds v0 -> v1 -> v2 and returns their RIDs in order.
   */
  private List<RID> createChain() {
    database.getSchema().createVertexType(VERTEX);
    database.getSchema().createEdgeType(EDGE);

    final MutableVertex v0 = database.newVertex(VERTEX).set("name", "v0").save();
    final MutableVertex v1 = database.newVertex(VERTEX).set("name", "v1").save();
    final MutableVertex v2 = database.newVertex(VERTEX).set("name", "v2").save();
    v0.newEdge(EDGE, v1).save();
    v1.newEdge(EDGE, v2).save();

    return List.of(v0.getIdentity(), v1.getIdentity(), v2.getIdentity());
  }

  private static Set<RID> collectRids(final ResultSet result) {
    return result.stream().map(Result::getIdentity).map(java.util.Optional::orElseThrow).collect(Collectors.toSet());
  }
}
