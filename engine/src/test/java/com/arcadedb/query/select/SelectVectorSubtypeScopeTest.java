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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code select().fromType(T).nearestTo(..)} answers with records of {@code T} (and of its sub-types, unless
 * {@code polymorphic(false)}) only, the same scope {@code vectorNeighbors('T[p]', ..)} and
 * {@code db.index.vector.queryNodes('T[p]', ..)} search (issue #8958). The index a sub-type inherits from its parent spans the
 * buckets of every sibling, and the native Select API used to search all of them, so a query on one sub-type answered with
 * records of its siblings.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SelectVectorSubtypeScopeTest extends TestHelper {
  private static final float[] ORIGIN = { 0f, 0f };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Parent");
    database.command("sql", "CREATE PROPERTY Parent.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Parent (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");
    database.command("sql", "CREATE VERTEX TYPE Child1 EXTENDS Parent");
    database.command("sql", "CREATE VERTEX TYPE Child2 EXTENDS Parent");
    database.command("sql", "CREATE VERTEX TYPE GrandChild1 EXTENDS Child1");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newVertex("Parent").set("pid", 300 + i, "vector", new float[] { i + 0.25f, 0f }).save();
        database.newVertex("Child1").set("pid", i, "vector", new float[] { i, 0f }).save();
        database.newVertex("Child2").set("pid", 100 + i, "vector", new float[] { i + 0.5f, 0f }).save();
        database.newVertex("GrandChild1").set("pid", 200 + i, "vector", new float[] { i + 0.75f, 0f }).save();
      }
    });
  }

  @Test
  void aSubTypeAnswersWithItsOwnRecordsAndItsSubTypes() {
    assertThat(pids(database.select().fromType("Child1").nearestTo("vector", ORIGIN, 4).vertices())).containsExactly(0, 200, 1,
        201);
  }

  @Test
  void aNonPolymorphicSubTypeAnswersWithItsOwnRecordsOnly() {
    assertThat(pids(database.select().fromType("Child1").polymorphic(false).nearestTo("vector", ORIGIN, 3).vertices()))
        .containsExactly(0, 1, 2);
  }

  @Test
  void aTypeWithNoBucketOfItsOwnAnswersNothingWhenNotPolymorphic() {
    database.getSchema().buildVertexType().withName("Hollow").withSuperType("Parent").withTotalBuckets(0).create();
    database.command("sql", "CREATE VERTEX TYPE Filled EXTENDS Hollow");
    database.transaction(() -> database.newVertex("Filled").set("pid", 400, "vector", new float[] { 0f, 0f }).save());

    assertThat(database.select().fromType("Hollow").polymorphic(false).nearestTo("vector", ORIGIN, 3).vertices()).isEmpty();
    assertThat(pids(database.select().fromType("Hollow").nearestTo("vector", ORIGIN, 3).vertices())).containsExactly(400);
  }

  @Test
  void theParentTypeStillSearchesEverySubType() {
    assertThat(pids(database.select().fromType("Parent").nearestTo("vector", ORIGIN, 4).vertices())).containsExactly(0, 300, 100,
        200);
  }

  private static List<Integer> pids(final List<SelectVectorResult<Vertex>> results) {
    final List<Integer> pids = new ArrayList<>(results.size());
    for (final SelectVectorResult<Vertex> result : results)
      pids.add(result.getDocument().getInteger("pid"));
    return pids;
  }
}
