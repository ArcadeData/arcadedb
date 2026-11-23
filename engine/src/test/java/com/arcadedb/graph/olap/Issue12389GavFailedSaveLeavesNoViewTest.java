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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12389: the builder registered a named view before saving its definition, and nothing took the
 * registration back when the save failed (a replica refusing the schema change, a user without {@code UPDATE_SCHEMA}, an
 * I/O error). The database was left with a live view that answered queries until the next open and was then gone, its
 * definition stored nowhere. A failed save now leaves no view behind, under the name nor as a traversal provider.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12389GavFailedSaveLeavesNoViewTest extends TestHelper {
  private static final String REFUSAL = "issue 12389: the definition cannot be saved";

  @BeforeEach
  void setupGraph() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("K");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      a.newEdge("K", b);
    });
  }

  @Test
  void aFailedSaveOfBuildLeavesNoView() {
    assertFailedSaveLeavesNoView("gvSync", GraphAnalyticalViewBuilder::build);
  }

  @Test
  void aFailedSaveOfBuildAsyncLeavesNoView() {
    assertFailedSaveLeavesNoView("gvAsync", GraphAnalyticalViewBuilder::buildAsync);
  }

  @Test
  void aNameAlreadyTakenKeepsTheViewThatHoldsIt() {
    final GraphAnalyticalView first = GraphAnalyticalView.builder(database).withName("gvTaken").build();
    try {
      assertThatThrownBy(() -> GraphAnalyticalView.builder(database).withName("gvTaken").withEdgeTypes("K").build())
          .isInstanceOf(AlreadyExistsException.class);
      assertThat(GraphAnalyticalViewRegistry.get(database, "gvTaken")).isSameAs(first);
      // the refused builder did not overwrite the definition of the view that holds the name
      assertThat(GraphAnalyticalViewPersistence.getDefinitions(database)).containsKey("gvTaken");
      assertThat(GraphAnalyticalViewPersistence.toJSON(first).toString())
          .isEqualTo(database.getSchema().getExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY).getJSONObject("gvTaken")
              .toString());
    } finally {
      first.drop();
    }
  }

  @Test
  void theViewThatFailedDoesNotFreeANameAnotherViewHolds() {
    final GraphAnalyticalView holder = GraphAnalyticalView.builder(database).withName("gvHeld").build();
    try {
      // a view of the same name that is not the registered one (the old instance of a name built again) shuts down
      final GraphAnalyticalView stray = new GraphAnalyticalView(database, "gvHeld", null, null, null, null,
          GraphAnalyticalView.UpdateMode.OFF);
      stray.shutdown();
      assertThat(GraphAnalyticalViewRegistry.get(database, "gvHeld")).isSameAs(holder);
    } finally {
      holder.drop();
    }
  }

  private void assertFailedSaveLeavesNoView(final String name, final Function<GraphAnalyticalViewBuilder, GraphAnalyticalView> build) {
    final int providersBefore = GraphTraversalProviderRegistry.getProviders(database).size();
    // the save of the definition is refused, as an HA replica refuses a schema change (schemaChangesNeedTheLeader)
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    local.refuseWrites(REFUSAL, true);
    try {
      assertThatThrownBy(() -> build.apply(GraphAnalyticalView.builder(database).withName(name)))
          .isInstanceOf(DatabaseNotAvailableException.class);
    } finally {
      local.acceptWrites(REFUSAL);
    }

    assertThat(GraphAnalyticalViewRegistry.get(database, name)).as("no view is left registered under the name").isNull();
    assertThat(GraphAnalyticalViewPersistence.getDefinitions(database)).doesNotContainKey(name);
    assertThat(GraphTraversalProviderRegistry.getProviders(database)).as("no view is left serving traversals")
        .hasSize(providersBefore);

    // the name is free: the same build succeeds once the save is accepted
    final GraphAnalyticalView view = build.apply(GraphAnalyticalView.builder(database).withName(name));
    try {
      assertThat(GraphAnalyticalViewRegistry.get(database, name)).isSameAs(view);
      assertThat(GraphAnalyticalViewPersistence.getDefinitions(database)).containsKey(name);
    } finally {
      view.drop();
    }
  }
}
