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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link EdgeType#holdsLightweightEdges}: the question every read path that resolves edges by type name has to ask
 * before trusting a record count or a bucket scan (issues #5071, #7477). It is asked of every SELECT target, so it
 * has to answer a vertex or document type without walking its hierarchy at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EdgeTypeHoldsLightweightEdgesTest extends TestHelper {

  @Test
  void aLightweightEdgeTypeAnswersYesAndARegularOneNo() {
    database.transaction(() -> {
      database.getSchema().buildEdgeType().withName("Cite").withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("Wrote").create();
    });

    assertThat(EdgeType.holdsLightweightEdges(database.getSchema().getType("Cite"))).isTrue();
    assertThat(EdgeType.holdsLightweightEdges(database.getSchema().getType("Wrote"))).isFalse();
  }

  /** A scan of the supertype returns the subtype's edges, so the supertype has to answer for them. */
  @Test
  void aSupertypeAnswersForALightweightSubtype() {
    database.transaction(() -> {
      database.getSchema().buildEdgeType().withName("Mentions").create();
      database.getSchema().buildEdgeType().withName("Quotes").withSuperType("Mentions").withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("Cites").withSuperType("Mentions").create();
    });

    assertThat(EdgeType.holdsLightweightEdges(database.getSchema().getType("Mentions"))).isTrue();
    assertThat(EdgeType.holdsLightweightEdges(database.getSchema().getType("Quotes"))).isTrue();
    assertThat(EdgeType.holdsLightweightEdges(database.getSchema().getType("Cites")))
        .as("a sibling of the lightweight subtype is not itself lightweight").isFalse();
  }

  /**
   * Vertex, edge and document hierarchies are disjoint, so nothing under a non-edge root can be a lightweight edge.
   * Answering that without walking the hierarchy is what keeps the check off the planning hot path.
   */
  @Test
  void aNonEdgeTypeAnswersNoWhateverItsHierarchyHolds() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("Work").create();
      database.getSchema().buildVertexType().withName("Article").withSuperType("Work").create();
      database.getSchema().buildDocumentType().withName("Note").create();
    });

    assertThat(EdgeType.holdsLightweightEdges(database.getSchema().getType("Work"))).isFalse();
    assertThat(EdgeType.holdsLightweightEdges(database.getSchema().getType("Article"))).isFalse();
    assertThat(EdgeType.holdsLightweightEdges(database.getSchema().getType("Note"))).isFalse();
  }

  @Test
  void nothingAnswersNo() {
    assertThat(EdgeType.holdsLightweightEdges(null)).isFalse();
  }
}
