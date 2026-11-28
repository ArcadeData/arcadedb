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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.LightEdgeRID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests of {@link DeletedEntityMarker}: what a marker keeps of the entity it stands for, and when two markers are equal
 * (issue #13582).
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class DeletedEntityMarkerTest extends TestHelper {
  private Vertex a;
  private Vertex b;
  private Edge   ab;

  @BeforeEach
  void createRecords() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      final MutableVertex va = database.newVertex("V").save();
      final MutableVertex vb = database.newVertex("V").save();
      a = va;
      b = vb;
      ab = va.newEdge("E", vb).save();
    });
  }

  @Test
  void aNodeMarkerKeepsItsIdentityAndNoType() {
    final DeletedEntityMarker marker = DeletedEntityMarker.of(a);
    assertThat(marker.getIdentity()).isEqualTo(a.getIdentity());
    assertThat(marker.getRelationshipType()).isNull();
  }

  @Test
  void aRelationshipMarkerKeepsItsIdentityAndType() {
    final DeletedEntityMarker marker = DeletedEntityMarker.of(ab);
    assertThat(marker.getIdentity()).isEqualTo(ab.getIdentity());
    assertThat(marker.getRelationshipType()).isEqualTo("E");
  }

  @Test
  void aResultWrappingARecordIsMarkedAsTheRecord() {
    assertThat(DeletedEntityMarker.of(new ResultInternal(a))).isEqualTo(DeletedEntityMarker.of(a));
    assertThat(DeletedEntityMarker.of(new ResultInternal(ab)).getRelationshipType()).isEqualTo("E");
  }

  @Test
  void aValueThatIsNoEntityIsMarkedWithTheSharedInstance() {
    assertThat(DeletedEntityMarker.of(new TraversalPath(a))).isSameAs(DeletedEntityMarker.INSTANCE);
    assertThat(DeletedEntityMarker.of(List.of(a))).isSameAs(DeletedEntityMarker.INSTANCE);
    assertThat(DeletedEntityMarker.of(Map.of("k", 1))).isSameAs(DeletedEntityMarker.INSTANCE);
    assertThat(DeletedEntityMarker.of(new ResultInternal(Map.of("k", 1)))).isSameAs(DeletedEntityMarker.INSTANCE);
    assertThat(DeletedEntityMarker.of(null)).isSameAs(DeletedEntityMarker.INSTANCE);
    assertThat(DeletedEntityMarker.INSTANCE.getIdentity()).isNull();
  }

  @Test
  void aMarkerIsMarkedAsItself() {
    final DeletedEntityMarker marker = DeletedEntityMarker.of(a);
    assertThat(DeletedEntityMarker.of(marker)).isSameAs(marker);
  }

  @Test
  void markersOfTheSameRecordAreEqual() {
    assertThat(DeletedEntityMarker.of(a)).isEqualTo(DeletedEntityMarker.of(a));
    assertThat(DeletedEntityMarker.of(a).hashCode()).isEqualTo(DeletedEntityMarker.of(a).hashCode());
    assertThat(DeletedEntityMarker.of(ab)).isEqualTo(DeletedEntityMarker.of(ab));
  }

  @Test
  void markersOfDifferentRecordsAreNotEqual() {
    assertThat(DeletedEntityMarker.of(a)).isNotEqualTo(DeletedEntityMarker.of(b));
    assertThat(DeletedEntityMarker.of(a)).isNotEqualTo(DeletedEntityMarker.of(ab));
    // same RID, one read as a node and one as a relationship: not the same entity
    assertThat(new DeletedEntityMarker(a.getIdentity(), null)).isNotEqualTo(new DeletedEntityMarker(a.getIdentity(), "E"));
  }

  @Test
  void aMarkerWithNoIdentityEqualsOnlyItself() {
    final DeletedEntityMarker first = new DeletedEntityMarker(null, null);
    final DeletedEntityMarker second = new DeletedEntityMarker(null, null);
    assertThat(first).isEqualTo(first);
    assertThat(first).isNotEqualTo(second);
    assertThat(DeletedEntityMarker.INSTANCE).isNotEqualTo(first);
  }

  @Test
  void markersOfLightweightEdgesEqualOnlyThemselves() {
    // every lightweight edge of a type shares the type marker RID #bucket:-1: it cannot tell two of them apart
    final RID shared = new LightEdgeRID(database, 3, a.getIdentity(), b.getIdentity());
    assertThat(shared.getPosition()).isNegative();
    final DeletedEntityMarker first = new DeletedEntityMarker(shared, "E");
    final DeletedEntityMarker second = new DeletedEntityMarker(shared, "E");
    assertThat(first).isEqualTo(first);
    assertThat(first).isNotEqualTo(second);
  }
}
