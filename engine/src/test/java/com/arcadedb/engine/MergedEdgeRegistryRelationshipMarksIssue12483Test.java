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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12483: the commit of a relationship of a type with an exact-key index marks the {@link MergedEdgeRegistry} key a MERGE
 * answered by that index checks, with the values the commit writes, whatever statement or API call created it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MergedEdgeRegistryRelationshipMarksIssue12483Test extends TestHelper {
  private RID a;
  private RID b;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1);
      database.getSchema().createEdgeType("E", 1).createProperty("uuid", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "E", "uuid");
      database.getSchema().createEdgeType("Plain", 1).createProperty("uuid", Type.STRING);
      a = database.newVertex("V").save().getIdentity();
      b = database.newVertex("V").save().getIdentity();
    });
  }

  /**
   * A relationship changed after its save and never saved again is committed with what the save wrote: that is the key marked, not
   * the one only the object holds.
   */
  @Test
  void aRelationshipChangedAfterItsSaveIsMarkedWithTheKeyTheCommitWrites() {
    final long since = registry().getSequence();
    database.transaction(() -> {
      final MutableEdge edge = ((Vertex) database.lookupByRID(a, true)).newEdge("E", b, "uuid", "x");
      edge.set("uuid", "other");
    });

    assertThat(database.query("sql", "SELECT uuid FROM E").next().<String>getProperty("uuid")).isEqualTo("x");
    assertThat(registry().isCommittedSince(keyOf("E", "x"), since)).isTrue();
    assertThat(registry().isCommittedSince(keyOf("E", "other"), since)).isFalse();
  }

  /** The key a SET after the creation gives, saved, is the one the commit writes and marks. */
  @Test
  void aRelationshipGivenItsKeyAfterItsCreationAndSavedIsMarkedWithIt() {
    final long since = registry().getSequence();
    database.transaction(() -> {
      final MutableEdge edge = ((Vertex) database.lookupByRID(a, true)).newEdge("E", b);
      edge.set("uuid", "y").save();
    });

    assertThat(registry().isCommittedSince(keyOf("E", "y"), since)).isTrue();
  }

  /** A type with no exact-key index marks nothing: no MERGE answers its existence check from an index. */
  @Test
  void aRelationshipOfATypeWithoutAnIndexMarksNothing() {
    final long since = registry().getSequence();
    database.transaction(() -> ((Vertex) database.lookupByRID(a, true)).newEdge("Plain", b, "uuid", "z"));

    assertThat(registry().isCommittedSince(keyOf("Plain", "z"), since)).isFalse();
    assertThat(registry().getSequence()).isEqualTo(since);
  }

  private long keyOf(final String type, final String uuid) {
    return MergedEdgeRegistry.keyOf(a, b, type, Map.of("uuid", uuid));
  }

  private MergedEdgeRegistry registry() {
    return ((DatabaseInternal) database).getTransactionManager().getMergedEdges();
  }
}
