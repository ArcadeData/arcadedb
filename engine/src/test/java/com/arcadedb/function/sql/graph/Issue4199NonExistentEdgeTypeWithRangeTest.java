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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4199 regression: in 26.4.2 a SELECT that applies a range to {@code bothE()} (or
 * {@code outE()} / {@code inE()}) with a non-existent edge type raised
 * {@code SchemaException("Type with name 'X' was not found")}, while the same query without
 * the range correctly returned an empty list (the behaviour of 26.3.2).
 *
 * Root cause: {@code EdgeLinkedList.count(String... edgeTypes)} resolved each edge-type name
 * via {@code schema.getType(name)} and threw when the type did not exist. {@code count()}
 * is invoked by {@code MultiValue.getSize(...)} during range evaluation, so the range form
 * crashed while the plain iteration form (which uses {@code existsType} + skip) did not.
 *
 * After the fix the range form returns an empty list, matching the non-ranged form and the
 * 26.3.2 behaviour.
 *
 * https://github.com/ArcadeData/arcadedb/issues/4199
 */
class Issue4199NonExistentEdgeTypeWithRangeTest {

  @Test
  void bothEWithRangeOnMissingEdgeTypeReturnsEmpty() throws Exception {
    TestHelper.executeInNewDatabase("testIssue4199BothERange", (db) -> {
      db.getSchema().createVertexType("Node");
      db.getSchema().createEdgeType("KnownEdge");

      final RID[] vertexRid = new RID[1];
      db.transaction(() -> {
        final MutableVertex v1 = db.newVertex("Node").save();
        final MutableVertex v2 = db.newVertex("Node").save();
        v1.newEdge("KnownEdge", v2);
        vertexRid[0] = v1.getIdentity();
      });

      // Without range: returns []
      try (final ResultSet rs = db.query("sql",
          "SELECT bothE('NonExistentEdge') AS edges FROM " + vertexRid[0])) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(MultiValue.getSize(rs.next().getProperty("edges"))).isZero();
      }

      // With range: must also return [] (regression from 26.4.2)
      try (final ResultSet rs = db.query("sql",
          "SELECT bothE('NonExistentEdge')[0...25] AS edges FROM " + vertexRid[0])) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(MultiValue.getSize(rs.next().getProperty("edges"))).isZero();
      }
    });
  }

  @Test
  void outEWithRangeOnMissingEdgeTypeReturnsEmpty() throws Exception {
    TestHelper.executeInNewDatabase("testIssue4199OutERange", (db) -> {
      db.getSchema().createVertexType("Node");
      db.getSchema().createEdgeType("KnownEdge");

      final RID[] vertexRid = new RID[1];
      db.transaction(() -> {
        final MutableVertex v1 = db.newVertex("Node").save();
        final MutableVertex v2 = db.newVertex("Node").save();
        v1.newEdge("KnownEdge", v2);
        vertexRid[0] = v1.getIdentity();
      });

      try (final ResultSet rs = db.query("sql",
          "SELECT outE('NonExistentEdge')[0...25] AS edges FROM " + vertexRid[0])) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(MultiValue.getSize(rs.next().getProperty("edges"))).isZero();
      }
    });
  }

  @Test
  void inEWithRangeOnMissingEdgeTypeReturnsEmpty() throws Exception {
    TestHelper.executeInNewDatabase("testIssue4199InERange", (db) -> {
      db.getSchema().createVertexType("Node");
      db.getSchema().createEdgeType("KnownEdge");

      final RID[] vertexRid = new RID[1];
      db.transaction(() -> {
        final MutableVertex v1 = db.newVertex("Node").save();
        final MutableVertex v2 = db.newVertex("Node").save();
        v1.newEdge("KnownEdge", v2);
        vertexRid[0] = v2.getIdentity();
      });

      try (final ResultSet rs = db.query("sql",
          "SELECT inE('NonExistentEdge')[0...25] AS edges FROM " + vertexRid[0])) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(MultiValue.getSize(rs.next().getProperty("edges"))).isZero();
      }
    });
  }

  @Test
  void bothEWithRangeMixingKnownAndMissingTypesReturnsKnownOnly() throws Exception {
    TestHelper.executeInNewDatabase("testIssue4199BothEMixed", (db) -> {
      db.getSchema().createVertexType("Node");
      db.getSchema().createEdgeType("KnownEdge");

      final RID[] vertexRid = new RID[1];
      db.transaction(() -> {
        final MutableVertex v1 = db.newVertex("Node").save();
        final MutableVertex v2 = db.newVertex("Node").save();
        v1.newEdge("KnownEdge", v2);
        vertexRid[0] = v1.getIdentity();
      });

      try (final ResultSet rs = db.query("sql",
          "SELECT bothE('KnownEdge','NonExistentEdge')[0...25] AS edges FROM " + vertexRid[0])) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(MultiValue.getSize(rs.next().getProperty("edges"))).isEqualTo(1);
      }
    });
  }
}
