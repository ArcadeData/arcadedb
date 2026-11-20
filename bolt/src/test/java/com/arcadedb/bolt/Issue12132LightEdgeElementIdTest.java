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
package com.arcadedb.bolt;

import com.arcadedb.bolt.structure.BoltPath;
import com.arcadedb.bolt.structure.BoltRelationship;
import com.arcadedb.bolt.structure.BoltStructureMapper;
import com.arcadedb.bolt.structure.BoltUnboundRelationship;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12132: the element id a Bolt relationship carries for a lightweight edge is the one {@code elementId()} answers, and
 * names that edge, so a driver can send it back in {@code WHERE elementId(r) = $id} without selecting every lightweight edge of
 * the type.
 */
class Issue12132LightEdgeElementIdTest {
  private static final String   DB_PATH = "./target/databases/Issue12132LightEdgeElementIdTest";
  private              Database db;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    db = factory.create();
    db.command("sql", "CREATE VERTEX TYPE V");
    db.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    db.transaction(() -> {
      final MutableVertex a = db.newVertex("V").set("k", "a").save();
      a.newEdge("L", db.newVertex("V").set("k", "b").save());
      a.newEdge("L", db.newVertex("V").set("k", "c").save());
    });
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void aLightweightRelationshipCarriesTheElementIdOfThatEdge() {
    final List<String> elementIds = new ArrayList<>();
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = db.query("opencypher", "MATCH (:V {k:'a'})-[r:L]->() RETURN r, elementId(r) AS e")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Edge edge = row.getProperty("r");
        final BoltRelationship relationship = BoltStructureMapper.toRelationship(edge);
        assertThat(relationship.getElementId()).isEqualTo(row.<String>getProperty("e"));
        assertThat(BoltStructureMapper.toUnboundRelationship(edge).getElementId()).isEqualTo(relationship.getElementId());
        elementIds.add(relationship.getElementId());
        ids.add(relationship.getId());
      }
    }
    assertThat(elementIds).hasSize(2).doesNotHaveDuplicates();
    // the numeric id is synthetic (the edge has no record position) and negative, so it never equals a record's id
    assertThat(ids).hasSize(2).doesNotHaveDuplicates().allMatch(id -> id < 0);
  }

  @Test
  void aLightweightRelationshipOfAPathCarriesTheElementIdOfThatEdge() {
    try (final ResultSet rs = db.query("opencypher", "MATCH p=(:V {k:'a'})-[:L]->(:V) RETURN p")) {
      final List<String> elementIds = new ArrayList<>();
      while (rs.hasNext()) {
        final BoltPath path = BoltStructureMapper.toPath(rs.next().getProperty("p"));
        assertThat(path.getNodes()).hasSize(2);
        assertThat(path.getRelationships()).hasSize(1);
        assertThat(path.getRelationships().getFirst().getId()).isNegative();
        elementIds.add(path.getRelationships().getFirst().getElementId());
      }
      assertThat(elementIds).hasSize(2).doesNotHaveDuplicates();
    }
  }

  @Test
  void twoParallelTwinsWalkedByOnePathStayTwoRelationships() {
    db.transaction(() -> {
      final MutableVertex x = db.newVertex("V").set("k", "x").save();
      final MutableVertex y = db.newVertex("V").set("k", "y").save();
      // two lightweight edges of one type over the same ordered pair: one RID, two relationships
      x.newEdge("L", y);
      x.newEdge("L", y);
    });

    int paths = 0;
    try (final ResultSet rs = db.query("opencypher", "MATCH p=(:V {k:'x'})-[:L]->(:V {k:'y'})<-[:L]-(:V {k:'x'}) RETURN p")) {
      while (rs.hasNext()) {
        final BoltPath path = BoltStructureMapper.toPath(rs.next().getProperty("p"));
        ++paths;
        // the path goes out by one twin and comes back by the other: one node pair, two relationships
        assertThat(path.getNodes()).hasSize(2);
        assertThat(path.getRelationships()).hasSize(2);
        assertThat(path.getRelationships().stream().map(BoltUnboundRelationship::getElementId)).doesNotHaveDuplicates();
        assertThat(path.getIndices()).containsExactly(1L, 1L, -2L, 0L);
      }
    }
    assertThat(paths).isEqualTo(2);
  }
}
