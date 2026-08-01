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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Soundness coverage for lightweight edges ({@link Vertex#newLightEdge}), the property-less edge representation that
 * lives entirely inside the vertex edge-list chunks with no backing record.
 * <p>
 * A lightweight edge is addressed by the RID {@code #<edgeType.firstBucketId>:-1}. That RID is a type marker, not an
 * identity: every lightweight edge of a given type carries the same one. The tests below pin the behaviour that the
 * representation must still provide - distinct identity between distinct edges, type-and-endpoint-accurate removal,
 * and an accurate record counter on the edge type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LightEdgeSoundnessTest extends TestHelper {
  private static final String V     = "V";
  private static final String KNOWS = "Knows";
  private static final String LIKES = "Likes";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName(V).create();
      database.getSchema().buildEdgeType().withName(KNOWS).create();
      database.getSchema().buildEdgeType().withName(LIKES).create();
    });
  }

  /**
   * Two lightweight edges connecting different pairs are different edges, so they must not compare equal nor collapse
   * in a hash container. Every path-uniqueness check in the engine (Cypher relationship uniqueness, the path
   * procedures, {@code SelfLoops}) is a set of edge RIDs, so a shared RID silently truncates traversals.
   */
  @Test
  void distinctLightEdgesHaveDistinctIdentity() {
    final RID[] v = chain(3);

    database.transaction(() -> {
      final Edge first = onlyOutEdge(v[0]);
      final Edge second = onlyOutEdge(v[1]);

      assertThat(first.getIn()).isEqualTo(v[1]);
      assertThat(second.getIn()).isEqualTo(v[2]);

      assertThat(first).as("two lightweight edges over different vertex pairs must not be equal").isNotEqualTo(second);

      final Set<Edge> distinct = new HashSet<>();
      distinct.add(first);
      distinct.add(second);
      assertThat(distinct).as("distinct lightweight edges must not collapse in a hash set").hasSize(2);
    });
  }

  /**
   * A two-hop pattern over a lightweight chain must return the same rows as over a regular chain. Cypher rejects a
   * relationship reused inside one MATCH clause, and it identifies relationships by RID.
   */
  @Test
  void cypherTwoHopPatternTraversesLightweightChain() {
    chain(3);

    assertThat(ids("cypher", "MATCH (a:V {id:0})-[:Knows]->(b)-[:Knows]->(c) RETURN c.id AS id"))
        .as("a 2-hop pattern over a lightweight chain").containsExactly(2);
  }

  /**
   * Same for a variable-length pattern: every reachable vertex must be produced, not just the first hop.
   */
  @Test
  void cypherVariableLengthPathTraversesLightweightChain() {
    chain(4);

    assertThat(ids("cypher", "MATCH (a:V {id:0})-[:Knows*1..3]->(x) RETURN x.id AS id ORDER BY id"))
        .as("variable-length expansion over a lightweight chain").containsExactly(1, 2, 3);
  }

  /**
   * Removing a lightweight edge must remove that edge. It is matched by destination vertex alone, so any other edge
   * reaching the same neighbour - here a regular edge of another type, created after it - is removed instead, leaving
   * its record alive but unreachable from either endpoint.
   */
  @Test
  void deletingALightEdgeLeavesAnEdgeOfAnotherTypeAlone() {
    final RID a = newVertex(0);
    final RID b = newVertex(1);

    final RID[] regular = new RID[1];
    database.transaction(() -> {
      final MutableVertex source = database.lookupByRID(a, true).asVertex().modify();
      source.newLightEdge(KNOWS, b);
      regular[0] = source.newEdge(LIKES, b, "since", 2020).getIdentity();
    });

    assertThat(outEdges(a)).as("precondition").containsExactlyInAnyOrder(KNOWS + "/light", LIKES + "/regular");

    database.transaction(() -> {
      for (final Edge e : database.lookupByRID(a, true).asVertex().getEdges(Vertex.DIRECTION.OUT))
        if (e instanceof LightEdge) {
          e.delete();
          break;
        }
    });

    assertThat(outEdges(a)).as("only the lightweight edge must be gone from the outgoing list")
        .containsExactly(LIKES + "/regular");
    assertThat(inEdges(b)).as("only the lightweight edge must be gone from the incoming list")
        .containsExactly(LIKES + "/regular");
    assertThat(recordExists(regular[0])).as("the regular edge record must still exist").isTrue();
  }

  /**
   * Same shape with two lightweight edges of different types: deleting the {@code Knows} one must leave {@code Likes}.
   */
  @Test
  void deletingALightEdgeLeavesALightEdgeOfAnotherTypeAlone() {
    final RID a = newVertex(0);
    final RID b = newVertex(1);

    database.transaction(() -> {
      final MutableVertex source = database.lookupByRID(a, true).asVertex().modify();
      source.newLightEdge(KNOWS, b);
      source.newLightEdge(LIKES, b);
    });

    database.transaction(() -> {
      for (final Edge e : database.lookupByRID(a, true).asVertex().getEdges(Vertex.DIRECTION.OUT))
        if (KNOWS.equals(e.getTypeName())) {
          e.delete();
          break;
        }
    });

    assertThat(outEdges(a)).containsExactly(LIKES + "/light");
  }

  /**
   * A lightweight edge has no record, so deleting one must not move the edge type's record counter. The counter backs
   * the O(1) {@code count(*)} and is persisted in {@code statistics.json}, so a drift survives a reopen.
   */
  @Test
  void deletingLightEdgesDoesNotDriftTheEdgeTypeRecordCounter() {
    final RID a = newVertex(0);
    final RID b = newVertex(1);

    database.transaction(() -> {
      final MutableVertex source = database.lookupByRID(a, true).asVertex().modify();
      source.newEdge(KNOWS, b, "n", 1);
      source.newEdge(KNOWS, b, "n", 2);
      source.newEdge(KNOWS, b, "n", 3);
      source.newLightEdge(KNOWS, b);
      source.newLightEdge(KNOWS, b);
    });

    assertThat(count("select count(*) as c from " + KNOWS)).isEqualTo(3);

    database.transaction(() -> {
      int deleted = 0;
      for (final Edge e : database.lookupByRID(a, true).asVertex().getEdges(Vertex.DIRECTION.OUT))
        if (e instanceof LightEdge) {
          e.delete();
          if (++deleted == 2)
            break;
        }
    });

    assertThat(count("select count(@rid) as c from " + KNOWS)).as("ground truth: the 3 regular edges are untouched")
        .isEqualTo(3);
    assertThat(count("select count(*) as c from " + KNOWS)).as("the cached counter must agree with the scan")
        .isEqualTo(3);

    reopenDatabase();

    assertThat(count("select count(*) as c from " + KNOWS)).as("a drift persisted in statistics.json survives a reopen")
        .isEqualTo(3);
  }

  // ---------------------------------------------------------------- helpers

  /** Builds a lightweight {@code Knows} chain 0 -> 1 -> ... -> n-1 and returns the vertex RIDs. */
  private RID[] chain(final int n) {
    final RID[] rids = new RID[n];
    for (int i = 0; i < n; i++)
      rids[i] = newVertex(i);

    database.transaction(() -> {
      for (int i = 0; i < n - 1; i++) {
        final int index = i;
        database.lookupByRID(rids[index], true).asVertex().modify().newLightEdge(KNOWS, rids[index + 1]);
      }
    });
    return rids;
  }

  private RID newVertex(final int id) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex(V);
      v.set("id", id);
      v.save();
      rid[0] = v.getIdentity();
    });
    return rid[0];
  }

  private Edge onlyOutEdge(final RID vertex) {
    return database.lookupByRID(vertex, true).asVertex().getEdges(Vertex.DIRECTION.OUT).iterator().next();
  }

  private List<String> outEdges(final RID vertex) {
    return describe(vertex, Vertex.DIRECTION.OUT);
  }

  private List<String> inEdges(final RID vertex) {
    return describe(vertex, Vertex.DIRECTION.IN);
  }

  private List<String> describe(final RID vertex, final Vertex.DIRECTION direction) {
    final List<String> found = new ArrayList<>();
    database.transaction(() -> {
      for (final Edge e : database.lookupByRID(vertex, true).asVertex().getEdges(direction))
        found.add(e.getTypeName() + (e instanceof LightEdge ? "/light" : "/regular"));
    });
    return found;
  }

  private boolean recordExists(final RID rid) {
    final boolean[] found = new boolean[1];
    database.transaction(() -> found[0] = database.existsRecord(rid));
    return found[0];
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private List<Integer> ids(final String language, final String query) {
    final List<Integer> found = new ArrayList<>();
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        found.add(((Number) row.getProperty("id")).intValue());
      }
    }
    return found;
  }
}
