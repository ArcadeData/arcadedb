/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #9573: two lightweight edges of the same type over the same ordered pair of vertices (two
 * parallel light self loops included) shared one identity, so the openCypher row pipeline treated a path walking one
 * and then the other as reusing a relationship and dropped it, and {@code id(r)} of a lightweight edge threw.
 * <p>
 * The expected counts are enumerated by hand, each stored edge being a relationship of its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9573ParallelLightEdgesTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  /** a has two parallel light self loops and a regular edge to b. */
  private void loopsAndRegularEdge() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("name", "a").save();
      final MutableVertex b = database.newVertex("P").set("name", "b").save();
      a.newLightEdge("K", a);
      a.newLightEdge("K", a);
      a.newEdge("K", b);
    });
  }

  @Test
  void rowPipelineCountsEachParallelLoopAsARelationship() {
    loopsAndRegularEdge();
    // from a: (loop1, a->b), (loop2, a->b); from b: (a->b, loop1), (a->b, loop2)
    assertThat(count("MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c WITH * RETURN count(*) AS n")).isEqualTo(4L);
  }

  @Test
  void rowPipelineWithoutTheInequality() {
    loopsAndRegularEdge();
    // each stored edge is a relationship of its own: the six paths are a-a-a (l1,l2) (l2,l1), a-a-b (l1,ab) (l2,ab) and
    // b-a-a (ab,l1) (ab,l2)
    assertThat(count("MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WITH * RETURN count(*) AS n")).isEqualTo(6L);
  }

  @Test
  void aTwoHopPathOverTwoParallelLoopsWalksEachLoopOnce() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      a.newLightEdge("K", a);
      a.newLightEdge("K", a);
    });
    // r1 and r2 are different relationships: (l1,l2) and (l2,l1) are the two paths, (l1,l1) and (l2,l2) reuse one
    assertThat(count("MATCH (a:P)-[r1:K]->(b:P)-[r2:K]->(c:P) WITH * RETURN count(*) AS n")).isEqualTo(2L);
    assertThat(count("MATCH (a:P)-[r1:K]-(b:P)-[r2:K]-(c:P) WITH * RETURN count(*) AS n")).isEqualTo(2L);
  }

  @Test
  void parallelEdgesBetweenTwoVertices() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      a.newLightEdge("K", b);
      a.newLightEdge("K", b);
    });
    // a->b by e1 or e2 is one hop each; a path must not walk the same edge back, but may walk the other one
    assertThat(count("MATCH (x:P)-[r1:K]-(y:P)-[r2:K]-(z:P) WITH * RETURN count(*) AS n")).isEqualTo(4L);
    assertThat(count("MATCH (x:P)-[r1:K]-(y:P)-[r2:K]-(z:P) RETURN count(*) AS n")).isEqualTo(4L);
    // one hop sees both edges
    assertThat(count("MATCH (x:P)-[r:K]->(y:P) WITH * RETURN count(*) AS n")).isEqualTo(2L);
  }

  @Test
  void parallelSelfLoopsAreEachSeenOnceInBothDirections() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      a.newLightEdge("K", a);
      a.newLightEdge("K", a);
    });
    assertThat(count("MATCH (a:P)-[r:K]-(b:P) WITH * RETURN count(*) AS n")).isEqualTo(2L);
    assertThat(count("MATCH (a:P)-[r:K]->(b:P) WITH * RETURN count(*) AS n")).isEqualTo(2L);
  }

  @Test
  void twinsStayCountedAfterADeleteAndAnAppendReorderTheLists() {
    final MutableVertex[] ends = new MutableVertex[2];
    database.transaction(() -> {
      ends[0] = database.newVertex("P").save();
      ends[1] = database.newVertex("P").save();
      ends[0].newLightEdge("K", ends[1]);
      ends[0].newLightEdge("K", ends[1]);
      ends[0].newLightEdge("K", ends[1]);
    });
    // drop one copy, then add one back: the entries of the two lists are no longer in the order they were appended
    database.transaction(() -> {
      final MutableVertex a = database.lookupByRID(ends[0].getIdentity(), true).asVertex().modify();
      a.getEdges(com.arcadedb.graph.Vertex.DIRECTION.OUT, "K").iterator().next().delete();
      a.newLightEdge("K", database.lookupByRID(ends[1].getIdentity(), true).asVertex());
    });
    assertThat(count("MATCH (x:P)-[r1:K]->(y:P) WITH * RETURN count(*) AS n")).isEqualTo(3L);
    assertThat(count("MATCH (x:P)<-[r1:K]-(y:P) WITH * RETURN count(*) AS n")).isEqualTo(3L);
    assertThat(count("MATCH (x:P)-[r1:K]->(y:P)<-[r2:K]-(z:P) WITH * RETURN count(*) AS n")).isEqualTo(6L);
    assertThat(count("MATCH (x:P)-[r1:K]->(y:P)-[r2:K]->(z:P) WITH * RETURN count(*) AS n")).isEqualTo(0L);
    // three copies between a and b: ordered pairs of distinct copies, from either end = 3 * 2 * 2
    assertThat(count("MATCH (x:P)-[r1:K]-(y:P)-[r2:K]-(z:P) WITH * RETURN count(*) AS n")).isEqualTo(12L);
  }

  @Test
  void aSingleLightEdgeIsStillNotReused() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      a.newLightEdge("K", b);
    });
    assertThat(count("MATCH (x:P)-[r1:K]-(y:P)-[r2:K]-(z:P) WITH * RETURN count(*) AS n")).isEqualTo(0L);
  }

  @Test
  void parallelLoopsAfterReopen() {
    loopsAndRegularEdge();
    reopenDatabase();
    assertThat(count("MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c WITH * RETURN count(*) AS n")).isEqualTo(4L);
  }

  @Test
  void parallelEdgesOnAUnidirectionalTypeWalkedBackwards() {
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL LIGHTWEIGHT");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("name", "a").save();
      final MutableVertex b = database.newVertex("P").set("name", "b").save();
      final MutableVertex c = database.newVertex("P").set("name", "c").save();
      a.newLightEdge("U", b);
      a.newLightEdge("U", b);
      c.newLightEdge("U", b);
    });
    // x<-r1-b: three incoming edges, r2 out of b backwards: pairs of distinct edges among the three: 3 * 2
    assertThat(count("MATCH (x:P)-[r1:U]->(b:P {name:'b'})<-[r2:U]-(y:P) WITH * RETURN count(*) AS n")).isEqualTo(6L);
    assertThat(count("MATCH (x:P)-[r1:U]->(b:P {name:'b'})<-[r2:U]-(y:P) WITH * WHERE x = y RETURN count(*) AS n")).isEqualTo(2L);
  }

  @Test
  void parallelEdgesWalkedAgainstTheirDirection() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("name", "a").save();
      final MutableVertex b = database.newVertex("P").set("name", "b").save();
      a.newLightEdge("K", b);
      a.newLightEdge("K", b);
      a.newLightEdge("K", b);
    });
    // a->b by one edge, then back from b to a by another one: 3 * 2
    assertThat(count("MATCH (x:P {name:'a'})-[r1:K]->(y:P)<-[r2:K]-(z:P) WITH * RETURN count(*) AS n")).isEqualTo(6L);
  }

  @Test
  void idOfALightEdgeIsNullAndOfARegularEdgeIsNot() {
    loopsAndRegularEdge();
    final List<Object> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:P)-[r:K]->(b:P) RETURN id(r) AS i, elementId(r) AS e")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        ids.add(row.getProperty("i"));
        assertThat((String) row.getProperty("e")).isNotNull();
      }
    }
    assertThat(ids).hasSize(3);
    assertThat(ids.stream().filter(i -> i == null)).hasSize(2);
    assertThat(ids.stream().filter(i -> i instanceof Long l && l >= 0)).hasSize(1);
  }
}
