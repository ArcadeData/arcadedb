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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.StripeDirectory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.opencypher.traversal.VariableLengthPathTraverser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cypher traversal over light-weight and parallel edges.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherLightEdgeTest {
  /**
   * Issue #9389: a one-hop {@code count(*)} over an unnamed relationship answered 0 for light edges stored in an edge type that does
   * not declare LIGHTWEIGHT, because the planner took the record counter of the type (0) for proof that the type is empty.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @SuppressWarnings("deprecation")
  @Nested
  class Issue9389LightEdgesInUndeclaredType extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E");
      database.command("sql", "CREATE EDGE TYPE Empty");
      database.transaction(() -> {
        final RID a = database.newVertex("V").save().getIdentity();
        final RID b = database.newVertex("V").save().getIdentity();
        final RID c = database.newVertex("V").save().getIdentity();
        a.asVertex().modify().newLightEdge("E", b);
        b.asVertex().modify().newLightEdge("E", c);
      });
    }

    @Test
    void oneHopCountStarSeesLightEdgesOfUndeclaredType() {
      assertThat(count("MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n")).isEqualTo(2L);
      assertThat(count("MATCH (a:V)-[r:E]->(b:V) RETURN count(*) AS n")).isEqualTo(2L);
      assertThat(count("MATCH (a:V)-[:E]->(b:V) RETURN count(b) AS n")).isEqualTo(2L);
      assertThat(count("MATCH (a:V)-[:E]->(b:V)-[:E]->(c:V) RETURN count(*) AS n")).isEqualTo(1L);
    }

    @Test
    void oneHopCountStarSeesLightEdgesAfterReopen() {
      reopenDatabase();
      assertThat(count("MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n")).isEqualTo(2L);
    }

    @Test
    void typeWithNoEdgeStillCountsZero() {
      assertThat(count("MATCH (a:V)-[:Empty]->(b:V) RETURN count(*) AS n")).isEqualTo(0L);
      assertThat(count("MATCH (a:V)-[:Missing]->(b:V) RETURN count(*) AS n")).isEqualTo(0L);
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Issue #9484: a two-arm star {@code count(*)} answered 0 when the first edge type held only light edges, because the
   * out-of-view fallback of the degree product built each vertex's degree from the records of the edge type, and a light
   * edge has no record.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9484LightEdgeStarJoin extends TestHelper {
    private static final String TWO_MATCH  = "MATCH (s:S)-[:E1]->(t:T) MATCH (s)-[:E2]->(u:U) RETURN count(*) AS n";
    private static final String COMMA      = "MATCH (s:S)-[:E1]->(t:T), (s)-[:E2]->(u:U) RETURN count(*) AS n";
    private static final String CHAIN      = "MATCH (t:T)<-[:E1]-(s:S)-[:E2]->(u:U) RETURN count(*) AS n";
    private static final String WITH       = "MATCH (s:S)-[:E1]->(t:T) WITH s, t MATCH (s)-[:E2]->(u:U) RETURN count(*) AS n";
    private static final String SECOND_ARM = "MATCH (s:S)-[:E2]->(u:U) MATCH (s)-[:E1]->(t:T) RETURN count(*) AS n";
    private static final String REVERSED   = "MATCH (t:T)<-[:E1]-(s:S) MATCH (u:U)<-[:E2]-(s) RETURN count(*) AS n";

    private enum Kind {REGULAR, LIGHT}

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE S");
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE VERTEX TYPE U");
    }

    @Test
    void regularEdges() {
      build("", Kind.REGULAR);
      assertEveryFormCountsThree();
    }

    @Test
    void declaredLightweightType() {
      build(" LIGHTWEIGHT", Kind.LIGHT);
      assertEveryFormCountsThree();
    }

    @Test
    void lightEdgesInUndeclaredType() {
      build("", Kind.LIGHT);
      assertEveryFormCountsThree();
    }

    @Test
    void lightEdgesAfterReopen() {
      build(" LIGHTWEIGHT", Kind.LIGHT);
      reopenDatabase();
      assertEveryFormCountsThree();
    }

    @Test
    void lightEdgesMixedWithRecordEdgesOfTheSameType() {
      database.command("sql", "CREATE EDGE TYPE E1");
      database.command("sql", "CREATE EDGE TYPE E2");
      database.transaction(() -> {
        for (int i = 0; i < 4; i++) {
          final MutableVertex s = database.newVertex("S").set("id", i).save();
          final MutableVertex t = database.newVertex("T").set("id", i).save();
          final MutableVertex u = database.newVertex("U").set("id", i).save();
          if (i % 2 == 0)
            s.newEdge("E1", t);
          else
            s.newLightEdge("E1", t);
          s.newEdge("E2", u);
        }
      });
      assertThat(count(TWO_MATCH)).isEqualTo(4L);
      assertThat(count(CHAIN)).isEqualTo(4L);
    }

    @Test
    void aVertexWithSeveralLightEdgesMultipliesTheArms() {
      database.command("sql", "CREATE EDGE TYPE E1 LIGHTWEIGHT");
      database.command("sql", "CREATE EDGE TYPE E2 LIGHTWEIGHT");
      database.transaction(() -> {
        final MutableVertex s = database.newVertex("S").save();
        for (int i = 0; i < 3; i++)
          s.newLightEdge("E1", database.newVertex("T").save());
        for (int i = 0; i < 2; i++)
          s.newLightEdge("E2", database.newVertex("U").save());
        // a second S vertex with only one arm contributes nothing
        database.newVertex("S").save().newLightEdge("E1", database.newVertex("T").save());
      });
      assertThat(count(TWO_MATCH)).isEqualTo(6L);
      assertThat(count(CHAIN)).isEqualTo(6L);
      assertThat(count(WITH)).isEqualTo(6L);
    }

    @Test
    void aMultiHopArmOverLightEdges() {
      database.command("sql", "CREATE VERTEX TYPE W");
      database.command("sql", "CREATE EDGE TYPE E1 LIGHTWEIGHT");
      database.command("sql", "CREATE EDGE TYPE E3 LIGHTWEIGHT");
      database.command("sql", "CREATE EDGE TYPE E2");
      database.transaction(() -> {
        final MutableVertex s = database.newVertex("S").save();
        // 2 paths s -> t -> w on the first arm (one t with 2 w, one t with 1 w) and 2 edges on the second arm
        final MutableVertex t1 = database.newVertex("T").save();
        final MutableVertex t2 = database.newVertex("T").save();
        s.newLightEdge("E1", t1);
        s.newLightEdge("E1", t2);
        t1.newLightEdge("E3", database.newVertex("W").save());
        t1.newLightEdge("E3", database.newVertex("W").save());
        t2.newLightEdge("E3", database.newVertex("W").save());
        s.newEdge("E2", database.newVertex("U").save());
        s.newEdge("E2", database.newVertex("U").save());
      });
      assertThat(count("MATCH (s:S)-[:E1]->(t:T)-[:E3]->(w:W), (s)-[:E2]->(u:U) RETURN count(*) AS n")).isEqualTo(6L);
      assertThat(count("MATCH (s:S)-[:E1]->(t:T)-[:E3]->(w:W) WITH s, t, w MATCH (s)-[:E2]->(u:U) RETURN count(*) AS n")).isEqualTo(6L);
    }

    private void build(final String e1Modifier, final Kind kind) {
      database.command("sql", "CREATE EDGE TYPE E1" + e1Modifier);
      database.command("sql", "CREATE EDGE TYPE E2");
      database.transaction(() -> {
        for (int i = 0; i < 3; i++) {
          final MutableVertex s = database.newVertex("S").set("id", i).save();
          final MutableVertex t = database.newVertex("T").set("id", i).save();
          final MutableVertex u = database.newVertex("U").set("id", i).save();
          if (kind == Kind.REGULAR)
            s.newEdge("E1", t);
          else
            s.newLightEdge("E1", t);
          s.newEdge("E2", u);
        }
      });
    }

    private void assertEveryFormCountsThree() {
      for (final String query : new String[] { TWO_MATCH, COMMA, CHAIN, WITH, SECOND_ARM, REVERSED })
        assertThat(count(query)).as(query).isEqualTo(3L);
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Regression tests for #9573: two lightweight edges of the same type over the same ordered pair of vertices (two
   * parallel light self loops included) shared one identity, so the openCypher row pipeline treated a path walking one
   * and then the other as reusing a relationship and dropped it, and {@code id(r)} of a lightweight edge threw.
   * <p>
   * The expected counts are enumerated by hand, each stored edge being a relationship of its own.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9573ParallelLightEdges extends TestHelper {

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
      assertThat(count("MATCH (a:P)-[r:K]-(b:P) WITH * RETURN count(r) AS n")).isEqualTo(2L);
      assertThat(count("MATCH (a:P)-[r:K]->(b:P) WITH * RETURN count(r) AS n")).isEqualTo(2L);
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

  /**
   * Regression tests for #9983: the within-path relationship uniqueness of the variable length and quantified path
   * forms compared lightweight edges by RID, and two parallel lightweight edges of one type over the same ordered pair
   * share their RID, so a path walking one and then its twin was dropped as if it reused a relationship. #9573 fixed the
   * same defect for the fixed-length hop-by-hop pipeline only.
   * <p>
   * Every pattern form runs over the same fixture built twice, once with lightweight and once with record-backed
   * edges: record-backed edges were never affected, so the two must agree, and both must give the count enumerated by
   * hand (each stored edge being a relationship of its own).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9983VarLengthParallelLightEdges extends TestHelper {

    private enum Fixture {
      /** two parallel K edges a->b */
      TWO_PARALLEL,
      /** three parallel K edges a->b */
      THREE_PARALLEL,
      /** two parallel K self loops on a */
      TWO_LOOPS
    }

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE K");
    }

    static Stream<Arguments> patterns() {
      final List<Arguments> cases = new ArrayList<>();
      for (final boolean lightweight : new boolean[] { true, false }) {
        // a-e1-b, a-e2-b: a two hop trail walks one edge and comes back by the other, from either end, in either order
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K]-(y:P)-[:K]-(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*2]-(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*2..2]-(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*1..2]-(z:P) RETURN z", 8);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*]-(z:P) RETURN z", 8);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K*2]-(z:P) RETURN r", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH p = (x:P)-[:K*2]-(z:P) RETURN p", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*2]-(z:P) WITH * RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K*2]->(z:P) RETURN z", 0);
        // WALK skips the uniqueness check: every ordered pair of edges, the same one twice included
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH WALK (x:P)-[:K*2]-(z:P) RETURN z", 8);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)){2}(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)){1,2}(z:P) RETURN z", 8);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH ((u:P)-[:K]-(v:P)){2} RETURN v", 4);
        // pattern predicate and pattern comprehension evaluate the variable length pattern through their own traverser
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) WHERE (x)-[:K*2]-(x) RETURN x", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K*2]-(z) | z] AS z RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K]-(y)-[:K]-(z) | z] AS z RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) WHERE (x)-[:K]-()-[:K]-(x) RETURN x", 2);
        // uniqueness spans the hops of the pattern, a fixed hop and a variable length one included
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K]-(y)-[:K*1]-(z) | z] AS z RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K*1]-(y)-[:K*1]-(z) | z] AS z RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) UNWIND [p = (x)-[:K]-()-[:K]-() | p] AS p RETURN p", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P) WHERE (x)-[:K]-()-[:K*1]-(x) RETURN x", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[:K]-(y:P)-[:K*1]-(z:P) RETURN z", 4);
        // relationship uniqueness across the patterns of one MATCH: the second pattern may take the twin of the first's
        // edge, never the edge itself
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]-(y:P), (y)-[:K*1]-(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]-(y:P), (y)-[r2:K*1]-(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K*1]-(y:P), (y)-[r2:K*1]-(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]-(y:P)((u:P)-[:K]-(v:P)){1}(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]-(y:P), (y)((u:P)-[:K]-(v:P)){1}(z:P) RETURN z", 4);
        // a relationship list bound by an earlier clause is matched as the whole path, from either end of the walk
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH ()-[r:K*2]-() WITH r MATCH (x:P)-[r*2]-(z:P) RETURN z", 8);

        // three copies: ordered sequences of distinct copies, alternating a and b, from either end
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K]-(y:P)-[:K]-(z:P) RETURN z", 12);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K*2]-(z:P) RETURN z", 12);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K*3]-(z:P) RETURN z", 12);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K*4]-(z:P) RETURN z", 0);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[:K*1..3]-(z:P) RETURN z", 30);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)){3}(z:P) RETURN z", 12);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)){1,3}(z:P) RETURN z", 30);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)((u:P)-[:K]-(v:P)-[:K]-(w:P)){1}(z:P) RETURN z", 12);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K]-()-[:K]-()-[:K]-(z) | z] AS z RETURN z", 12);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P) UNWIND [(x)-[:K*3]-(z) | z] AS z RETURN z", 12);

        // two loops: (l1, l2) and (l2, l1)
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K]->(y:P)-[:K]->(z:P) RETURN z", 2);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*2]->(z:P) RETURN z", 2);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*1..2]->(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*2]-(z:P) RETURN z", 2);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*1..2]-(z:P) RETURN z", 4);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K]-(z:P) RETURN z", 2);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P) UNWIND [(x)-[:K*2]-(z) | z] AS z RETURN z", 2);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P) UNWIND [(x)-[:K]-(z) | z] AS z RETURN z", 2);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[:K*3]->(z:P) RETURN z", 0);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)((u:P)-[:K]->(v:P)){2}(z:P) RETURN z", 2);
      }
      return cases.stream();
    }

    private static void add(final List<Arguments> cases, final Fixture fixture, final boolean lightweight, final String query,
        final int expected) {
      cases.add(Arguments.of(fixture, lightweight, query, expected));
    }

    @ParameterizedTest(name = "{0} lightweight={1}: {2} -> {3}")
    @MethodSource("patterns")
    void everyPatternFormCountsParallelEdgesAsDistinctRelationships(final Fixture fixture, final boolean lightweight,
        final String query, final int expected) {
      build(fixture, lightweight);
      assertThat(rows(query)).as(query).isEqualTo(expected);
    }

    @Test
    void countPushDownAgreesWithTheRows() {
      build(Fixture.TWO_PARALLEL, true);
      assertThat(count("MATCH (x:P)-[:K*2]-(z:P) RETURN count(*) AS n")).isEqualTo(4L);
      assertThat(count("MATCH (x:P)-[:K*1..2]-(z:P) RETURN count(*) AS n")).isEqualTo(8L);
      assertThat(count("MATCH (x:P)((u:P)-[:K]-(v:P)){2}(z:P) RETURN count(*) AS n")).isEqualTo(4L);
    }

    @Test
    void aSingleLightEdgeIsStillNeverWalkedTwice() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").save();
        final MutableVertex b = database.newVertex("P").save();
        a.newLightEdge("K", b);
      });
      assertThat(rows("MATCH (x:P)-[:K*2]-(z:P) RETURN z")).isZero();
      assertThat(rows("MATCH (x:P)-[:K*1..3]-(z:P) RETURN z")).isEqualTo(2);
      assertThat(rows("MATCH (x:P)((u:P)-[:K]-(v:P)){2}(z:P) RETURN z")).isZero();
      assertThat(rows("MATCH (x:P)-[r1:K]-(y:P), (y)-[:K*1]-(z:P) RETURN z")).isZero();
      assertThat(rows("MATCH (x:P)-[r1:K]-(y:P)((u:P)-[:K]-(v:P)){1}(z:P) RETURN z")).isZero();
    }

    @Test
    void aLightEdgeParallelToARecordBackedOne() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").save();
        final MutableVertex b = database.newVertex("P").save();
        a.newLightEdge("K", b);
        a.newEdge("K", b);
      });
      assertThat(rows("MATCH (x:P)-[:K*2]-(z:P) RETURN z")).isEqualTo(4);
      assertThat(rows("MATCH (x:P)((u:P)-[:K]-(v:P)){2}(z:P) RETURN z")).isEqualTo(4);
    }

    @Test
    void twinsSurviveAReopen() {
      build(Fixture.THREE_PARALLEL, true);
      reopenDatabase();
      assertThat(rows("MATCH (x:P)-[:K*1..3]-(z:P) RETURN z")).isEqualTo(30);
      assertThat(rows("MATCH (x:P)((u:P)-[:K]-(v:P)){1,3}(z:P) RETURN z")).isEqualTo(30);
    }

    /**
     * The planner always picks the depth-first strategy (#6097), so the breadth-first one is driven directly: it runs the
     * same uniqueness check and must agree.
     */
    @Test
    void breadthAndDepthFirstTraversersAgree() {
      build(Fixture.THREE_PARALLEL, true);
      database.transaction(() -> {
        final Vertex a = database.query("opencypher", "MATCH (x:P {name:'a'}) RETURN x").next().getProperty("x");
        for (final boolean bfs : new boolean[] { true, false }) {
          // from a: 3 one-hop paths, 3*2 two-hop paths, 3*2*1 three-hop paths
          assertThat(paths(a, 1, 3, bfs)).as("bfs=" + bfs).isEqualTo(15);
          assertThat(paths(a, 3, 3, bfs)).as("bfs=" + bfs).isEqualTo(6);
        }
      });
    }

    private int paths(final Vertex start, final int minHops, final int maxHops, final boolean bfs) {
      final VariableLengthPathTraverser traverser = new VariableLengthPathTraverser(Direction.BOTH, new String[] { "K" }, null,
          minHops, maxHops, true, bfs, PathMode.TRAIL);
      int found = 0;
      for (final Iterator<TraversalPath> it = traverser.traversePaths(start); it.hasNext(); it.next())
        ++found;
      return found;
    }

    private void build(final Fixture fixture, final boolean lightweight) {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").set("name", "a").save();
        final MutableVertex b = database.newVertex("P").set("name", "b").save();
        final int copies = fixture == Fixture.THREE_PARALLEL ? 3 : 2;
        final MutableVertex to = fixture == Fixture.TWO_LOOPS ? a : b;
        for (int i = 0; i < copies; i++)
          if (lightweight)
            a.newLightEdge("K", to);
          else
            a.newEdge("K", to);
      });
    }

    private int rows(final String query) {
      int found = 0;
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          rs.next();
          ++found;
        }
      }
      return found;
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Regression tests for #10008: #9573 and #9983 taught the pattern matcher that two lightweight edges of one type over
   * the same ordered pair ("twins") are two relationships, but the value-level operations still compared a relationship
   * by its RID, which twins share: {@code =} / {@code <>}, {@code DISTINCT} and grouping keys, path equality and the dedup
   * of {@code DELETE}, which left twins the statement asked to delete in the graph.
   * <p>
   * Every query runs over the same fixture built twice, once with lightweight and once with record-backed edges: the
   * record-backed edges were never affected, so the two must agree.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10008ParallelLightEdgeValueOps extends TestHelper {

    private enum Fixture {
      /** two parallel K edges a->b */
      TWO_PARALLEL,
      /** three parallel K edges a->b */
      THREE_PARALLEL,
      /** two parallel K self loops on a */
      TWO_LOOPS
    }

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE K");
    }

    static Stream<Arguments> queries() {
      final List<Arguments> cases = new ArrayList<>();
      for (final boolean lightweight : new boolean[] { true, false }) {
        // = and <>
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P), (x)-[r2:K]->(y) WHERE r1 <> r2 RETURN r2", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (x)-[r2:K]->(y) WHERE r1 <> r2 RETURN r2", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (x)-[r2:K]->(y) WHERE r1 = r2 RETURN r2", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (x)<-[r2:K]-(y) WHERE r1 = r2 RETURN r2", 0);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (y)<-[r2:K]-(x) WHERE r1 = r2 RETURN r2", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (y)<-[r2:K]-(x) WHERE r1 <> r2 RETURN r2", 2);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (y)<-[r2:K]-(x) WHERE r1 = r2 RETURN r2", 3);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (y)<-[r2:K]-(x) WHERE r1 <> r2 RETURN r2", 6);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[r1:K]->(x) MATCH (x)<-[r2:K]-(x) WHERE r1 = r2 RETURN r2", 2);
        // DISTINCT, grouping and the aggregations that dedup
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]->(y:P) RETURN DISTINCT r", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) RETURN DISTINCT r", 2);
        add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) RETURN DISTINCT r", 3);
        add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[r:K]-(y:P) RETURN DISTINCT r", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K*2]-(z:P) RETURN DISTINCT r", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH p = (x:P)-[:K*2]-(z:P) RETURN DISTINCT p", 4);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH p = (x:P)-[:K]->(z:P) RETURN DISTINCT p", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) WITH DISTINCT r RETURN r", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) RETURN r, count(*) AS n", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]->(y:P) RETURN r UNION MATCH (x:P)<-[r:K]-(y:P) RETURN r", 2);
        add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) WITH collect(DISTINCT r) AS rs UNWIND rs AS r RETURN r",
            2);
      }
      return cases.stream();
    }

    private static void add(final List<Arguments> cases, final Fixture fixture, final boolean lightweight, final String query,
        final int expected) {
      cases.add(Arguments.of(fixture, lightweight, query, expected));
    }

    @ParameterizedTest(name = "{0} lightweight={1}: {2} -> {3}")
    @MethodSource("queries")
    void valueOperationsCountTwinsAsDistinctRelationships(final Fixture fixture, final boolean lightweight, final String query,
        final int expected) {
      build(fixture, lightweight);
      assertThat(rows(query)).as(query).isEqualTo(expected);
    }

    @ParameterizedTest(name = "lightweight={0}")
    @MethodSource("lightweightOrNot")
    void countDistinctCountsTwins(final boolean lightweight) {
      build(Fixture.THREE_PARALLEL, lightweight);
      assertThat(single("MATCH (x:P)-[r:K]-(y:P) RETURN count(DISTINCT r) AS n")).isEqualTo(3L);
      assertThat(single("MATCH (x:P)-[r:K]-(y:P) RETURN size(collect(DISTINCT r)) AS n")).isEqualTo(3L);
      assertThat(single("MATCH (x:P)-[r:K]->(y:P) WITH collect(r) AS rs RETURN size(rs) AS n")).isEqualTo(3L);
    }

    static Stream<Arguments> deletes() {
      final List<Arguments> cases = new ArrayList<>();
      for (final boolean lightweight : new boolean[] { true, false })
        for (final Fixture fixture : Fixture.values()) {
          final long edges = fixture == Fixture.THREE_PARALLEL ? 3 : 2;
          cases.add(Arguments.of(fixture, lightweight, "MATCH (:P)-[r:K]-(:P) DELETE r", edges));
          cases.add(Arguments.of(fixture, lightweight, "MATCH (:P)-[r:K]->(:P) DELETE r", edges));
          cases.add(Arguments.of(fixture, lightweight, "MATCH (:P)<-[r:K]-(:P) DELETE r", edges));
          cases.add(Arguments.of(fixture, lightweight, "MATCH ()-[r:K]-() WITH r DELETE r", edges));
          cases.add(Arguments.of(fixture, lightweight, "MATCH ()-[r:K]-() WITH collect(r) AS rs UNWIND rs AS r DELETE r", edges));
          cases.add(Arguments.of(fixture, lightweight, "MATCH p = (:P)-[:K]-(:P) DELETE p", edges));
          cases.add(Arguments.of(fixture, lightweight, "MATCH (n:P) DETACH DELETE n", edges));
          cases.add(Arguments.of(fixture, lightweight, "MATCH (n:P)-[r:K]-() DELETE r, n", edges));
          cases.add(Arguments.of(fixture, lightweight, "MATCH (n:P) FOREACH (r IN [(n)-[k:K]-() | k] | DELETE r)", edges));
        }
      return cases.stream();
    }

    @ParameterizedTest(name = "{0} lightweight={1}: {2}")
    @MethodSource("deletes")
    void deleteRemovesEveryTwinItWasAskedTo(final Fixture fixture, final boolean lightweight, final String statement,
        final long edges) {
      build(fixture, lightweight);
      final long[] reported = new long[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", statement)) {
          while (rs.hasNext())
            rs.next();
          reported[0] = rs.getStatistics().orElseThrow().getRelationshipsDeleted();
        }
      });
      assertThat(edgesLeft()).as(statement).isZero();
      assertThat(reported[0]).as(statement).isEqualTo(edges);
    }

    @ParameterizedTest(name = "lightweight={0}")
    @MethodSource("lightweightOrNot")
    void deleteReportsOneRelationshipPerTwin(final boolean lightweight) {
      build(Fixture.THREE_PARALLEL, lightweight);
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "MATCH (:P)-[r:K]-(:P) DELETE r RETURN count(*) AS n")) {
          assertThat(((Number) rs.next().getProperty("n")).longValue()).isEqualTo(6L);
        }
      });
      assertThat(edgesLeft()).isZero();
    }

    /** Deleting one of the twins leaves the other: a relationship bound by one row is deleted once, not once per twin. */
    @ParameterizedTest(name = "lightweight={0}")
    @MethodSource("lightweightOrNot")
    void deletingOneTwinLeavesTheOthers(final boolean lightweight) {
      build(Fixture.THREE_PARALLEL, lightweight);
      database.transaction(() -> database.command("opencypher", "MATCH (:P)-[r:K]->(:P) WITH r LIMIT 1 DELETE r").close());
      assertThat(edgesLeft()).isEqualTo(2L);
      database.transaction(() -> database.command("opencypher", "MATCH (:P)-[r:K]->(:P) WITH r LIMIT 1 DELETE r, r").close());
      assertThat(edgesLeft()).isEqualTo(1L);
    }

    /**
     * A query that creates a twin of the edges it is reading: the new one is no row of the MATCH, and the edges that are
     * keep their identities, so DISTINCT counts the relationships the MATCH read, twins apart.
     */
    @Test
    void creatingATwinWhileReadingKeepsTheRowsApart() {
      database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").set("name", "a").save();
        final MutableVertex b = database.newVertex("P").set("name", "b").save();
        for (int i = 0; i < 3; i++)
          a.newEdge("L", b);
      });
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher",
            "MATCH (x:P)-[r:L]-(y:P) CREATE (x)-[:L]->(y) WITH DISTINCT r RETURN count(r) AS n")) {
          assertThat(((Number) rs.next().getProperty("n")).longValue()).isEqualTo(3L);
        }
      });
      // 3 edges read from both ends, one created per row
      assertThat(single("MATCH ()-[r:L]->() RETURN count(r) AS n")).isEqualTo(9L);
      assertThat(single("MATCH (:P)-[r:L]-(:P) RETURN count(DISTINCT r) AS n")).isEqualTo(9L);
    }

    /**
     * An explicit delete of a twin and a DETACH of its endpoint in one statement: the DETACH reads the list after the twin
     * is gone, so its survivors are numbered anew and must not be mistaken for the twins already deleted.
     */
    @ParameterizedTest(name = "{0} lightweight={1}: {2}")
    @MethodSource("explicitAndDetach")
    void anExplicitDeleteAndADetachOfItsEndpointCountEveryTwinOnce(final Fixture fixture, final boolean lightweight,
        final String statement, final long nodes) {
      build(fixture, lightweight);
      final long[] reported = new long[2];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", statement)) {
          while (rs.hasNext())
            rs.next();
          reported[0] = rs.getStatistics().orElseThrow().getRelationshipsDeleted();
          reported[1] = rs.getStatistics().orElseThrow().getNodesDeleted();
        }
      });
      assertThat(edgesLeft()).as(statement).isZero();
      assertThat(reported[0]).as(statement).isEqualTo(fixture == Fixture.THREE_PARALLEL ? 3L : 2L);
      assertThat(reported[1]).as(statement).isEqualTo(nodes);
    }

    static Stream<Arguments> explicitAndDetach() {
      final List<Arguments> cases = new ArrayList<>();
      for (final boolean lightweight : new boolean[] { true, false })
        for (final Fixture fixture : Fixture.values()) {
          cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'})-[r:K]->(:P) DETACH DELETE r, a", 1L));
          cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'})-[r:K]->(:P) DETACH DELETE a, r", 1L));
          if (fixture != Fixture.TWO_LOOPS)
            cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'})-[r:K]->(b:P) DETACH DELETE r, b", 1L));
          cases.add(Arguments.of(fixture, lightweight,
              "MATCH (a:P {name: 'a'})-[r:K]->(:P) WITH a, collect(r) AS rs FOREACH (x IN rs | DETACH DELETE x, a)", 1L));
          // both endpoints of the twins detached by one row: the second DETACH finds the twins gone with the first
          cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) DETACH DELETE a, b", 2L));
          cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'})-[r:K]-(b:P) DETACH DELETE r, a, b",
              fixture == Fixture.TWO_LOOPS ? 1L : 2L));
        }
      return cases.stream();
    }

    /**
     * Twins met only by the last rows of a streaming DELETE: the rows before them are deleted as they stream, the twins
     * read the rest in and are numbered before the first of them goes.
     */
    @ParameterizedTest(name = "lightweight={0}")
    @MethodSource("lightweightOrNot")
    void twinsMetByTheLastRowsOfAStreamingDelete(final boolean lightweight) {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").set("name", "a").save();
        final MutableVertex b = database.newVertex("P").set("name", "b").save();
        // a list is read newest first: the twins, created first, come last
        for (int i = 0; i < 3; i++)
          if (lightweight)
            a.newLightEdge("K", b);
          else
            a.newEdge("K", b);
        for (int i = 0; i < 50; i++)
          if (lightweight)
            a.newLightEdge("K", database.newVertex("P").save());
          else
            a.newEdge("K", database.newVertex("P").save());
      });
      final long[] reported = new long[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "MATCH (:P {name: 'a'})-[r:K]->(:P) DELETE r")) {
          while (rs.hasNext())
            rs.next();
          reported[0] = rs.getStatistics().orElseThrow().getRelationshipsDeleted();
        }
      });
      assertThat(edgesLeft()).isZero();
      assertThat(reported[0]).isEqualTo(53L);
    }

    /**
     * The first twin reads the remaining rows in before it is deleted, under the per-operation heap limit. A limit too
     * small for them fails the statement, which rolls back whole: nothing is deleted, rather than the rows that fit.
     */
    @Test
    void aDeleteOfTwinsOverTheHeapLimitFailsWithoutDeletingAnything() {
      build(Fixture.THREE_PARALLEL, true);
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 2);
      assertThatThrownBy(() -> database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "MATCH (:P)-[r:K]-(:P) DELETE r")) {
          while (rs.hasNext())
            rs.next();
        }
      })).hasStackTraceContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP,
          GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getDefValue());
      assertThat(edgesLeft()).isEqualTo(3L);
    }

    /**
     * A super-node: its edge list is promoted to stripes past the threshold, so the occurrences of its twins are counted
     * across generations and stripes, and the rows of one expansion all come off its one list (resolved once, by the
     * census of that list, rather than once per row).
     */
    @ParameterizedTest(name = "lightweight={0}")
    @MethodSource("lightweightOrNot")
    void twinsOnAStripedSuperNode(final boolean lightweight) {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
      final int leaves = 40;
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("P").set("name", "hub").save();
        final List<MutableVertex> leafs = new ArrayList<>();
        for (int i = 0; i < leaves; i++)
          leafs.add(database.newVertex("P").set("name", "leaf" + i).save());
        // twins spread over the list's life: some before the promotion, some after
        for (int round = 0; round < 3; round++)
          for (int i = 0; i < leaves; i++)
            if (round == 0 || (round == 1 && i < 5) || (round == 2 && i < 2))
              if (lightweight)
                hub.newLightEdge("K", leafs.get(i));
              else
                hub.newEdge("K", leafs.get(i));
      });
      final long relationships = leaves + 5 + 2;
      try (final ResultSet rs = database.query("opencypher", "MATCH (h:P {name: 'hub'}) RETURN h")) {
        final VertexInternal hub = (VertexInternal) rs.next().<Vertex>getProperty("h");
        assertThat(database.lookupByRID(hub.getOutEdgesHeadChunk(), true)).isInstanceOf(StripeDirectory.class);
      }

      assertThat(single("MATCH (h:P {name: 'hub'})-[r:K]-(:P) RETURN count(DISTINCT r) AS n")).isEqualTo(relationships);
      assertThat(single("MATCH (:P)-[r:K]-(:P) RETURN count(DISTINCT r) AS n")).isEqualTo(relationships);
      assertThat(single("MATCH (h:P {name: 'hub'})-[r1:K]->(l:P) MATCH (l)<-[r2:K]-(h) WHERE r1 = r2 RETURN count(*) AS n"))
          .isEqualTo(relationships);
      assertThat(rows("MATCH (:P)-[r:K]-(:P) RETURN DISTINCT r")).isEqualTo((int) relationships);

      final long[] reported = new long[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "MATCH (:P)-[r:K]-(:P) DELETE r")) {
          while (rs.hasNext())
            rs.next();
          reported[0] = rs.getStatistics().orElseThrow().getRelationshipsDeleted();
        }
      });
      assertThat(edgesLeft()).isZero();
      assertThat(reported[0]).isEqualTo(relationships);
    }

    static Stream<Arguments> lightweightOrNot() {
      return Stream.of(Arguments.of(true), Arguments.of(false));
    }

    private long edgesLeft() {
      return single("MATCH ()-[r:K]->() RETURN count(r) AS n");
    }

    private void build(final Fixture fixture, final boolean lightweight) {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").set("name", "a").save();
        final MutableVertex b = database.newVertex("P").set("name", "b").save();
        final int copies = fixture == Fixture.THREE_PARALLEL ? 3 : 2;
        final MutableVertex to = fixture == Fixture.TWO_LOOPS ? a : b;
        for (int i = 0; i < copies; i++)
          if (lightweight)
            a.newLightEdge("K", to);
          else
            a.newEdge("K", to);
      });
    }

    private int rows(final String query) {
      int found = 0;
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          rs.next();
          ++found;
        }
      }
      return found;
    }

    private long single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        final Result row = rs.next();
        return ((Number) row.getProperty("n")).longValue();
      }
    }
  }
}
