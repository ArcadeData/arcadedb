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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlEdgeEndpointWalkTest {
  /**
   * Regression for #10684.
   * <p>
   * Issue #10684: on an edge, SQL reads the endpoints as {@code @out} / {@code @in}. Reading the plain names {@code out} and
   * {@code in} - the OrientDB spelling, or a guess - silently answered null for every path through them. On an edge that has
   * no property of that name they now resolve to the edge's endpoints; a property of that name still wins, and nothing
   * changes on a vertex or a document.
   */
  @Nested
  class EdgeOutInEndpoints extends TestHelper {

    private RID aRid;
    private RID bRid;

    @BeforeEach
    void setUpGraph() {
      database.command("sql", "CREATE VERTEX TYPE A");
      database.command("sql", "CREATE VERTEX TYPE B");
      database.command("sql", "CREATE EDGE TYPE E");
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.transaction(() -> {
        aRid = database.command("sql", "INSERT INTO A SET id = 1").next().getIdentity().orElseThrow();
        bRid = database.command("sql", "INSERT INTO B SET name = 'x'").next().getIdentity().orElseThrow();
        database.command("sql", "CREATE EDGE E FROM (SELECT FROM A) TO (SELECT FROM B) SET qty = 7");
        database.command("sql", "INSERT INTO D SET name = 'doc'");
      });
    }

    @Test
    void propertyPathThroughOutAndInReachesTheEndpoints() {
      final Result row = single("SELECT out.id AS oid, in.name AS iname, @out.id AS aoid, outV().id AS voi FROM E");
      assertThat(row.<Object>getProperty("oid")).isEqualTo(1);
      assertThat(row.<Object>getProperty("iname")).isEqualTo("x");
      assertThat(row.<Object>getProperty("aoid")).isEqualTo(1);
      assertThat(row.<Object>getProperty("voi")).isEqualTo(1);
    }

    @Test
    void bareOutAndInProjectTheEndpointRids() {
      final Result row = single("SELECT out, in FROM E");
      assertThat(row.<Object>getProperty("out")).isEqualTo(aRid);
      assertThat(row.<Object>getProperty("in")).isEqualTo(bRid);
    }

    @Test
    void whereOnOutAndInFiltersByTheEndpoints() {
      assertThat(count("SELECT FROM E WHERE out = " + aRid)).isEqualTo(1);
      assertThat(count("SELECT FROM E WHERE in = " + bRid)).isEqualTo(1);
      assertThat(count("SELECT FROM E WHERE out = " + bRid)).isZero();
      assertThat(count("SELECT FROM E WHERE out.id = 1 AND in.name = 'x'")).isEqualTo(1);
    }

    @Test
    void edgesReachedThroughATraversalResolveTheirEndpointsToo() {
      final Result row = single("SELECT outE().in.name AS names, outE('E')[0].out.id AS firstOut FROM A");
      assertThat(row.<List<Object>>getProperty("names")).containsExactly("x");
      assertThat(row.<Object>getProperty("firstOut")).isEqualTo(1);
    }

    @Test
    void orderByAndGroupByOnOutUseTheEndpoints() {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO A SET id = 2");
        database.command("sql", "CREATE EDGE E FROM (SELECT FROM A WHERE id = 2) TO (SELECT FROM B) SET qty = 8");
      });

      try (final ResultSet rs = database.query("sql", "SELECT FROM E ORDER BY out DESC")) {
        assertThat(rs.stream().map(r -> r.<Integer>getProperty("qty")).toList()).containsExactly(8, 7);
      }
      try (final ResultSet rs = database.query("sql", "SELECT FROM E ORDER BY out ASC")) {
        assertThat(rs.stream().map(r -> r.<Integer>getProperty("qty")).toList()).containsExactly(7, 8);
      }
      try (final ResultSet rs = database.query("sql", "SELECT qty FROM E ORDER BY out DESC")) {
        assertThat(rs.stream().map(r -> r.<Integer>getProperty("qty")).toList()).containsExactly(8, 7);
      }
      try (final ResultSet rs = database.query("sql", "SELECT in.name AS n, count(*) AS c FROM E GROUP BY in.name")) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("n")).isEqualTo("x");
        assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void matchWhereOnAnEdgeAliasReadsTheEndpoints() {
      final Result row = single("MATCH {type: A, as: a}.outE('E'){as: e, where: (out.id = 1 AND in.name = 'x')} RETURN e.qty AS qty");
      assertThat(row.<Object>getProperty("qty")).isEqualTo(7);
    }

    @Test
    void aRealPropertyNamedOutStillWins() {
      database.transaction(() -> database.command("sql", "UPDATE E SET out = 'mine'"));
      final Result row = single("SELECT out, in.name AS iname, @out AS endpoint FROM E");
      assertThat(row.<Object>getProperty("out")).isEqualTo("mine");
      assertThat(row.<Object>getProperty("iname")).isEqualTo("x");
      assertThat(row.<Object>getProperty("endpoint")).isEqualTo(aRid);
    }

    @Test
    void anExplicitNullPropertyNamedOutStillWins() {
      database.transaction(() -> database.command("sql", "UPDATE E SET out = null"));
      final Result row = single("SELECT out AS o, in.name AS iname FROM E");
      assertThat(row.<Object>getProperty("o")).isNull();
      assertThat(row.<Object>getProperty("iname")).isEqualTo("x");
      assertThat(count("SELECT FROM E WHERE out IS NULL")).isEqualTo(1);
    }

    @Test
    void aProjectedColumnNamedOutIsNotReplacedByTheEndpoint() {
      final Result row = single("SELECT out FROM (SELECT qty AS out FROM E)");
      assertThat(row.<Object>getProperty("out")).isEqualTo(7);

      final Result nullRow = single("SELECT out FROM (SELECT null AS out FROM E)");
      assertThat(nullRow.<Object>getProperty("out")).isNull();
    }

    @Test
    void orderByAProjectedNullColumnNamedOutOnAnEdgeRowKeepsTheNull() {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO A SET id = 2");
        database.command("sql", "CREATE EDGE E FROM (SELECT FROM A WHERE id = 2) TO (SELECT FROM B) SET qty = 8");
      });
      try (final ResultSet rs = database.query("sql", "SELECT *, null AS out FROM E ORDER BY out DESC, qty ASC")) {
        final List<Result> rows = rs.stream().toList();
        assertThat(rows).hasSize(2);
        assertThat(rows.stream().map(r -> r.<Object>getProperty("out")).toList()).containsOnlyNulls();
        assertThat(rows.stream().map(r -> r.<Integer>getProperty("qty")).toList()).containsExactly(7, 8);
      }
    }

    @Test
    void lightweightEdgesResolveTheirEndpointsToo() {
      database.command("sql", "CREATE EDGE TYPE L");
      database.transaction(() -> {
        final Vertex a = database.lookupByRID(aRid, true).asVertex();
        a.newLightEdge("L", bRid);
      });
      final Result row = single("SELECT outE('L').in.name AS names, outE('L').out.id AS ids FROM A");
      assertThat(row.<List<Object>>getProperty("names")).containsExactly("x");
      assertThat(row.<List<Object>>getProperty("ids")).containsExactly(1);
    }

    @Test
    void aPropertyNamedInDeclaredOnTheEdgeTypeWinsEvenWhenUnset() {
      database.command("sql", "CREATE PROPERTY E.in STRING");
      final Result row = single("SELECT in AS i, out.id AS oid FROM E");
      assertThat(row.<Object>getProperty("i")).isNull();
      assertThat(row.<Object>getProperty("oid")).isEqualTo(1);
    }

    @Test
    void verticesAndDocumentsAreUnchanged() {
      final Result vertex = single("SELECT out AS o, in AS i FROM A");
      assertThat(vertex.<Object>getProperty("o")).isNull();
      assertThat(vertex.<Object>getProperty("i")).isNull();

      final Result doc = single("SELECT out AS o, in AS i FROM D");
      assertThat(doc.<Object>getProperty("o")).isNull();
      assertThat(doc.<Object>getProperty("i")).isNull();
    }

    private Result single(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        assertThat(rs.hasNext()).as(sql).isTrue();
        final Result row = rs.next();
        assertThat(rs.hasNext()).as(sql).isFalse();
        return row;
      }
    }

    private long count(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.stream().count();
      }
    }
  }

  /**
   * Regression for #10807.
   * <p>
   * Issue #10807: since #10684 a bare {@code out} / {@code in} on an edge with no property of that name reads its endpoint, exactly
   * as {@code @out} / {@code @in} do, but only {@code WHERE @out = #rid} was rewritten into a walk of the vertex's adjacency list;
   * {@code WHERE out = #rid} scanned the whole edge type. The planner now rewrites the bare name too, while the dictionary does not
   * know it: a name no record stores and no type declares reads the endpoint on every edge. Once a record or a type uses it, the scan
   * stays and the stored property wins.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BareEndpointNameAdjacencyWalk extends TestHelper {
    private static final String WALK = "FETCH EDGES FROM VERTEX";

    private RID a;
    private RID b;
    private RID c;

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("V1");
      database.getSchema().createEdgeType("E");
      database.getSchema().createEdgeType("E2").addSuperType("E");
      database.transaction(() -> {
        final MutableVertex va = database.newVertex("V1").set("name", "a").save();
        final MutableVertex vb = database.newVertex("V1").set("name", "b").save();
        final MutableVertex vc = database.newVertex("V1").set("name", "c").save();
        va.newEdge("E", vb).set("qty", 1).save();
        va.newEdge("E2", vc).set("qty", 2).save();
        vb.newEdge("E", vc).set("qty", 3).save();
        a = va.getIdentity();
        b = vb.getIdentity();
        c = vc.getIdentity();
      });
    }

    @Test
    void aBareEndpointNameWalksTheAdjacencyListLikeTheAttribute() {
      assertThat(database.getSchema().getDictionary().getIdByName("out", false)).as("the fixture stores no property named out")
          .isNegative();

      for (final String endpoint : new String[] { "out", "in" }) {
        final boolean out = "out".equals(endpoint);
        final RID vertex = out ? a : c;
        final String bare = "SELECT FROM E WHERE " + endpoint + " = " + vertex;
        final String attribute = "SELECT FROM E WHERE @" + endpoint + " = " + vertex;

        assertThat(plan(bare)).as(bare).contains(WALK);
        assertThat(qtys(bare)).as(bare).isEqualTo(qtys(attribute)).containsExactly(out ? new Integer[] { 1, 2 } : new Integer[] { 2, 3 });
      }

      // the remaining conditions stay as a filter, and a parameter is resolved like a literal
      final String filtered = "SELECT FROM E WHERE out = " + a + " AND qty > 1";
      assertThat(plan(filtered)).contains(WALK);
      assertThat(qtys(filtered)).containsExactly(2);
      try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE in = ?", c)) {
        assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains(WALK);
        final Set<Integer> qtys = new TreeSet<>();
        rs.forEachRemaining(r -> qtys.add(r.getProperty("qty")));
        assertThat(qtys).containsExactly(2, 3);
      }
    }

    /**
     * A record that stores a property named {@code out} makes the name ambiguous: the scan comes back, and the stored value wins on
     * that record. Asked of the same statement text that was rewritten before the write, which pins that the rewrite is never
     * served from the plan cache.
     */
    @Test
    void aStoredPropertyNamedLikeTheEndpointKeepsTheScan() {
      final String query = "SELECT FROM E WHERE out = " + a;
      assertThat(plan(query)).contains(WALK);
      assertThat(qtys(query)).containsExactly(1, 2);

      // b -> c, with a stored property out that names a: by its endpoints it is not an edge out of a, by its property it is
      database.transaction(
          () -> b.asVertex().modify().newEdge("E", c.asVertex()).set("qty", 4).set("out", a).save());

      assertThat(database.getSchema().getDictionary().getIdByName("out", false)).isNotNegative();
      assertThat(plan(query)).doesNotContain(WALK);
      assertThat(qtys(query)).as("the stored property wins on the record that has it").containsExactly(1, 2, 4);
      assertThat(qtys("SELECT FROM E WHERE @out = " + a)).as("the attribute is still the endpoint").containsExactly(1, 2);
      // the other name is still unused, so it keeps the walk
      assertThat(plan("SELECT FROM E WHERE in = " + c)).contains(WALK);
    }

    /** A property of that name declared by any type, here a subtype, keeps the scan too: on its edges the name reads the property. */
    @Test
    void aDeclaredPropertyNamedLikeTheEndpointKeepsTheScan() {
      database.getSchema().getType("E2").createProperty("in", Type.STRING);

      final String query = "SELECT FROM E WHERE in = " + c;
      assertThat(plan(query)).doesNotContain(WALK);
      assertThat(qtys(query)).as("on E2 the declared, unset, property reads null").containsExactly(3);
    }

    /** A RID that names no vertex answers no row, as the scan does, rather than failing: a deleted vertex, and a document. */
    @Test
    void aRidThatNamesNoVertexAnswersNoRow() {
      database.getSchema().createDocumentType("D");
      final RID[] created = new RID[2];
      database.transaction(() -> {
        created[0] = database.newDocument("D").set("name", "d").save().getIdentity();
        final MutableVertex gone = database.newVertex("V1").set("name", "gone").save();
        gone.newEdge("E", a.asVertex()).set("qty", 9).save();
        created[1] = gone.getIdentity();
      });
      final RID document = created[0];
      final RID deleted = created[1];
      database.transaction(() -> deleted.asVertex().delete());

      for (final String endpoint : new String[] { "out", "@out", "in", "@in" }) {
        final String walked = "SELECT FROM E WHERE " + endpoint + " = " + deleted;
        assertThat(plan(walked)).as(walked).contains(WALK);
        assertThat(qtys(walked)).as(walked).isEmpty();

        final String scanned = "SELECT FROM E WHERE " + endpoint + " = " + document;
        assertThat(plan(scanned)).as(scanned).doesNotContain(WALK);
        assertThat(qtys(scanned)).as(scanned).isEmpty();
      }
    }

    /** A unidirectional edge type keeps no incoming lists: an incoming endpoint is answered by the scan, the outgoing one walks. */
    @Test
    void anIncomingEndpointOfAUnidirectionalTypeKeepsTheScan() {
      database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();
      database.getSchema().buildEdgeType().withName("U2").withBidirectional(false).create().addSuperType("E");
      database.transaction(() -> {
        a.asVertex().modify().newEdge("U", c.asVertex()).set("qty", 5).save();
        a.asVertex().modify().newEdge("U2", c.asVertex()).set("qty", 6).save();
      });

      for (final String endpoint : new String[] { "in", "@in" }) {
        final String unidirectional = "SELECT FROM U WHERE " + endpoint + " = " + c;
        assertThat(plan(unidirectional)).as(unidirectional).doesNotContain(WALK);
        assertThat(qtys(unidirectional)).as(unidirectional).containsExactly(5);

        // a unidirectional subtype keeps the scan of its super type too, or its edges would be missed
        final String polymorphic = "SELECT FROM E WHERE " + endpoint + " = " + c;
        assertThat(plan(polymorphic)).as(polymorphic).doesNotContain(WALK);
        assertThat(qtys(polymorphic)).as(polymorphic).containsExactly(2, 3, 6);
      }
      assertThat(plan("SELECT FROM U WHERE out = " + a)).contains(WALK);
      assertThat(qtys("SELECT FROM U WHERE out = " + a)).containsExactly(5);
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private Set<Integer> qtys(final String query) {
      final Set<Integer> qtys = new TreeSet<>();
      try (final ResultSet rs = database.query("sql", query)) {
        rs.forEachRemaining(r -> qtys.add(r.getProperty("qty")));
      }
      return qtys;
    }
  }

  /**
   * Regression for #10807.
   * <p>
   * Issue #10807: walking a vertex's adjacency instead of scanning the edge type loads the vertex, which a user may read the edges of
   * without being allowed to read. For that user the planner keeps the scan, which answers from the edges alone, rather than turning
   * a query the user may run into a refusal.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EndpointWalkNeedsVertexGrant {
    private static final String PATH = "target/databases/Issue10807EndpointWalkNeedsVertexGrantTest";
    private static final String WALK = "FETCH EDGES FROM VERTEX";

    private DatabaseFactory factory;
    private Database        database;
    private RID             start;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      database.getSchema().createVertexType("V1");
      database.getSchema().createEdgeType("E");
      database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V1").set("name", "a").save();
        final MutableVertex b = database.newVertex("V1").set("name", "b").save();
        a.newEdge("E", b).set("qty", 1).save();
        a.newLightEdge("L", b);
        start = a.getIdentity();
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen()) {
        DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
        database.drop();
      }
      factory.close();
    }

    @Test
    void aUserWhoCannotReadTheVertexIsAnsweredByTheScan() {
      for (final String endpoint : new String[] { "out", "@out" })
        assertThat(plan("SELECT FROM E WHERE " + endpoint + " = " + start)).as("a user who may read both walks").contains(WALK);

      bindUserWithoutReadOnVertices();

      for (final String endpoint : new String[] { "out", "@out" }) {
        final String query = "SELECT FROM E WHERE " + endpoint + " = " + start;
        try (final ResultSet rs = database.query("sql", query)) {
          assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).as(query).doesNotContain(WALK);
          assertThat(rs.next().<Integer>getProperty("qty")).as(query).isEqualTo(1);
          assertThat(rs.hasNext()).isFalse();
        }
      }
    }

    /**
     * A lightweight edge has no record, so the walk would hand it out without asking the edge type's grant: for a user who may not read
     * the edge type the planner keeps the scan, which applies the grant, and the edge is never answered.
     */
    @Test
    void aUserWhoCannotReadTheEdgeTypeIsNeverAnsweredItsLightweightEdges() {
      for (final String endpoint : new String[] { "out", "@out" })
        assertThat(plan("SELECT FROM L WHERE " + endpoint + " = " + start)).as("a user who may read both walks").contains(WALK);

      bindUser(fileIdsOf("L"));

      for (final String endpoint : new String[] { "out", "@out" }) {
        final String query = "SELECT FROM L WHERE " + endpoint + " = " + start;
        try (final ResultSet rs = database.query("sql", query)) {
          assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).as(query).doesNotContain(WALK);
          assertThat(rs.hasNext()).as(query).isFalse();
        } catch (final SecurityException e) {
          // the scan refuses the type outright: refused, not answered
        }
      }
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private void bindUserWithoutReadOnVertices() {
      bindUser(fileIdsOf("V1"));
    }

    private Set<Integer> fileIdsOf(final String type) {
      final Set<Integer> files = new HashSet<>();
      for (final Bucket bucket : database.getSchema().getType(type).getBuckets(true))
        files.add(bucket.getFileId());
      return files;
    }

    /** Binds a user refused READ_RECORD on {@code deniedFiles} and granted everything else. */
    private void bindUser(final Set<Integer> deniedFiles) {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "restricted";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return false;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return !(access == ACCESS.READ_RECORD && deniedFiles.contains(fileId));
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }
  }

  /**
   * Regression for #7847.
   * <p>
   * Pins the two SQL shapes Studio's graph-expansion picker is built on (issue #7847).
   * <p>
   * The picker lives in JavaScript, where a test can only assert on the text of the command it builds. What that
   * text MEANS is an engine question, and if either shape stops working the picker fails in the browser with an
   * error a Studio test would never have seen. So the shapes are pinned here, next to the engine that answers
   * them, and the JS test that asserts the builder produces them names this class.
   * <ul>
   *   <li>the per-type count, which is what lets the picker show how big a choice is WITHOUT fetching a
   *       supernode's edges in order to count them;</li>
   *   <li>the filtered expansion, whose edge type names arrive as named PARAMETERS - the reason the builder needs
   *       no string-escaping convention, and the reason a type name containing a quote cannot break it.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeExpansionQueries extends TestHelper {

    private String hub;

    private void buildGraph() {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE EDGE TYPE Knows");
      database.command("sql", "CREATE EDGE TYPE Bought");
      // A type name carrying a quote: free-form names are exactly why the builder passes them as parameters.
      database.command("sql", "CREATE EDGE TYPE `it's`");

      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX Person SET name = 'hub'");
        database.command("sql", "CREATE VERTEX Person SET name = 'a'");
        database.command("sql", "CREATE VERTEX Person SET name = 'b'");
        database.command("sql", "CREATE VERTEX Person SET name = 'c'");

        edge("Knows", "hub", "a");
        edge("Knows", "hub", "b");
        edge("Knows", "hub", "c");
        edge("Bought", "hub", "a");
        edge("it's", "hub", "b");
        // One incoming edge, so the two directions cannot be confused with each other.
        edge("Knows", "a", "hub");
      });

      hub = database.query("sql", "SELECT @rid AS r FROM Person WHERE name = 'hub'").next().getProperty("r").toString();
    }

    private void edge(final String type, final String from, final String to) {
      database.command("sql", "CREATE EDGE `" + type + "` FROM (SELECT FROM Person WHERE name = ?) "
          + "TO (SELECT FROM Person WHERE name = ?)", from, to);
    }

    /** The counts the picker renders, per direction, aggregated by the server. */
    @Test
    void theEdgeTypeCountIsAggregatedPerTypeAndPerDirection() {
      buildGraph();

      assertThat(countsOf("out")).containsExactlyInAnyOrderEntriesOf(
          Map.of("Knows", 3L, "Bought", 1L, "it's", 1L));
      assertThat(countsOf("in")).containsExactlyInAnyOrderEntriesOf(Map.of("Knows", 1L));
    }

    /** A node with no edges in a direction answers nothing, which is what makes the picker's empty state real. */
    @Test
    void aNodeWithNoEdgesInADirectionAnswersNoRows() {
      buildGraph();
      final String leaf = database.query("sql", "SELECT @rid AS r FROM Person WHERE name = 'c'").next()
          .<Object>getProperty("r").toString();

      final ResultSet rs = database.query("sql",
          "select @type as type, count(*) as total from (select expand( outE() ) from " + leaf + ") group by @type");
      assertThat(rs.hasNext()).isFalse();
    }

    /** The filtered expansion: only the chosen types, named by parameter. */
    @Test
    void theExpansionTakesItsEdgeTypesAsNamedParameters() {
      buildGraph();

      final Map<String, Object> params = new HashMap<>();
      params.put("t0", "Knows");
      params.put("t1", "Bought");

      final ResultSet rs = database.query("sql", "select expand( outE(:t0, :t1) ) from " + hub, params);
      int count = 0;
      while (rs.hasNext()) {
        assertThat(typeNameOf(rs.next())).isIn("Knows", "Bought");
        count++;
      }
      assertThat(count).as("3 Knows + 1 Bought, and not the it's edge").isEqualTo(4);
    }

    /**
     * The reason the names are parameters and not quoted literals: a type name can contain a quote, and the
     * escaping convention for a SQL string literal here is backslash rather than doubling - a detail a builder
     * gets wrong once and then carries.
     */
    @Test
    void anEdgeTypeNameCarryingAQuoteNeedsNoEscapingAtAll() {
      buildGraph();

      final ResultSet rs = database.query("sql", "select expand( outE(:t0) ) from " + hub, Map.of("t0", "it's"));
      assertThat(rs.hasNext()).isTrue();
      assertThat(typeNameOf(rs.next())).isEqualTo("it's");
      assertThat(rs.hasNext()).isFalse();
    }

    /** The ceiling the picker offers, which is what keeps a supernode from flooding the canvas. */
    @Test
    void theExpansionCanBeBounded() {
      buildGraph();

      final ResultSet rs = database.query("sql", "select expand( outE() ) from " + hub + " limit 2");
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    }

    /** The edge's type. Read off the element, not as an "@type" property: a Result exposes the record's own. */
    private static String typeNameOf(final Result row) {
      return row.getElement().orElseThrow().getTypeName();
    }

    private Map<String, Long> countsOf(final String direction) {
      final Map<String, Long> counts = new LinkedHashMap<>();
      final ResultSet rs = database.query("sql", "select @type as type, count(*) as total from "
          + "(select expand( " + direction + "E() ) from " + hub + ") group by @type");
      while (rs.hasNext()) {
        final Result row = rs.next();
        counts.put(row.getProperty("type"), ((Number) row.getProperty("total")).longValue());
      }
      return counts;
    }
  }

  /**
   * Regression for #7914.
   * <p>
   * Issue #7914 made {@code RemoteVertex.isConnectedTo} BIND its edge type instead of interpolating it into the
   * string literal argument of {@code both()}/{@code out()}/{@code in()}. The unit test for that change asserts on
   * the SQL text and the captured parameters against a mocked database, which cannot tell whether the statement
   * actually runs - and a bound parameter in a FUNCTION ARGUMENT is a less common position than in a WHERE clause
   * (PR #7942 review).
   * <p>
   * So this executes it against a real database. The graph traversal functions take their labels from
   * ALREADY-EVALUATED argument values ({@code SQLFunctionMove.execute} reads {@code iParameters}), so a parameter
   * is exactly as good as a literal there - this pins that, and would fail loudly if the parameter ever stopped
   * being resolved before the function sees it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BoundEdgeTypeArgument extends TestHelper {

    @Test
    void aBoundEdgeTypeResolvesTheSameWayAsALiteralOne() {
      database.command("sql", "create vertex type Node");
      database.command("sql", "create edge type Linked");
      database.command("sql", "create edge type Ignored");

      database.transaction(() -> {
        database.command("sql", "create vertex Node set n = 1");
        database.command("sql", "create vertex Node set n = 2");
        database.command("sql", "create vertex Node set n = 3");
        database.command("sql",
            "create edge Linked from (select from Node where n = 1) to (select from Node where n = 2)");
        database.command("sql",
            "create edge Ignored from (select from Node where n = 1) to (select from Node where n = 3)");
      });

      // the literal spelling, which is what the client used to send
      final long viaLiteral = countNeighbours("select both('Linked') as vertices from Node where n = 1", Map.of());

      // ...and the bound spelling the fix sends instead
      final long viaParameter = countNeighbours("select both(:edgeType) as vertices from Node where n = 1",
          Map.of("edgeType", "Linked"));

      assertThat(viaParameter)
          .as("a bound edge type must select the same neighbours as the literal one")
          .isEqualTo(viaLiteral)
          .isEqualTo(1);

      // and it really is filtering: the other edge type answers its own neighbour, not both
      assertThat(countNeighbours("select both(:edgeType) as vertices from Node where n = 1",
          Map.of("edgeType", "Ignored"))).isEqualTo(1);
    }

    /**
     * The full shape {@code isConnectedTo} sends, parameter included: the neighbour set of one vertex filtered by
     * edge type, then tested with CONTAINS.
     */
    @Test
    void theIsConnectedToShapeRunsWithABoundEdgeType() {
      database.command("sql", "create vertex type V2");
      database.command("sql", "create edge type E2");

      database.transaction(() -> {
        database.command("sql", "create vertex V2 set n = 1");
        database.command("sql", "create vertex V2 set n = 2");
        database.command("sql", "create edge E2 from (select from V2 where n = 1) to (select from V2 where n = 2)");
      });

      final Object from = database.query("sql", "select @rid as rid from V2 where n = 1").next().getProperty("rid");
      final Object to = database.query("sql", "select @rid as rid from V2 where n = 2").next().getProperty("rid");

      final ResultSet connected = database.query("sql",
          "select from ( select both(:edgeType) as vertices from " + from + " ) where vertices contains " + to,
          Map.of("edgeType", "E2"));

      assertThat(connected.hasNext()).as("the bound edge type must not stop the traversal from matching").isTrue();
    }

    /**
     * The shape {@code countEdges} and the {@code getEdges}/{@code getVertices} family send: SEVERAL bound type
     * names in one traversal call, which is where those two sites used to write single-quoted literals.
     */
    @Test
    void severalBoundTypesFilterTheTraversalTheSameWayLiteralsDid() {
      database.command("sql", "create vertex type N3");
      database.command("sql", "create edge type A3");
      database.command("sql", "create edge type B3");
      database.command("sql", "create edge type C3");

      database.transaction(() -> {
        database.command("sql", "create vertex N3 set n = 1");
        for (int i = 2; i <= 4; i++)
          database.command("sql", "create vertex N3 set n = " + i);
        database.command("sql", "create edge A3 from (select from N3 where n = 1) to (select from N3 where n = 2)");
        database.command("sql", "create edge B3 from (select from N3 where n = 1) to (select from N3 where n = 3)");
        database.command("sql", "create edge C3 from (select from N3 where n = 1) to (select from N3 where n = 4)");
      });

      final long viaLiterals = count("select both('A3', 'B3').size() as count from N3 where n = 1", Map.of());
      final long viaParameters = count("select both(:t0, :t1).size() as count from N3 where n = 1",
          Map.of("t0", "A3", "t1", "B3"));

      assertThat(viaParameters)
          .as("two bound type names must filter exactly as the two literals did")
          .isEqualTo(viaLiterals)
          .isEqualTo(2);
    }

    /**
     * The shape {@code countBucket}/{@code iterateBucket} send. A bucket target in a FROM position is the single
     * lexer token {@code BUCKET_IDENTIFIER: BUCKET COLON IDENTIFIER}, whose identifier is the BARE form, so
     * {@code bucket:`name`} is a PARSE ERROR - which is how #7914's first pass turned a working countBucket into one
     * that counted nothing. The single-element bucket LIST takes a quoted identifier and addresses the same bucket
     * (PR #7942 review).
     */
    @Test
    void aQuotedBucketTargetCountsTheSameBucketAsTheBareOne() {
      database.command("sql", "create document type Bkt");
      database.transaction(() -> database.command("sql", "insert into Bkt set n = 1"));

      final String bucket = database.getSchema().getType("Bkt").getBuckets(false).getFirst().getName();

      final long bare = count("select count(*) as count from bucket:" + bucket, Map.of());
      final long quotedList = count("select count(*) as count from bucket:[`" + bucket + "`]", Map.of());

      assertThat(quotedList).as("the escaped form must address the same bucket").isEqualTo(bare).isEqualTo(1);

      // and the shape that looked like the obvious escaping does not parse at all
      assertThatThrownBy(() -> database.query("sql", "select count(*) as count from bucket:`" + bucket + "`"))
          .isInstanceOf(CommandSQLParsingException.class);
    }

    private long count(final String sql, final Map<String, Object> params) {
      return database.query("sql", sql, params).next().<Number>getProperty("count").longValue();
    }

    private long countNeighbours(final String sql, final Map<String, Object> params) {
      final ResultSet result = database.query("sql", sql, params);
      assertThat(result.hasNext()).isTrue();
      final Object vertices = result.next().getProperty("vertices");
      return vertices instanceof Iterable<?> iterable ?
          StreamSupport.stream(iterable.spliterator(), false).count() :
          vertices == null ? 0 : 1;
    }
  }
}
