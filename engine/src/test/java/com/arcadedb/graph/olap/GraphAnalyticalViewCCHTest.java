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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreateGraphAnalyticalViewStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Customizable Contraction Hierarchies kept on a Graph Analytical View (issue #9437), end to end: built with the view,
 * re-customized when weights change, rebuilt when the topology changes, never answering for a snapshot it was not
 * prepared for, persisted with the view definition, and reachable from SQL and Cypher.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphAnalyticalViewCCHTest {
  private static final String DB_PATH = "./target/databases/test-gav-cch";
  private static final double EPS     = 1e-9;

  private Database database;
  private RID[]    junctions;
  // the reference graph, mirrored by every write the test makes: edge RID -> {tail, head, weight}
  private final Map<RID, double[]> roads = new HashMap<>();
  // RAIL edges carry no distance: a search over every edge type walks them at weight 1
  private final List<double[]>     rails = new ArrayList<>();
  private final Map<RID, Integer>  index = new HashMap<>();

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Junction");
    database.getSchema().createEdgeType("ROAD").createProperty("distance", Type.DOUBLE);
    database.getSchema().createEdgeType("RAIL");
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      GraphAnalyticalViewRegistry.dropAll(database);
      database.drop();
      database = null;
    }
  }

  @Test
  void answersLikeDijkstraInEveryDirection() {
    grid(25, new Random(11));
    final GraphAnalyticalView view = syncView("roads");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch).isNotNull();
    assertThat(cch.awaitReady(true, 60, TimeUnit.SECONDS)).isTrue();

    final Random random = new Random(5);
    for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values())
      assertRandomPairs(random, 150, direction, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
    assertThat(cch.getTopologyBuildCount()).isEqualTo(1);
  }

  @Test
  void weightChangesOnlyReCustomize() {
    grid(20, new Random(3));
    final GraphAnalyticalView view = syncView("roads");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    final long customizations = cch.getCustomizationCount() + cch.getPartialCustomizationCount();

    final Random random = new Random(8);
    database.transaction(() -> {
      for (final Map.Entry<RID, double[]> road : roads.entrySet())
        if (random.nextInt(3) == 0) {
          final double weight = random.nextInt(200);
          road.getValue()[2] = weight;
          road.getKey().asEdge().modify().set("distance", weight).save();
        }
    });

    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertRandomPairs(random, 150, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
    assertThat(cch.getTopologyBuildCount()).as("a weight change keeps the topology").isEqualTo(1);
    // a third of the roads changed at once: partially or fully, depending on how much of the hierarchy that reaches
    assertThat(cch.getCustomizationCount() + cch.getPartialCustomizationCount()).isGreaterThan(customizations);
  }

  /**
   * Weight changes, closed roads, removed roads and a road added beside an existing one are caught up incrementally:
   * the view keeps its base CSR (the new weights live in its overlay), the hierarchy keeps its topology, and only the
   * arcs above the change are re-customized.
   */
  @Test
  void updatesAreCaughtUpIncrementally() {
    grid(20, new Random(51));
    final GraphAnalyticalView view = syncView("roads");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(true, 60, TimeUnit.SECONDS)).isTrue();
    final long buildTimestamp = view.getBuildTimestamp();
    final long fullCustomizations = cch.getCustomizationCount();
    final Random random = new Random(52);
    final List<RID> all = new ArrayList<>(roads.keySet());

    // one weight
    long partials = cch.getPartialCustomizationCount();
    final RID one = all.get(random.nextInt(all.size()));
    database.transaction(() -> setDistance(one, 500.0));
    assertCaughtUp(cch, partials, random);

    // a hundred weights in one commit
    partials = cch.getPartialCustomizationCount();
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        setDistance(all.get(random.nextInt(all.size())), 1 + random.nextInt(60));
    });
    assertCaughtUp(cch, partials, random);

    // a closed road: an infinite weight is not walkable
    partials = cch.getPartialCustomizationCount();
    final RID closed = all.get(random.nextInt(all.size()));
    database.transaction(() -> setDistance(closed, Double.POSITIVE_INFINITY));
    roads.remove(closed);
    assertCaughtUp(cch, partials, random);

    // a removed road
    partials = cch.getPartialCustomizationCount();
    final RID removed = all.get(random.nextInt(all.size()));
    if (roads.remove(removed) != null)
      database.transaction(() -> removed.asEdge().delete());
    assertCaughtUp(cch, partials, random);

    // a second road beside an existing one joins two vertices the hierarchy already joins
    partials = cch.getPartialCustomizationCount();
    database.transaction(() -> road(0, 1, 0.25));
    assertCaughtUp(cch, partials, random);

    assertThat(view.getBuildTimestamp()).as("no update rebuilt the view's CSR").isEqualTo(buildTimestamp);
    assertThat(cch.getTopologyBuildCount()).as("no update rebuilt the topology").isEqualTo(1);
    assertThat(cch.getCustomizationCount()).as("no update customized the whole hierarchy").isEqualTo(fullCustomizations);
  }

  /** New weights held in the overlay count toward compaction: once folded into a rebuilt base, answers stay exact. */
  @Test
  void weightUpdatesCrossingTheCompactionThreshold() {
    grid(15, new Random(61));
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("roads").withEdgeTypes("ROAD")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).withCompactionThreshold(25)
        .withContractionHierarchy("distance").build();
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    final long buildTimestamp = view.getBuildTimestamp();

    final Random random = new Random(62);
    final List<RID> all = new ArrayList<>(roads.keySet());
    for (int commit = 0; commit < 8; commit++) {
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          setDistance(all.get(random.nextInt(all.size())), 1 + random.nextInt(80));
      });
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
      // a compaction can publish a newer snapshot at any moment here, answered by Dijkstra until caught up: exact either way
      assertRandomPairs(random, 30, Vertex.DIRECTION.OUT, null);
    }
    final long deadline = System.currentTimeMillis() + 60_000;
    while (view.getBuildTimestamp() == buildTimestamp && System.currentTimeMillis() < deadline)
      Thread.yield();
    assertThat(view.getBuildTimestamp()).as("80 updated edges crossed the threshold of 25: the view compacted").isNotEqualTo(
        buildTimestamp);
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertRandomPairs(random, 60, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
    assertThat(cch.getTopologyBuildCount()).isEqualTo(1);
  }

  /**
   * A base edge's new weight is held for its pair while it is the only BASE edge of the pair. A parallel edge added
   * later lives in the overlay under its own identity, so it neither hides that value nor is mistaken for it.
   */
  @Test
  void aSoleEdgeUpdatedThenGivenAParallelTwin() {
    grid(10, new Random(71));
    final GraphAnalyticalView view = syncView("roads");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    final Random random = new Random(72);

    RID original = null;
    for (final Map.Entry<RID, double[]> road : roads.entrySet())
      if ((int) road.getValue()[0] == 0) {
        original = road.getKey();
        break;
      }
    assertThat(original).isNotNull();
    final RID edge = original;
    final int to = (int) roads.get(edge)[1];

    database.transaction(() -> setDistance(edge, 90.0));
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertRandomPairs(random, 40, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);

    database.transaction(() -> road(0, to, 45.0));
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertThat(find(0, to, Vertex.DIRECTION.OUT).weight()).isLessThanOrEqualTo(45.0);
    assertRandomPairs(random, 40, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);

    database.transaction(() -> setDistance(edge, 2.0));
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertThat(find(0, to, Vertex.DIRECTION.OUT).weight()).isEqualTo(2.0);
    assertRandomPairs(random, 40, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
  }

  private void setDistance(final RID edge, final double distance) {
    final double[] road = roads.get(edge);
    if (road != null)
      road[2] = distance;
    edge.asEdge().modify().set("distance", distance).save();
  }

  /**
   * A weight update to one of two parallel roads rebuilds the view (the column slot cannot be told), and the hierarchy is
   * UNAVAILABLE meanwhile. It keeps its metric across that state: on the rebuilt base it re-reads the view and finds one
   * arc changed, so it updates the metric partially instead of customizing it afresh.
   */
  @Test
  void aViewRebuildEndsInAPartialCustomization() {
    grid(12, new Random(81));
    database.transaction(() -> road(0, 1, 100.0)); // a parallel twin of the 0 -> 1 road, and the longer of the two
    final GraphAnalyticalView view = syncView("roads");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    final long buildTimestamp = view.getBuildTimestamp();
    final long fullCustomizations = cch.getCustomizationCount();
    final long partials = cch.getPartialCustomizationCount();

    RID twin = null;
    for (final Map.Entry<RID, double[]> road : roads.entrySet())
      if ((int) road.getValue()[0] == 0 && (int) road.getValue()[1] == 1 && road.getValue()[2] == 100.0)
        twin = road.getKey();
    final RID edge = twin;
    database.transaction(() -> setDistance(edge, 0.5));

    final long deadline = System.currentTimeMillis() + 60_000;
    while ((view.getBuildTimestamp() == buildTimestamp || cch.getPartialCustomizationCount() == partials)
        && System.currentTimeMillis() < deadline)
      Thread.yield();
    assertThat(view.getBuildTimestamp()).as("the parallel pair made the view rebuild").isNotEqualTo(buildTimestamp);
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertThat(cch.getPartialCustomizationCount()).isGreaterThan(partials);
    assertThat(cch.getCustomizationCount()).as("no full customization after the rebuild").isEqualTo(fullCustomizations);
    assertThat(cch.getTopologyBuildCount()).isEqualTo(1);
    assertThat(find(0, 1, Vertex.DIRECTION.OUT).weight()).isEqualTo(0.5);
    assertRandomPairs(new Random(82), 60, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
  }

  private void assertCaughtUp(final ContractionHierarchy cch, final long partialsBefore, final Random random) {
    assertThat(cch.awaitReady(true, 60, TimeUnit.SECONDS)).isTrue();
    assertThat(cch.getPartialCustomizationCount()).isGreaterThan(partialsBefore);
    for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.BOTH })
      assertRandomPairs(random, 60, direction, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
  }

  @Test
  void deletionsAndInsertionsAreFollowed() {
    grid(15, new Random(21));
    final GraphAnalyticalView view = syncView("roads");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();

    // deletions are infinite weights: same topology
    final Random random = new Random(4);
    database.transaction(() -> {
      final List<RID> all = new ArrayList<>(roads.keySet());
      for (int i = 0; i < 30; i++) {
        final RID edge = all.get(random.nextInt(all.size()));
        if (roads.remove(edge) != null)
          edge.asEdge().delete();
      }
    });
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertRandomPairs(random, 100, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
    assertThat(cch.getTopologyBuildCount()).isEqualTo(1);

    // a shortcut between two far corners is not an arc of the supergraph: a new topology, contracted in the order the
    // old one had rather than in a new one
    database.transaction(() -> road(0, junctions.length - 1, 0.5));
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertThat(cch.getTopologyBuildCount()).isEqualTo(1);
    assertThat(cch.getTopologyRecontractionCount()).isEqualTo(1);
    assertRandomPairs(random, 100, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);

    // a new junction joined to the grid
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Junction").set("i", junctions.length).save();
      junctions = Arrays.copyOf(junctions, junctions.length + 1);
      junctions[junctions.length - 1] = v.getIdentity();
      index.put(v.getIdentity(), junctions.length - 1);
      road(junctions.length - 1, 7, 1.0);
      road(8, junctions.length - 1, 1.0);
    });
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    // the new junction is ranked below everything the kept order already held
    assertThat(cch.getTopologyRecontractionCount()).isEqualTo(2);
    assertThat(cch.getTopologyBuildCount()).isEqualTo(1);
    assertRandomPairs(random, 100, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
  }

  @Test
  void aTransactionSeesItsOwnChanges() {
    grid(10, new Random(17));
    final GraphAnalyticalView view = syncView("roads");
    assertThat(view.getContractionHierarchy("distance", "ROAD").awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();

    final int s = 0;
    final int t = junctions.length - 1;
    final ShortestPathFinder.Result before = find(s, t, Vertex.DIRECTION.OUT);
    assertThat(before.engine()).isEqualTo(ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);

    database.begin();
    // break the path the hierarchy found: every road on it becomes very expensive
    final List<RID> path = before.vertices();
    final Map<RID, double[]> saved = new HashMap<>();
    for (int i = 0; i + 1 < path.size(); i++)
      for (final Edge e : path.get(i).asVertex().getEdges(Vertex.DIRECTION.OUT, "ROAD"))
        if (e.getIn().equals(path.get(i + 1))) {
          saved.put(e.getIdentity(), roads.get(e.getIdentity()).clone());
          roads.get(e.getIdentity())[2] = 10_000;
          e.modify().set("distance", 10_000.0).save();
        }
    final ShortestPathFinder.Result inside = find(s, t, Vertex.DIRECTION.OUT);
    assertThat(inside.engine()).isEqualTo(ShortestPathFinder.Engine.RECORDS);
    assertThat(inside.weight()).isCloseTo(reference(s, Vertex.DIRECTION.OUT)[t], within(EPS));
    assertThat(inside.weight()).isGreaterThan(before.weight());
    database.rollback();
    roads.putAll(saved);

    final ShortestPathFinder.Result after = find(s, t, Vertex.DIRECTION.OUT);
    assertThat(after.engine()).isEqualTo(ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
    assertThat(after.weight()).isCloseTo(before.weight(), within(EPS));
  }

  @Test
  void ddlSqlFunctionCypherProcedureAndReopen() {
    grid(12, new Random(2));
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW roads EDGE TYPES (ROAD) UPDATE MODE SYNCHRONOUS CCH (distance)");
    GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "roads");
    assertThat(view).isNotNull();
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getEdgePropertyFilter()).contains("distance");
    ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch).isNotNull();
    assertThat(cch.awaitReady(true, 60, TimeUnit.SECONDS)).isTrue();

    final int s = 3;
    final int t = junctions.length - 2;
    final double expected = reference(s, Vertex.DIRECTION.OUT)[t];

    // no edge types: every type of the database, rails included, which the ROAD-only view cannot answer for
    try (final ResultSet rs = database.query("sql", "SELECT cchShortestPath(?, ?, 'distance', 'OUT') AS path", junctions[s],
        junctions[t])) {
      final List<RID> path = rs.next().getProperty("path");
      assertThat(path.getFirst()).isEqualTo(junctions[s]);
      assertThat(path.getLast()).isEqualTo(junctions[t]);
      assertThat(pathWeight(path, Vertex.DIRECTION.OUT, true)).isCloseTo(reference(s, Vertex.DIRECTION.OUT, true)[t],
          within(EPS));
    }
    try (final ResultSet rs = database.query("sql",
        "SELECT cchShortestPath(?, ?, 'distance', { direction: 'OUT', edgeTypeNames: ['ROAD'] }) AS path", junctions[s],
        junctions[t])) {
      assertThat(pathWeight(rs.next().getProperty("path"), Vertex.DIRECTION.OUT)).isCloseTo(expected, within(EPS));
    }
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT cchShortestPath(?, ?, 'distance', 'SIDEWAYS') AS path",
          junctions[s], junctions[t])) {
        rs.next();
      }
    }).hasStackTraceContaining("use OUT, IN or BOTH");

    final long queriesBefore = cch.getQueryCount();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Junction {i: $s}), (b:Junction {i: $t}) CALL algo.cch.shortestPath(a, b, 'ROAD', 'distance', 'OUT') "
            + "YIELD path, weight RETURN path, weight", Map.of("s", s, "t", t))) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("weight")).doubleValue()).isCloseTo(expected, within(EPS));
      final Map<String, Object> path = row.getProperty("path");
      assertThat((List<?>) path.get("relationships")).hasSize(((List<?>) path.get("nodes")).size() - 1);
    }
    assertThat(cch.getQueryCount()).isGreaterThan(queriesBefore);
    assertThat(cch.getFallbackCount()).isZero();

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = 'roads'")) {
      final List<Map<String, Object>> hierarchies = rs.next().getProperty("contractionHierarchies");
      assertThat(hierarchies).hasSize(1);
      assertThat(hierarchies.getFirst().get("weightProperty")).isEqualTo("distance");
      assertThat(hierarchies.getFirst().get("status")).isEqualTo("READY");
    }

    // reopen: the definition, and with it the hierarchy, comes back
    database.close();
    database = new DatabaseFactory(DB_PATH).open();
    view = GraphAnalyticalViewRegistry.get(database, "roads");
    assertThat(view).isNotNull();
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch).isNotNull();
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.isRestoredFromPersistedCsr()).isTrue();
    assertThat(cch.getTopologyRestoreCount()).as("the order persisted at close is reused, not computed again").isEqualTo(1);
    assertThat(cch.getTopologyBuildCount()).isZero();
    junctions = reloadJunctions();
    final ShortestPathFinder.Result reopened = find(s, t, Vertex.DIRECTION.OUT);
    assertThat(reopened.engine()).isEqualTo(ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
    assertThat(reopened.weight()).isCloseTo(expected, within(EPS));

    // dropping the view takes the persisted order with it
    final File orderFile = cch.orderFile();
    assertThat(orderFile).exists();
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW roads");
    assertThat(orderFile).doesNotExist();
  }

  @Test
  void aCorruptOrDifferentlyCertifiedOrderIsIgnored() throws Exception {
    grid(10, new Random(13));
    final GraphAnalyticalView view = syncView("roads");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    final File orderFile = cch.orderFile();

    // corrupt: a checksum that cannot match makes the file go away rather than be trusted
    Files.write(orderFile.toPath(), new byte[] { 0x43, 0x43, 0x48, 0x4F, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 4, 1, 2,
        3, 4, 0, 0, 0, 0, 0, 0, 0, 0 });
    assertThat(CCHOrderPersistence.load(database, orderFile, 7)).isNull();
    assertThat(orderFile).doesNotExist();

    // a valid file certified for another CSR is not used
    CCHOrderPersistence.save(database, orderFile, 41, new int[] { 2, 0, 1 });
    assertThat(CCHOrderPersistence.load(database, orderFile, 42)).isNull();
    assertThat(CCHOrderPersistence.load(database, orderFile, 41)).containsExactly(2, 0, 1);

    // an order that is not a permutation of the graph is never contracted in
    final CCHTopology topology = CCHTopology.build(3, new int[] { 0, 1 }, new int[] { 1, 2 }, 2, Long.MAX_VALUE,
        new int[] { 0, 0, 1 }, null);
    assertThat(topology).isNotNull();
    assertThat(topology.nodeAt).containsExactlyInAnyOrder(0, 1, 2);
  }

  @Test
  void ddlClauseRoundTripsAndCchStaysAnIdentifier() {
    final String sql = "CREATE GRAPH ANALYTICAL VIEW G1 EDGE TYPES (ROAD) EDGE PROPERTIES (distance) UPDATE MODE SYNCHRONOUS "
        + "CCH (distance, time)";
    final Statement statement = new SQLAntlrParser(null).parse(sql);
    assertThat(statement).isInstanceOf(CreateGraphAnalyticalViewStatement.class);
    assertThat(statement.toString()).isEqualTo(sql);
    final Statement copy = statement.copy();
    assertThat(copy.toString()).isEqualTo(sql);
    assertThat(copy).isEqualTo(statement);
    assertThat(new SQLAntlrParser(null).parse("CREATE GRAPH ANALYTICAL VIEW G1 EDGE TYPES (ROAD)"))
        .isNotEqualTo(statement);
    // the quoted form Studio writes
    final CreateGraphAnalyticalViewStatement quoted = (CreateGraphAnalyticalViewStatement) new SQLAntlrParser(null).parse(
        "CREATE GRAPH ANALYTICAL VIEW `G1` CCH (`travel time`)");
    assertThat(quoted.cchWeights[0].getStringValue()).isEqualTo("travel time");

    // the new keyword must not break a schema that already uses it as a name
    database.getSchema().createDocumentType("cch").createProperty("cch", Type.INTEGER);
    database.transaction(() -> database.command("sql", "INSERT INTO cch SET cch = 7"));
    try (final ResultSet rs = database.query("sql", "SELECT cch FROM cch WHERE cch = 7")) {
      assertThat(((Number) rs.next().getProperty("cch")).intValue()).isEqualTo(7);
    }
  }

  @Test
  void aGraphWithoutSmallSeparatorsIsRefusedAndStillAnswered() {
    database.getConfiguration().setValue(GlobalConfiguration.GAV_CCH_MAX_ARCS_PER_EDGE, 1);
    // a random graph of average degree 20: its separators are a sizeable fraction of it, the supergraph near complete
    final Random random = new Random(6);
    final int n = 1500;
    database.transaction(() -> {
      junctions = new RID[n];
      for (int i = 0; i < n; i++) {
        junctions[i] = database.newVertex("Junction").set("i", i).save().getIdentity();
        index.put(junctions[i], i);
      }
      for (int i = 0; i < n * 10; i++)
        road(random.nextInt(n), random.nextInt(n), 1 + random.nextInt(9));
    });

    final GraphAnalyticalView view = syncView("social");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isFalse();
    assertThat(cch.getStatus()).isEqualTo(ContractionHierarchy.Status.UNSUITABLE);
    assertThat(cch.getStatusReason()).contains("supergraph arcs");

    assertRandomPairs(random, 30, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.VIEW);
  }

  /**
   * The bidirectional Dijkstra that answers on the view's columns when no hierarchy can reads one captured snapshot for
   * the whole search, overlay included: added vertices and edges, deleted edges.
   */
  @Test
  void viewDijkstraReadsOneSnapshotWithItsOverlay() {
    grid(12, new Random(23));
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("plain").withEdgeTypes("ROAD")
        .withEdgeProperties("distance").withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
    final Random random = new Random(31);
    assertRandomPairs(random, 60, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.VIEW);

    database.transaction(() -> {
      final List<RID> all = new ArrayList<>(roads.keySet());
      for (int i = 0; i < 15; i++) {
        final RID edge = all.get(random.nextInt(all.size()));
        if (roads.remove(edge) != null)
          edge.asEdge().delete();
      }
      final MutableVertex v = database.newVertex("Junction").set("i", junctions.length).save();
      junctions = Arrays.copyOf(junctions, junctions.length + 1);
      junctions[junctions.length - 1] = v.getIdentity();
      index.put(v.getIdentity(), junctions.length - 1);
      road(junctions.length - 1, 3, 0.5);
      road(100, junctions.length - 1, 0.5);
    });
    assertThat(view.hasPendingChanges()).isTrue();
    for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values())
      assertRandomPairs(random, 60, direction, ShortestPathFinder.Engine.VIEW);
  }

  /**
   * A negative weight makes its edge unusable (a shortest path is not defined over it) and a missing weight counts 1, on
   * the hierarchy exactly as on the Dijkstra fallbacks: a negative shortcut must not leak into the customized metric.
   */
  @Test
  void negativeAndMissingWeightsOnAHierarchy() {
    grid(10, new Random(41));
    database.transaction(() -> {
      // a very negative shortcut across the grid: walking it would make every far route look cheap
      final MutableEdge negative = junctions[0].asVertex().modify().newEdge("ROAD", junctions[99].asVertex());
      negative.set("distance", -1000.0).save();
      // a weightless shortcut counts 1
      final MutableEdge missing = junctions[5].asVertex().modify().newEdge("ROAD", junctions[94].asVertex());
      missing.save();
      roads.put(missing.getIdentity(), new double[] { 5, 94, 1.0 });
    });
    final GraphAnalyticalView view = syncView("roads");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(true, 60, TimeUnit.SECONDS)).isTrue();

    final ShortestPathFinder.Result corner = find(0, 99, Vertex.DIRECTION.OUT);
    assertThat(corner.engine()).isEqualTo(ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
    assertThat(corner.weight()).isGreaterThan(0).isCloseTo(reference(0, Vertex.DIRECTION.OUT)[99], within(EPS));
    assertThat(find(5, 94, Vertex.DIRECTION.OUT).weight()).isEqualTo(1.0);

    final Random random = new Random(43);
    for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values())
      assertRandomPairs(random, 80, direction, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);

    // the records fallback agrees
    database.begin();
    database.newVertex("Junction").set("i", -1).save(); // any uncommitted change withholds the view
    final ShortestPathFinder.Result records = find(0, 99, Vertex.DIRECTION.OUT);
    assertThat(records.engine()).isEqualTo(ShortestPathFinder.Engine.RECORDS);
    assertThat(records.weight()).isCloseTo(corner.weight(), within(EPS));
    database.rollback();
  }

  @Test
  void otherEdgeTypesAndUnmatchedRequestsFallBack() {
    grid(8, new Random(1));
    final GraphAnalyticalView view = syncView("roads");
    assertThat(view.getContractionHierarchy("distance", "ROAD").awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    // another weight, or every edge type of the database, is not what the hierarchy routes over
    assertThat(view.getContractionHierarchy("other", "ROAD")).isNull();
    assertThat(ContractionHierarchy.find(database, "distance")).isNull();
    assertThat(ContractionHierarchy.find(database, "distance", "RAIL")).isNull();
    assertThat(ContractionHierarchy.find(database, "distance", "ROAD")).isNotNull();

    final ShortestPathFinder.Result anyType = ShortestPathFinder.find(database, junctions[0], junctions[20], "distance",
        Vertex.DIRECTION.OUT, null, null);
    assertThat(anyType.engine()).isNotEqualTo(ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
    assertThat(anyType.weight()).isCloseTo(reference(0, Vertex.DIRECTION.OUT, true)[20], within(EPS));
  }

  @Test
  void concurrentQueriesWhileWeightsChange() throws Exception {
    grid(15, new Random(9));
    final GraphAnalyticalView view = syncView("roads");
    final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread[] readers = new Thread[4];
    final long deadline = System.currentTimeMillis() + 3_000;
    for (int r = 0; r < readers.length; r++) {
      final int seed = r;
      readers[r] = new Thread(() -> {
        final Random random = new Random(seed);
        try {
          while (System.currentTimeMillis() < deadline && failure.get() == null) {
            final int s = random.nextInt(junctions.length);
            final int t = random.nextInt(junctions.length);
            final ShortestPathFinder.Result result = find(s, t, Vertex.DIRECTION.OUT);
            if (result != null) {
              assertThat(result.vertices().getFirst()).isEqualTo(junctions[s]);
              assertThat(result.vertices().getLast()).isEqualTo(junctions[t]);
            }
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      });
      readers[r].setDaemon(true);
      readers[r].start();
    }

    final Random random = new Random(77);
    final List<RID> all = new ArrayList<>(roads.keySet());
    try {
      while (System.currentTimeMillis() < deadline && failure.get() == null)
        database.transaction(() -> {
          final RID edge = all.get(random.nextInt(all.size()));
          final double weight = 1 + random.nextInt(50);
          roads.get(edge)[2] = weight;
          edge.asEdge().modify().set("distance", weight).save();
        });
    } catch (final RuntimeException e) {
      failure.compareAndSet(null, e);
    } finally {
      // a failed writer has set the shared failure flag, which ends the readers' loop as well
      for (final Thread reader : readers)
        reader.join(60_000);
    }
    assertThat(failure.get()).isNull();

    assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();
    assertRandomPairs(random, 100, Vertex.DIRECTION.OUT, ShortestPathFinder.Engine.CONTRACTION_HIERARCHY);
  }

  // ---------------------------------------------------------------------------------------------------------------

  private GraphAnalyticalView syncView(final String name) {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName(name).withEdgeTypes("ROAD")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).withContractionHierarchy("distance").build();
    assertThat(view.getEdgePropertyFilter()).contains("distance");
    return view;
  }

  private ShortestPathFinder.Result find(final int s, final int t, final Vertex.DIRECTION direction) {
    return ShortestPathFinder.find(database, junctions[s], junctions[t], "distance", direction, new String[] { "ROAD" }, null);
  }

  private void assertRandomPairs(final Random random, final int pairs, final Vertex.DIRECTION direction,
      final ShortestPathFinder.Engine engine) {
    for (int p = 0; p < pairs; p++) {
      final int s = random.nextInt(junctions.length);
      final int t = random.nextInt(junctions.length);
      final double expected = reference(s, direction)[t];
      final ShortestPathFinder.Result result = find(s, t, direction);
      if (expected == Double.POSITIVE_INFINITY) {
        assertThat(result).as("%d -> %d %s", s, t, direction).isNull();
        continue;
      }
      assertThat(result).as("%d -> %d %s", s, t, direction).isNotNull();
      if (s != t && engine != null)
        assertThat(result.engine()).isEqualTo(engine);
      assertThat(result.weight()).as("%d -> %d %s", s, t, direction).isCloseTo(expected, within(EPS));
      assertThat(result.vertices().getFirst()).isEqualTo(junctions[s]);
      assertThat(result.vertices().getLast()).isEqualTo(junctions[t]);
      assertThat(pathWeight(result.vertices(), direction)).isCloseTo(expected, within(EPS));
    }
  }

  /** The weight of a vertex path walked over the cheapest usable road between each pair, per the reference graph. */
  private double pathWeight(final List<RID> path, final Vertex.DIRECTION direction) {
    return pathWeight(path, direction, false);
  }

  private double pathWeight(final List<RID> path, final Vertex.DIRECTION direction, final boolean withRails) {
    final List<double[]> edges = new ArrayList<>(roads.values());
    if (withRails)
      edges.addAll(rails);
    double total = 0;
    for (int i = 0; i + 1 < path.size(); i++) {
      final int a = index.get(path.get(i));
      final int b = index.get(path.get(i + 1));
      double best = Double.POSITIVE_INFINITY;
      for (final double[] road : edges) {
        final int tail = (int) road[0];
        final int head = (int) road[1];
        final boolean forward = tail == a && head == b;
        final boolean backward = tail == b && head == a;
        if ((direction == Vertex.DIRECTION.OUT && forward) || (direction == Vertex.DIRECTION.IN && backward)
            || (direction == Vertex.DIRECTION.BOTH && (forward || backward)))
          best = Math.min(best, road[2]);
      }
      assertThat(best).as("a road joins %d and %d", a, b).isLessThan(Double.POSITIVE_INFINITY);
      total += best;
    }
    return total;
  }

  private double[] reference(final int source, final Vertex.DIRECTION direction) {
    return reference(source, direction, false);
  }

  private double[] reference(final int source, final Vertex.DIRECTION direction, final boolean withRails) {
    final List<double[]> edges = new ArrayList<>(roads.values());
    if (withRails)
      edges.addAll(rails);
    final int n = junctions.length;
    final double[] dist = new double[n];
    Arrays.fill(dist, Double.POSITIVE_INFINITY);
    dist[source] = 0;
    final List<List<double[]>> adjacency = new ArrayList<>(n);
    for (int i = 0; i < n; i++)
      adjacency.add(new ArrayList<>());
    for (final double[] road : edges) {
      final int tail = (int) road[0];
      final int head = (int) road[1];
      if (direction != Vertex.DIRECTION.IN)
        adjacency.get(tail).add(new double[] { head, road[2] });
      if (direction != Vertex.DIRECTION.OUT)
        adjacency.get(head).add(new double[] { tail, road[2] });
    }
    final PriorityQueue<double[]> heap = new PriorityQueue<>((a, b) -> Double.compare(a[0], b[0]));
    heap.add(new double[] { 0, source });
    while (!heap.isEmpty()) {
      final double[] top = heap.poll();
      final int u = (int) top[1];
      if (top[0] > dist[u])
        continue;
      for (final double[] arc : adjacency.get(u)) {
        final int v = (int) arc[0];
        final double nd = top[0] + arc[1];
        if (nd < dist[v]) {
          dist[v] = nd;
          heap.add(new double[] { nd, v });
        }
      }
    }
    return dist;
  }

  /** A side x side grid of junctions with one-way streets here and there, plus a few rail links the views ignore. */
  private void grid(final int side, final Random random) {
    database.transaction(() -> {
      junctions = new RID[side * side];
      for (int i = 0; i < junctions.length; i++) {
        junctions[i] = database.newVertex("Junction").set("i", i).save().getIdentity();
        index.put(junctions[i], i);
      }
      for (int r = 0; r < side; r++)
        for (int c = 0; c < side; c++) {
          final int u = r * side + c;
          final int[] neighbours = { c + 1 < side ? u + 1 : -1, r + 1 < side ? u + side : -1 };
          for (final int v : neighbours) {
            if (v < 0)
              continue;
            final boolean forward = random.nextInt(6) != 0;
            final boolean backward = !forward || random.nextInt(6) != 0;
            if (forward)
              road(u, v, 1 + random.nextInt(30));
            if (backward)
              road(v, u, 1 + random.nextInt(30));
          }
        }
      for (int i = 0; i < side; i++) {
        final int from = random.nextInt(junctions.length);
        final int to = random.nextInt(junctions.length);
        junctions[from].asVertex().modify().newEdge("RAIL", junctions[to].asVertex()).save();
        rails.add(new double[] { from, to, 1.0 });
      }
    });
  }

  private void road(final int from, final int to, final double distance) {
    final MutableEdge edge = junctions[from].asVertex().modify().newEdge("ROAD", junctions[to].asVertex());
    edge.set("distance", distance).save();
    roads.put(edge.getIdentity(), new double[] { from, to, distance });
  }

  private RID[] reloadJunctions() {
    final RID[] result = new RID[junctions.length];
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, i FROM Junction")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result[((Number) row.getProperty("i")).intValue()] = row.getProperty("rid");
      }
    }
    return result;
  }
}
