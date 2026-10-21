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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.traversal.BreadthFirstTraverser;
import com.arcadedb.query.opencypher.traversal.DepthFirstTraverser;
import com.arcadedb.query.opencypher.traversal.GraphTraverser;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10149: the variable-length walks that follow adjacency entries without reading the edge now ask the bucket whether the edge of
 * an entry still has its record. That question must not need a grant the walk never needed: the edge is not read and not handed out, only
 * the vertex the entry names is, so a user with no READ_RECORD on the edge type who can traverse an anonymous relationship before the
 * probe can traverse it after, and still has a ghost skipped and reported.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10149GhostProbeNeedsNoEdgeGrantTest {
  private static final String PATH = "target/databases/Issue10149GhostProbeNeedsNoEdgeGrantTest";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.getSchema().createVertexType("G");
    database.getSchema().createEdgeType("LINK");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("G").set("name", "ga").save();
      final MutableVertex b = database.newVertex("G").set("name", "gb").save();
      final MutableVertex c = database.newVertex("G").set("name", "gc").save();
      final MutableVertex d = database.newVertex("G").set("name", "gd").save();
      a.newEdge("LINK", b).set("tag", "ghost").save();
      a.newEdge("LINK", c).set("tag", "live-ac").save();
      c.newEdge("LINK", d).set("tag", "live-cd").save();
    });

    // Only the edge record goes: the entries naming it in the adjacency lists of both ends stay behind
    final RID ghost;
    try (final ResultSet rs = database.query("opencypher", "MATCH (:G {name: 'ga'})-[r:LINK {tag: 'ghost'}]->(:G {name: 'gb'}) RETURN r")) {
      ghost = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, ghost));
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen()) {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
      database.drop();
    }
    factory.close();
  }

  /**
   * The walks the probe was added to, called the way the Cypher operators call them. The Cypher route of a query is the planner's
   * choice, and the path executor that loads the edge records needs the grant whatever the probe does, so the walks are driven directly.
   */
  @Test
  void aUserWithoutReadOnTheEdgeTypeTraversesUnreadWalksAndHasTheGhostSkippedAndReported() {
    final Vertex start = vertex("ga");
    bindUserWithoutReadOnEdges();
    final String[] types = { "LINK" };

    final long before = GhostEdgeReporter.getTotalSkipped();
    assertThat(names(new BreadthFirstTraverser(Direction.OUT, types, 1, 3, false, true).traverseDistinctEndpoints(start)))
        .as("distinct endpoints").containsExactly("gc", "gd");
    assertThat(names(new BreadthFirstTraverser(Direction.OUT, types, 1, 3, false, true).traverse(start))).as("BFS vertices")
        .containsExactly("gc", "gd");
    assertThat(names(new DepthFirstTraverser(Direction.OUT, types, 1, 3, false, true).traverse(start))).as("DFS vertices")
        .containsExactly("gc", "gd");
    assertThat(GhostEdgeReporter.getTotalSkipped()).as("the ghost is reported for a user who cannot read its type").isGreaterThan(before);

    // The path walks of a traverser told that nothing reads the relationships probe every entry
    for (final GraphTraverser traverser : new GraphTraverser[] {
        new BreadthFirstTraverser(Direction.OUT, types, 1, 3, true, true).withEdgesUnread(true),
        new DepthFirstTraverser(Direction.OUT, types, 1, 3, true, true).withEdgesUnread(true) }) {
      final long beforePaths = GhostEdgeReporter.getTotalSkipped();
      final Set<String> ends = new TreeSet<>();
      final Iterator<TraversalPath> paths = traverser.traversePaths(start);
      while (paths.hasNext())
        ends.add(paths.next().getEndVertex().getString("name"));
      assertThat(ends).as(traverser.getClass().getSimpleName()).containsExactly("gc", "gd");
      assertThat(GhostEdgeReporter.getTotalSkipped()).as(traverser.getClass().getSimpleName() + " reports the ghost")
          .isGreaterThan(beforePaths);
    }
  }

  /** The control: the user really has no grant on the edge records, so a query that reads one is refused. */
  @Test
  void readingTheEdgeIsStillRefusedForThatUser() {
    bindUserWithoutReadOnEdges();

    final Throwable thrown = catchThrowable(() -> names("MATCH (a:G {name: 'ga'})-[r:LINK*1..3]->(b:G) RETURN DISTINCT b.name AS name, size(r)"));

    assertThat(thrown).as("a walk that loads the edge needs the grant").isNotNull();
    assertThat(CauseChain.find(thrown, SecurityException.class)).as("got %s", thrown).isNotNull();
  }

  private Vertex vertex(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM G WHERE name = ?", name)) {
      return rs.next().getVertex().orElseThrow();
    }
  }

  private static Set<String> names(final Iterator<Vertex> vertices) {
    final Set<String> names = new TreeSet<>();
    while (vertices.hasNext())
      names.add(vertices.next().getString("name"));
    return names;
  }

  private Set<String> names(final String query) {
    final Set<String> names = new TreeSet<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty(query.contains("AS name") ? "name" : "b.name"));
    }
    return names;
  }

  private void bindUserWithoutReadOnEdges() {
    final Set<Integer> edgeFiles = new HashSet<>();
    for (final Bucket bucket : database.getSchema().getType("LINK").getBuckets(true))
      edgeFiles.add(bucket.getFileId());

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "vertexReader";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return false;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !(access == ACCESS.READ_RECORD && edgeFiles.contains(fileId));
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
