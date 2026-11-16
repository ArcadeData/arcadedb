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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12250: a Graph Analytical View follows the commits of its own node through record events, and a commit that reaches the
 * node as pages - a follower applying the replicated log, the leader applying a commit a follower made in its own transaction -
 * fires none. A view restored when a follower opened its database stayed READY on the graph as of that open, and SQL
 * {@code out()} and Cypher traversals on that node were answered from it. Such a commit now marks the view STALE before the node
 * reports the entry applied, and an incremental view rebuilds from the records.
 */
class Issue12250GavFollowsReplicatedCommitsIT extends BaseRaftHATest {
  private static final String[] VIEWS = { "GSync", "GAsync", "GOff" };

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  void aViewRestoredOnAFollowerAtOpenFollowsTheReplicatedCommits() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    createGraph(leader);
    addEdges(leader, 0, 20);
    leader.command("sql", "CREATE GRAPH ANALYTICAL VIEW GSync UPDATE MODE SYNCHRONOUS");
    leader.command("sql", "CREATE GRAPH ANALYTICAL VIEW GAsync UPDATE MODE ASYNCHRONOUS");
    leader.command("sql", "CREATE GRAPH ANALYTICAL VIEW GOff UPDATE MODE OFF");
    waitForAllServers();

    // The follower opens the database again, as a restart does: it restores the three views from their definitions
    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    closeAndDeregister(followerIndex);
    getServer(followerIndex).getDatabaseForOperator(getDatabaseName());
    final Database follower = getServerDatabase(followerIndex, getDatabaseName());
    for (final String name : VIEWS) {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(follower, name);
      assertThat(view).as("view %s restored on the follower", name).isNotNull();
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).as("view %s ready on the follower", name).isTrue();
      assertThat(view.getEdgeCount()).as("view %s on the follower", name).isEqualTo(20);
    }
    assertEdgeCounts(follower, 20, 4);

    addEdges(leader, 20, 15);
    waitForAllServers();

    // STALE as soon as the follower applied the entries, before anything queried them: no view answers from the graph as of the open
    for (final String name : VIEWS)
      assertThat(GraphAnalyticalViewRegistry.get(follower, name).getStatus()).as("view %s on the follower", name)
          .isEqualTo(GraphAnalyticalView.Status.STALE);
    assertEdgeCounts(follower, 35, 7);

    // An incremental view rebuilds from the records when it is asked for, by a query or a wait: the first query may only reach
    // one of the views, so each is waited for here. The OFF view stays STALE until it is rebuilt, as on the leader
    for (final String name : new String[] { "GSync", "GAsync" }) {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(follower, name);
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).as("view %s rebuilt on the follower", name).isTrue();
      assertThat(view.getEdgeCount()).as("view %s on the follower", name).isEqualTo(35);
    }
    assertThat(GraphAnalyticalViewRegistry.get(follower, "GOff").getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
    assertEdgeCounts(follower, 35, 7);

    // The rebuilt views keep following: one more entry, read at once again
    addEdges(leader, 35, 5);
    waitForAllServers();
    assertEdgeCounts(follower, 40, 8);
  }

  @Test
  void theLeaderViewFollowsACommitAFollowerMadeInItsOwnTransaction() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    createGraph(leader);
    addEdges(leader, 0, 20);
    leader.command("sql", "CREATE GRAPH ANALYTICAL VIEW GSync UPDATE MODE SYNCHRONOUS");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(leader, "GSync");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertEdgeCounts(leader, 20, 4);
    waitForAllServers();

    // Committed on the follower: the leader applies the replicated pages, with no record event
    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    addEdges(getServerDatabase(followerIndex, getDatabaseName()), 20, 15);
    waitForAllServers();

    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
    assertEdgeCounts(leader, 35, 7);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      assertEdgeCounts(leader, 35, 7);
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(view.getEdgeCount()).isEqualTo(35);
    });
    assertEdgeCounts(leader, 35, 7);
  }

  private static void createGraph(final Database leader) {
    leader.command("sql", "CREATE VERTEX TYPE H");
    leader.command("sql", "CREATE PROPERTY H.h STRING");
    leader.command("sql", "CREATE INDEX ON H (h) UNIQUE");
    leader.command("sql", "CREATE VERTEX TYPE Lf");
    leader.command("sql", "CREATE EDGE TYPE L");
    leader.transaction(() -> {
      for (int h = 0; h < 5; h++)
        leader.newVertex("H").set("h", "h" + h).save();
    });
  }

  /** Edge {@code n} goes from hub {@code n % 5} to a new leaf, one transaction per edge. */
  private static void addEdges(final Database database, final int from, final int count) {
    for (int n = from; n < from + count; n++) {
      final int edge = n;
      database.transaction(() -> {
        final Vertex hub;
        try (final ResultSet rs = database.query("sql", "SELECT FROM H WHERE h = ?", "h" + edge % 5)) {
          hub = rs.next().getVertex().orElseThrow();
        }
        final MutableVertex leaf = database.newVertex("Lf").set("l", edge).save();
        hub.newEdge("L", leaf);
      });
    }
  }

  /** The edges of every hub by SQL out() and by Cypher, and the edges of hub h0 by Cypher. */
  private static void assertEdgeCounts(final Database database, final long all, final long ofHubZero) {
    assertThat(count(database, "sql", "SELECT count(@rid) AS c FROM L")).as("records").isEqualTo(all);
    assertThat(count(database, "sql", "SELECT sum(out('L').size()) AS c FROM H")).as("SQL out()").isEqualTo(all);
    assertThat(count(database, "cypher", "MATCH (:H)-[r:L]->() RETURN count(r) AS c")).as("Cypher").isEqualTo(all);
    assertThat(count(database, "cypher", "MATCH (h:H {h: 'h0'})-->(x) RETURN count(x) AS c")).as("Cypher from h0")
        .isEqualTo(ofHubZero);
  }

  private static long count(final Database database, final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /** As a restart does to the database: closed and forgotten, opened by the next request that names it. */
  private void closeAndDeregister(final int serverIndex) {
    final ArcadeDBServer server = getServer(serverIndex);
    final ServerDatabase database = server.getDatabaseForOperator(getDatabaseName());
    database.getEmbedded().close();
    server.removeDatabase(getDatabaseName());
  }
}
