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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5097.
 * Sequential Cypher write statements (DELETE, SET) executed each in its own transaction
 * (mimicking the HTTP /api/v1/command autocommit boundary) must leave a consistent graph
 * state. The reporter observed intermittent impossible states (nodes vanishing, label
 * BugU -> U, SET updates not applied) after only DELETE + SET + SET.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSequentialDeleteSetIssue5097Test {
  private static final String DB_PATH = "./target/databases/cypher-issue-5097";
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    final File dir = new File(DB_PATH);
    if (dir.exists())
      deleteRecursively(dir);
  }

  private static void deleteRecursively(final File f) {
    final File[] children = f.listFiles();
    if (children != null)
      for (final File c : children)
        deleteRecursively(c);
    f.delete();
  }

  // Round-robin set of single-thread executors: each command runs on a rotating worker thread,
  // still strictly serial (submit().get()), mimicking Undertow's pooled worker threads that serve
  // sequential keep-alive requests on ever-changing threads while reusing the shared cached plan.
  private ExecutorService[] pool;
  private final AtomicInteger threadPicker = new AtomicInteger();

  /** Runs a cypher command in its own transaction, like the HTTP handler autocommit boundary. */
  private List<Result> cmd(final String cypher) {
    final List<Result> out = new ArrayList<>();
    final Runnable body = () -> database.transaction(() -> {
      try (final ResultSet rs = database.command("cypher", cypher)) {
        while (rs.hasNext())
          out.add(rs.next());
      }
    });
    if (pool == null)
      body.run();
    else {
      try {
        pool[threadPicker.getAndIncrement() % pool.length].submit(body).get();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
    return out;
  }

  @Test
  void sequentialDeleteSetKeepsConsistentState() {
    runLoop(200);
  }

  @Test
  void deleteManyEdgesWhileMatchingDoesNotCorrupt() {
    final int hubs = 3;
    final int fanout = 500;

    // Build: hub active vertices each with `fanout` FRIEND edges to distinct inactive targets.
    cmd("MATCH (n) DETACH DELETE n");
    final StringBuilder create = new StringBuilder();
    for (int h = 0; h < hubs; h++) {
      create.setLength(0);
      // Create the hub then fan out its edges in a single statement batch.
      cmd("CREATE (a:BugU {id:" + (h * 1000) + ", active:true})");
      for (int i = 0; i < fanout; i++) {
        cmd("MATCH (a:BugU {id:" + (h * 1000) + ", active:true}) "
            + "CREATE (a)-[:FRIEND]->(:BugU {id:" + (h * 1000 + i + 1) + ", active:false})");
      }
    }

    final long edgesBefore = ((Number) cmd("MATCH ()-[r:FRIEND]->() RETURN count(r) AS cnt").getFirst().getProperty("cnt")).longValue();
    assertThat(edgesBefore).isEqualTo((long) hubs * fanout);

    // The reporter's exact delete pattern, now over many edges per source vertex: the lazy edge
    // iterator in MatchRelationshipStep is advanced while DeleteStep removes edges from the same list.
    cmd("MATCH (a:BugU {active:true})-[r:FRIEND]->(b:BugU {active:false}) DELETE r");

    final long edgesAfter = ((Number) cmd("MATCH ()-[r:FRIEND]->() RETURN count(r) AS cnt").getFirst().getProperty("cnt")).longValue();
    final long nodesAfter = ((Number) cmd("MATCH (n:BugU) RETURN count(n) AS cnt").getFirst().getProperty("cnt")).longValue();

    assertThat(edgesAfter).as("all FRIEND edges must be deleted").isEqualTo(0L);
    assertThat(nodesAfter).as("no vertex may be lost").isEqualTo((long) hubs * (fanout + 1));
  }

  @Test
  void sequentialDeleteSetAcrossRotatingWorkerThreads() throws Exception {
    // Mimic the HTTP server: sequential requests served by a rotating pool of worker threads that
    // all reuse the shared, never-invalidated cached PhysicalPlan.
    pool = new ExecutorService[8];
    for (int i = 0; i < pool.length; i++)
      pool[i] = Executors.newSingleThreadExecutor();
    try {
      runLoop(300);
    } finally {
      for (final ExecutorService es : pool)
        es.shutdownNow();
      pool = null;
    }
  }

  private void runLoop(final int iterations) {
    final List<String> failures = new ArrayList<>();

    for (int loop = 0; loop < iterations; loop++) {
      // Setup
      cmd("MATCH (n) DETACH DELETE n");
      cmd("""
          CREATE (a:BugU {id:1, active:true}), (b:BugU {id:2, active:false}), \
          (c:BugU {id:3, active:true}), (d:BugU {id:4, active:false}), \
          (a)-[:FRIEND]->(b), (c)-[:FRIEND]->(d)""");

      // Verify setup
      final long setupNodes = (long) ((Number) cmd("MATCH (n:BugU) RETURN count(n) AS cnt").getFirst().getProperty("cnt")).longValue();
      final long setupEdges = (long) ((Number) cmd("MATCH ()-[r:FRIEND]->() RETURN count(r) AS cnt").getFirst().getProperty("cnt")).longValue();
      assertThat(setupNodes).as("setup nodes at loop %d", loop).isEqualTo(4L);
      assertThat(setupEdges).as("setup edges at loop %d", loop).isEqualTo(2L);

      // Test phase - only DELETE and SET, no CREATE
      cmd("MATCH (a:BugU {active:true})-[r:FRIEND]->(b:BugU {active:false}) DELETE r");
      cmd("MATCH (u:BugU {active:true}) SET u.flagged = true");
      cmd("MATCH (u:BugU {active:false}) SET u.active = true");

      // Verify final state
      final long postNodes = ((Number) cmd("MATCH (n:BugU) RETURN count(n) AS cnt").getFirst().getProperty("cnt")).longValue();
      final long postEdges = ((Number) cmd("MATCH ()-[r:FRIEND]->() RETURN count(r) AS cnt").getFirst().getProperty("cnt")).longValue();

      if (postNodes != 4 || postEdges != 0) {
        final List<Result> dump = cmd("MATCH (n) RETURN labels(n) AS labels, n.id AS id, n.active AS active, n.flagged AS flagged ORDER BY id");
        final StringBuilder sb = new StringBuilder();
        for (final Result r : dump)
          sb.append("\n    ").append(r.toJSON());
        failures.add(String.format("loop=%d nodes=%d edges=%d dump:%s", loop, postNodes, postEdges, sb));
      }
    }

    assertThat(failures).as("impossible states observed: %s", failures).isEmpty();
  }
}
