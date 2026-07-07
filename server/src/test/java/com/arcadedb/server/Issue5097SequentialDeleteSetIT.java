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
package com.arcadedb.server;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Server integration test for GitHub issue #5097.
 * <p>
 * Reproduces the reporter's Python stress test: a single persistent HTTP session (keep-alive)
 * runs sequential Cypher write statements (DELETE, SET) - no CREATE in the test phase - and some
 * clean iterations left an impossible final graph state (all nodes gone, label BugU -> U, SETs
 * not applied). Neo4j returns the correct 4 nodes / 0 edges in 500/500 iterations.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5097SequentialDeleteSetIT extends BaseGraphServerTest {

  @Override
  protected String getDatabaseName() {
    return "issue5097";
  }

  @Override
  protected void populateDatabase() {
    // No pre-population: the Cypher CREATE in the loop auto-creates the BugU vertex type.
  }

  private final String auth = "Basic " + Base64.getEncoder().encodeToString(
      ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());

  private final List<String> errors = Collections.synchronizedList(new ArrayList<>());

  /** Executes one Cypher command over the shared keep-alive client; returns the result array (or null on error). */
  private JSONArray cmd(final HttpClient client, final String cypher) throws Exception {
    final JSONObject payload = new JSONObject().put("language", "cypher").put("command", cypher);
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:2480/api/v1/command/" + getDatabaseName()))
        .header("Authorization", auth)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    final JSONObject body = new JSONObject(response.body());
    if (body.has("error")) {
      errors.add(cypher + " => " + body.getString("detail", body.getString("exception", "?")));
      return null;
    }
    return body.getJSONArray("result");
  }

  private long count(final HttpClient client, final String cypher) throws Exception {
    final JSONArray r = cmd(client, cypher);
    return r.getJSONObject(0).getLong("cnt");
  }

  @Test
  void sequentialDeleteSetKeepsConsistentStateOverHttp() throws Exception {
    final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    final int iterations = Integer.getInteger("issue5097.iterations", 300);
    final List<String> failures = new ArrayList<>();

    for (int loop = 0; loop < iterations; loop++) {
      // Setup
      if (cmd(client, "MATCH (n) DETACH DELETE n") == null)
        continue;
      if (cmd(client, "CREATE (a:BugU {id:1, active:true}), (b:BugU {id:2, active:false}), "
          + "(c:BugU {id:3, active:true}), (d:BugU {id:4, active:false}), "
          + "(a)-[:FRIEND]->(b), (c)-[:FRIEND]->(d)") == null)
        continue;

      // Verify setup
      if (count(client, "MATCH (n:BugU) RETURN count(n) AS cnt") != 4)
        continue;
      if (count(client, "MATCH ()-[r:FRIEND]->() RETURN count(r) AS cnt") != 2)
        continue;

      // Test phase - only DELETE and SET, no CREATE
      if (cmd(client, "MATCH (a:BugU {active:true})-[r:FRIEND]->(b:BugU {active:false}) DELETE r") == null)
        continue;
      if (cmd(client, "MATCH (u:BugU {active:true}) SET u.flagged = true") == null)
        continue;
      if (cmd(client, "MATCH (u:BugU {active:false}) SET u.active = true") == null)
        continue;

      // Verify final state
      final long postNodes = count(client, "MATCH (n:BugU) RETURN count(n) AS cnt");
      final long postEdges = count(client, "MATCH ()-[r:FRIEND]->() RETURN count(r) AS cnt");

      if (postNodes != 4 || postEdges != 0) {
        final JSONArray dump = cmd(client,
            "MATCH (n) RETURN labels(n) AS labels, n.id AS id, n.active AS active, n.flagged AS flagged ORDER BY id");
        failures.add(String.format("loop=%d nodes=%d edges=%d dump=%s", loop, postNodes, postEdges,
            dump != null ? dump.toString() : "<err>"));
      }
    }

    assertThat(failures).as("impossible states observed (%d): %s", failures.size(), failures).isEmpty();
  }

  /**
   * Concurrency amplifier: N independent HTTP sessions each drive the reporter's exact loop, but on
   * its OWN vertex label and id-space so there is no <em>logical</em> conflict between sessions - each
   * session's final state is deterministic in isolation, exactly like the single-session reporter.
   * They share the engine (schema, dictionary, page manager, edge engine, plan/statement caches), so
   * any engine-level race that produces the reporter's "impossible" states will surface here.
   */
  @Test
  void concurrentSessionsIsolatedLabelsKeepConsistentState() throws Exception {
    final int workers = Integer.getInteger("issue5097.workers", 6);
    final int iterations = Integer.getInteger("issue5097.iterations", 100);
    final List<String> failures = new CopyOnWriteArrayList<>();
    final List<String> concErrors = new CopyOnWriteArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int w = 0; w < workers; w++) {
      final String label = "BugU" + w;
      final Thread t = new Thread(() -> {
        final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        try {
          start.await();
          for (int loop = 0; loop < iterations; loop++) {
            if (cmd(client, "MATCH (n:" + label + ") DETACH DELETE n") == null) { continue; }
            if (cmd(client, "CREATE (a:" + label + " {id:1, active:true}), (b:" + label + " {id:2, active:false}), "
                + "(c:" + label + " {id:3, active:true}), (d:" + label + " {id:4, active:false}), "
                + "(a)-[:FRIEND]->(b), (c)-[:FRIEND]->(d)") == null) { continue; }
            if (count(client, "MATCH (n:" + label + ") RETURN count(n) AS cnt") != 4) { continue; }

            if (cmd(client, "MATCH (a:" + label + " {active:true})-[r:FRIEND]->(b:" + label + " {active:false}) DELETE r") == null) { continue; }
            if (cmd(client, "MATCH (u:" + label + " {active:true}) SET u.flagged = true") == null) { continue; }
            if (cmd(client, "MATCH (u:" + label + " {active:false}) SET u.active = true") == null) { continue; }

            // Full final-state check (labels + properties), not just the count, so subtler corruption
            // (label BugU->U, unapplied SET) is caught even when the node count stays 4.
            final JSONArray dump = cmd(client,
                "MATCH (n:" + label + ") RETURN labels(n) AS labels, n.id AS id, n.active AS active, n.flagged AS flagged ORDER BY id");
            if (dump == null)
              continue;
            boolean ok = dump.length() == 4;
            for (int i = 0; ok && i < dump.length(); i++) {
              final JSONObject n = dump.getJSONObject(i);
              final JSONArray labels = n.getJSONArray("labels");
              final long id = n.getLong("id");
              final boolean active = n.getBoolean("active");
              // Every node must keep its label, be active=true, and flagged=true only for originally-active id 1,3.
              if (labels.length() != 1 || !label.equals(labels.getString(0)) || !active
                  || (id == 1 || id == 3) != (n.has("flagged") && !n.isNull("flagged") && n.getBoolean("flagged")))
                ok = false;
            }
            final long postEdges = count(client, "MATCH (a:" + label + ")-[r:FRIEND]->(b:" + label + ") RETURN count(r) AS cnt");
            if (!ok || postEdges != 0)
              failures.add(label + " loop=" + loop + " edges=" + postEdges + " dump=" + dump);
          }
        } catch (final Exception e) {
          concErrors.add(label + ": " + e);
        }
      });
      threads.add(t);
      t.start();
    }
    start.countDown();
    for (final Thread t : threads)
      t.join();

    // Concurrent conflicts on the shared FRIEND edge buckets are expected and surface as retryable
    // "Concurrent modification on page ... please retry" HTTP errors (collected in `errors`); those
    // iterations are skipped. What must never happen is a committed-but-impossible final state.
    assertThat(concErrors).as("unexpected thread errors: %s", concErrors).isEmpty();
    assertThat(failures).as("impossible states under concurrency (%d): %s", failures.size(), failures).isEmpty();
  }
}
