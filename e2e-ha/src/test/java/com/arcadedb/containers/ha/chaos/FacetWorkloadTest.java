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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.FacetSchema;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class FacetWorkloadTest {
  private static final String NON_EXACT = "{\"result\":[],\"facets\":{\"meta\":{\"exact\":false,\"patched\":3,\"lagMs\":5}}}";
  private static final String EXACT     = "{\"result\":[],\"facets\":{\"meta\":{\"exact\":true,\"patched\":0,\"lagMs\":0}}}";
  /** Inexact, yet no lag reported: the metadata contradicts itself. */
  private static final String NO_LAG    = "{\"result\":[],\"facets\":{\"meta\":{\"exact\":false,\"patched\":3,\"lagMs\":0}}}";

  private final AtomicInteger           status   = new AtomicInteger(200);
  private final AtomicReference<String> answer   = new AtomicReference<>(NON_EXACT);
  private final AtomicInteger requests = new AtomicInteger();
  private final List<String>  bodies   = new CopyOnWriteArrayList<>();
  private       HttpServer    server;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(Executors.newCachedThreadPool());
    server.createContext("/", exchange -> {
      bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      requests.incrementAndGet();
      final byte[] body = answer.get().getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(status.get(), body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private static Endpoints single(final int port) {
    final Endpoint endpoint = new Endpoint("127.0.0.1", port);
    return new Endpoints() {
      @Override
      public int size() {
        return 1;
      }

      @Override
      public Endpoint endpoint(final int node) {
        return endpoint;
      }
    };
  }

  private FacetWorkload workload(final int writers) {
    return new FacetWorkload(7L, writers, single(server.getAddress().getPort()), ChaosSchema.DATABASE, 1_000, 2_000, 1);
  }

  private List<JSONObject> firstRequests(final int count) {
    final List<JSONObject> list = new ArrayList<>();
    for (final String body : new ArrayList<>(bodies)) {
      if (list.size() == count)
        break;
      list.add(new JSONObject(body));
    }
    return list;
  }

  private List<String> run(final int writers, final int count) {
    try (final FacetWorkload workload = workload(writers)) {
      workload.start();
      await().atMost(Duration.ofSeconds(30)).until(() -> requests.get() >= count + 20);
    }
    final List<String> commands = new ArrayList<>();
    for (final JSONObject request : firstRequests(count))
      commands.add(request.getString("command"));
    return commands;
  }

  @Test
  void sameSeedSendsTheSameStatementSequence() {
    final List<String> first = run(1, 200);
    bodies.clear();
    requests.set(0);
    final List<String> second = run(1, 200);
    assertThat(first).hasSize(200);
    assertThat(second).isEqualTo(first);
  }

  @Test
  void mixCoversEveryStatement() {
    final List<String> commands = run(2, 2000);
    for (final String statement : List.of(FacetSchema.INSERT, FacetSchema.INSERT_NO_BRAND, FacetSchema.UPDATE_VALUES,
        FacetSchema.REMOVE_BRAND, FacetSchema.GROW, FacetSchema.UPDATE_GROWN, FacetSchema.DELETE, FacetSchema.LINK,
        FacetSchema.facetQuery(""), FacetSchema.facetQuery("fileSize >= 1024")))
      assertThat(commands).as(statement).contains(statement);
    final long deletes = commands.stream().filter(FacetSchema.DELETE::equals).count();
    assertThat(deletes).isBetween(160L, 320L);
    // 5% of the operations are facet queries: about 100 of 2000, binomial standard deviation about 10
    final long queries = commands.stream()
        .filter(command -> command.equals(FacetSchema.facetQuery("")) || command.equals(FacetSchema.facetQuery("fileSize >= 1024")))
        .count();
    assertThat(queries).isBetween(60L, 140L);
  }

  @Test
  void writesTargetOnlyOwnedKeys() {
    run(2, 600);
    final Map<Long, Integer> writers = new HashMap<>();
    for (final JSONObject request : firstRequests(600)) {
      final JSONObject params = request.getJSONObject("params", new JSONObject());
      for (final String name : List.of("aid", "from", "to"))
        if (params.has(name))
          writers.merge(params.getLong(name) >>> 32, 1, Integer::sum);
    }
    assertThat(writers.keySet()).isSubsetOf(0L, 1L);
    assertThat(writers).hasSize(2);
  }

  @Test
  void quiesceBlocksEveryWriter() throws InterruptedException {
    try (final FacetWorkload workload = workload(2)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> requests.get() >= 20);
      workload.quiesce();
      final int frozen = requests.get();
      Thread.sleep(300);
      assertThat(requests.get()).isEqualTo(frozen);
      workload.resume();
      await().atMost(Duration.ofSeconds(10)).until(() -> requests.get() > frozen);
    }
  }

  @Test
  void serverErrorsOnQueriesAreCountedNotThrown() {
    status.set(500);
    try (final FacetWorkload workload = workload(2)) {
      workload.start();
      await().atMost(Duration.ofSeconds(30)).until(() -> workload.stats().queryServerErrors() > 0);
      final long before = requests.get();
      await().atMost(Duration.ofSeconds(10)).until(() -> requests.get() > before + 5);
      assertThat(workload.stats().writesFailed()).isGreaterThan(0L);
      assertThat(workload.stats().writesOk()).isZero();
    }
  }

  @Test
  void nonExactAnswersAreCounted() {
    try (final FacetWorkload workload = workload(2)) {
      workload.start();
      await().atMost(Duration.ofSeconds(30)).until(() -> workload.stats().queries() >= 3);
      assertThat(workload.stats().nonExact()).isGreaterThan(0L);
      assertThat(workload.stats().queryFailures()).isZero();
      // an inexact answer that reports its lag is consistent
      assertThat(workload.stats().metaInconsistent()).isZero();
      assertThat(workload.acked()).isEqualTo(workload.stats().writesOk());
    }
  }

  /** The forced-inexact run (patch budget 0) must see, under faults, an inexact answer that reports no lag. */
  @Test
  void inexactAnswersWithoutLagAreCountedAsMetaInconsistent() {
    answer.set(NO_LAG);
    try (final FacetWorkload workload = workload(2)) {
      workload.start();
      await().atMost(Duration.ofSeconds(30)).until(() -> workload.stats().queries() >= 3);
      workload.close();
      final FacetWorkload.Stats stats = workload.stats();
      assertThat(stats.metaInconsistent()).isEqualTo(stats.queries());
      assertThat(stats.nonExact()).isEqualTo(stats.queries());
      assertThat(stats.queryFailures()).isZero();
      assertThat(stats.warnings()).singleElement().asString().startsWith("FACET_META workload")
          .contains(stats.queries() + " samples");
    }
  }

  @Test
  void exactAnswersAreNotCountedAsNonExact() {
    answer.set(EXACT);
    try (final FacetWorkload workload = workload(2)) {
      workload.start();
      await().atMost(Duration.ofSeconds(30)).until(() -> workload.stats().queries() >= 5);
      workload.close();
      assertThat(workload.stats().nonExact()).isZero();
      assertThat(workload.stats().queryFailures()).isZero();
      assertThat(workload.stats().queryServerErrors()).isZero();
    }
  }

  /** A query the server rejects (a broken FACETS statement answers 400) must show in the run summary, never vanish. */
  @Test
  void clientErrorsOnQueriesAreCounted() {
    status.set(400);
    try (final FacetWorkload workload = workload(2)) {
      workload.start();
      await().atMost(Duration.ofSeconds(30)).until(() -> workload.stats().queries() >= 3);
      workload.close();
      final FacetWorkload.Stats stats = workload.stats();
      assertThat(stats.queryFailures()).isEqualTo(stats.queries());
      assertThat(stats.queryServerErrors()).isZero();
      assertThat(stats.nonExact()).isZero();
      assertThat(stats.warnings()).singleElement().asString().startsWith("FACET_QUERY_FAILURES " + stats.queryFailures());
    }
  }

  @Test
  void unparsableQueryAnswersAreCounted() {
    answer.set("not json");
    try (final FacetWorkload workload = workload(2)) {
      workload.start();
      await().atMost(Duration.ofSeconds(30)).until(() -> workload.stats().queries() >= 3);
      workload.close();
      final FacetWorkload.Stats stats = workload.stats();
      assertThat(stats.queryFailures()).isEqualTo(stats.queries());
      assertThat(stats.nonExact()).isZero();
      // writes only look at the status, so they still succeed
      assertThat(stats.writesOk()).isGreaterThan(0L);
    }
  }

  @Test
  void statsWarnOnlyOnFailuresAndInconsistentMetadata() {
    assertThat(new FacetWorkload.Stats(10, 3, 5, 2, 0, 4, 0).warnings()).as("faults explain 5xx and failed writes").isEmpty();
    assertThat(new FacetWorkload.Stats(10, 0, 5, 0, 1, 0, 0).warnings()).containsExactly(
        "FACET_QUERY_FAILURES 1 facet queries answered with neither a 5xx nor a parsable result (first cause in the log)");
    assertThat(new FacetWorkload.Stats(10, 0, 5, 0, 0, 2, 2).warnings()).containsExactly(
        "FACET_META workload 2 samples inexact without lag (first sample in the log)");
    assertThat(new FacetWorkload.Stats(10, 0, 5, 0, 1, 2, 2).warnings()).hasSize(2);
  }

  @Test
  void statsAreReportedAsJson() {
    final JSONObject json = new FacetWorkload.Stats(1, 2, 3, 4, 5, 6, 7).toJSON();
    assertThat(json.getLong("writesOk")).isEqualTo(1L);
    assertThat(json.getLong("writesFailed")).isEqualTo(2L);
    assertThat(json.getLong("queries")).isEqualTo(3L);
    assertThat(json.getLong("queryServerErrors")).isEqualTo(4L);
    assertThat(json.getLong("queryFailures")).isEqualTo(5L);
    assertThat(json.getLong("nonExact")).isEqualTo(6L);
    assertThat(json.getLong("metaInconsistent")).isEqualTo(7L);
  }
}
