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

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.QueryResult;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7704: the idempotency key must bind to the request body on the BINARY routes too.
 * <p>
 * {@code AbstractServerHttpHandler.handleRequest} folds the body into the key precisely so a client reusing one
 * {@code X-Request-Id} across two distinct writes cannot be answered with the first write's cached response. What
 * it folded in was {@code payloadAsString}, and {@code AbstractBinaryHttpHandler.parseRequestPayload} returns
 * {@code null} for that BY CONSTRUCTION - the body is bytes, and {@code execute} reads them back off the exchange.
 * So on {@code POST /api/v1/ts/{database}/prom/write} and {@code .../prom/read} the key was the request id, the
 * method, the path and the database, and nothing else: two remote-write requests carrying DIFFERENT samples under
 * one correlation id hashed to the same key, the second was a cache hit replayed the first's {@code 204}, and its
 * samples were never appended. The very protection the body was added to the key to provide was absent on exactly
 * the routes whose body is not text.
 * <p>
 * Latent rather than live against Prometheus itself, which sends no {@code X-Request-Id}; reachable for any
 * collector or proxy that stamps a correlation id per connection, per scrape loop or per retry batch - which is
 * the usual reason to stamp one at all.
 * <p>
 * The text routes were never affected ({@code PostTimeSeriesWriteHandler.parseRequestPayload} returns the body),
 * and the last test here says so, so a future change that fixes the binary side by weakening the text side has
 * somewhere to fail.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7704BinaryIdempotencyKeyBodyIT extends BaseGraphServerTest {

  private static final String SHARED_REQUEST_ID = "collector-connection-1";

  /**
   * The defect itself: two remote-write bodies, one correlation id, and both sets of samples must land.
   */
  @Test
  void twoRemoteWritesSharingOneRequestIdBothLand() throws Exception {
    assertThat(postPromWrite(writeOf("ts7704_first", 1.5, 100_001L), SHARED_REQUEST_ID)).isEqualTo(204);
    assertThat(postPromWrite(writeOf("ts7704_second", 2.5, 100_002L), SHARED_REQUEST_ID))
        .as("a second, DIFFERENT body under the same correlation id is not a retry of the first")
        .isEqualTo(204);

    assertThat(storedSamples("ts7704_first", "ts7704_second"))
        .as("#7704: with the body out of the key the second write is a cache hit replayed the first's 204, and "
            + "its samples are never appended")
        .containsExactlyInAnyOrder("ts7704_first=1.5", "ts7704_second=2.5");
  }

  /**
   * The other half of the contract, which the fix must not break: the SAME body under the same correlation id is
   * a genuine retry and must still be answered from the cache rather than appended twice. Without this the fix
   * could be "never cache a binary route", which would take the replay protection away instead of correcting it.
   */
  @Test
  void anIdenticalRemoteWriteRetryIsStillDeduplicated() throws Exception {
    final WriteRequest body = writeOf("ts7704_retried", 7.5, 100_003L);

    assertThat(postPromWrite(body, "retry-batch-9")).isEqualTo(204);
    assertThat(postPromWrite(body, "retry-batch-9")).isEqualTo(204);

    assertThat(storedSamples("ts7704_retried"))
        .as("a byte-identical retry under the same id is the case the idempotency cache exists for")
        .containsExactly("ts7704_retried=7.5");
  }

  /**
   * {@code remote_read} shares the base class, and two concurrent reads under one correlation id must each come
   * back with THEIR OWN series.
   * <p>
   * <b>What this test can and cannot detect, stated plainly so the next reader does not assume more of it.</b>
   * {@code PostPrometheusReadHandler} writes its response itself and returns {@code null}, which ABORTS the
   * reservation, so the route never populates the cache and nothing can be replayed from it - the same shape a
   * streaming route has (#7311). What the body-agnostic key COULD still do is make two concurrent reads collide
   * on a single key, the second finding the first in flight and waiting for a result that was never going to be
   * its own. That collision is not reachable from a test: a remote read of a handful of samples finishes and
   * aborts its reservation in microseconds, long before a second HTTP request gets to the handler. A timing
   * assertion on the pair was written and MEASURED against the unfixed key here - it passed, i.e. it separated
   * nothing - so it was removed rather than shipped as a bound that never fires. Manufacturing the window (a
   * six-figure-sample first read) would be engineering a delay to make a bound separate, for a latent collision
   * on a route that does not cache.
   * <p>
   * The deterministic proof that these two requests are two keys is
   * {@code IdempotencyKeyTest.sameRequestIdDifferentBinaryBodyProducesDifferentKey}, which needs no server and no
   * clock. This test is the end-to-end guard beside it: it runs the reads concurrently, so a future change making
   * this route return an {@code ExecutionResponse} - which WOULD cache, and would replay one caller's series to
   * another - lands on an assertion rather than on a Grafana panel.
   */
  @Test
  void twoConcurrentRemoteReadsSharingOneRequestIdEachAnswerTheirOwnQuery() throws Exception {
    assertThat(postPromWrite(writeOf("ts7704_read_a", 3.5, 100_004L), null)).isEqualTo(204);
    assertThat(postPromWrite(writeOf("ts7704_read_b", 4.5, 100_005L), null)).isEqualTo(204);

    final String[] metrics = { "ts7704_read_a", "ts7704_read_b" };
    final CyclicBarrier releaseTogether = new CyclicBarrier(metrics.length);
    final ExecutorService pool = Executors.newFixedThreadPool(metrics.length);
    final List<String> answers;
    try {
      final List<Callable<String>> calls = new ArrayList<>(metrics.length);
      for (final String metric : metrics)
        calls.add(() -> {
          releaseTogether.await(30, TimeUnit.SECONDS);
          return readBack(metric, SHARED_REQUEST_ID);
        });

      answers = new ArrayList<>(metrics.length);
      for (final Future<String> answered : pool.invokeAll(calls, 120, TimeUnit.SECONDS))
        answers.add(answered.get());
    } finally {
      pool.shutdownNow();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(answers)
        .as("#7704: each concurrent read must be answered from its OWN body")
        .containsExactlyInAnyOrder("ts7704_read_a -> 3.5@100004", "ts7704_read_b -> 4.5@100005");
  }

  // ---- Helpers ----

  private static WriteRequest writeOf(final String metric, final double value, final long timestampMs) {
    return new WriteRequest(List.of(new TimeSeries(
        List.of(new Label("__name__", metric)),
        List.of(new Sample(value, timestampMs)))));
  }

  /** Every sample of the named metrics in the database, as {@code metric=value}, read back embedded. */
  private List<String> storedSamples(final String... metrics) {
    final Database database = getServer(0).getDatabase(getDatabaseName());
    final List<String> found = new ArrayList<>();
    for (final String type : metrics) {
      if (!database.getSchema().existsType(type))
        continue;
      final Iterator<Result> records = database.query("sql", "SELECT FROM " + type);
      while (records.hasNext())
        found.add(type + "=" + records.next().getProperty("value"));
    }
    return found;
  }

  private String readBack(final String metric, final String requestId) throws Exception {
    final ReadResponse response = postPromRead(new ReadRequest(List.of(
        new Query(0, 200_000, List.of(new LabelMatcher(MatchType.EQ, "__name__", metric))))), requestId);

    final List<QueryResult> results = response.getResults();
    if (results.size() != 1 || results.getFirst().getTimeSeries().size() != 1)
      return metric + " -> " + results.size() + " result(s), "
          + (results.isEmpty() ? "-" : results.getFirst().getTimeSeries().size()) + " series";

    final Sample sample = results.getFirst().getTimeSeries().getFirst().getSamples().getFirst();
    return metric + " -> " + sample.value() + "@" + sample.timestampMs();
  }

  private int postPromWrite(final WriteRequest writeRequest, final String requestId) throws Exception {
    final HttpURLConnection connection = createPromConnection("prom/write", requestId);
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(writeRequest.encode()));
      os.flush();
    }
    return connection.getResponseCode();
  }

  private ReadResponse postPromRead(final ReadRequest readRequest, final String requestId) throws Exception {
    final HttpURLConnection connection = createPromConnection("prom/read", requestId);
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(readRequest.encode()));
      os.flush();
    }

    assertThat(connection.getResponseCode()).isEqualTo(200);

    try (final InputStream is = connection.getInputStream()) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final byte[] buf = new byte[4096];
      int n;
      while ((n = is.read(buf)) != -1)
        baos.write(buf, 0, n);
      return ReadResponse.decode(Snappy.uncompress(baos.toByteArray()));
    }
  }

  private HttpURLConnection createPromConnection(final String path, final String requestId) throws Exception {
    final HttpURLConnection connection = createConnection("ts/" + getDatabaseName() + "/" + path, requestId);
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    return connection;
  }

  /** The port the server actually bound: a stranger already on 2480 pushes this one up the configured range. */
  private HttpURLConnection createConnection(final String path, final String requestId) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/" + path)
        .toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    if (requestId != null)
      connection.setRequestProperty("X-Request-Id", requestId);
    return connection;
  }
}
