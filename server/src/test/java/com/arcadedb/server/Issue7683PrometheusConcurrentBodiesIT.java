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
import com.arcadedb.server.http.handler.AbstractBinaryHttpHandler;
import com.arcadedb.server.http.handler.PostPrometheusReadHandler;
import com.arcadedb.server.http.handler.PostPrometheusWriteHandler;
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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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
 * Issue #7683: one request's body must not become another request's body on the Prometheus remote_write and
 * remote_read routes.
 * <p>
 * {@code AbstractBinaryHttpHandler} kept the request body in the INSTANCE field {@code protected byte[] rawBytes},
 * and both of its subclasses are singletons - one instance each, constructed once at route registration in
 * {@code HttpServer}, serving every request:
 *
 * <pre>
 * .post("/ts/{database}/prom/write", new PostPrometheusWriteHandler(this))
 * .post("/ts/{database}/prom/read", new PostPrometheusReadHandler(this))
 * </pre>
 *
 * {@code parseRequestPayload} and {@code execute} are two separate calls from
 * {@code AbstractServerHttpHandler.handleRequest}, with authentication and the idempotency reservation in
 * between, so two concurrent requests on two Undertow worker threads interleave as: T1 parses body1, T2
 * overwrites the field with body2, T1 executes and appends T2's samples, T2 executes and appends them again. T1
 * answers 204 having written the wrong body and lost its own, and the counts it reports are computed from that
 * same wrong body, so they agree with themselves. This is the same defect the issue reported on
 * {@code PostTimeSeriesWriteHandler} - the sibling the issue's closing line asked to be swept for - and
 * remote_write is if anything the more exposed of the two, since a Prometheus fleet fanning out to one remote
 * endpoint is concurrent by construction.
 * <p>
 * Each test releases eight threads together on a barrier, every one carrying a body only it sends, so the
 * assertion is an exact set rather than a count: against the shared field a run that happens not to interleave
 * reports green honestly instead of flakily red, and a run that does interleave names the bodies that were lost
 * and the ones that were written twice. Both tests were confirmed to fail against the unfixed handler before the
 * fix was applied - the write side with six of eight requests answered 500, the read side by answering
 * {@code ts7683_metric_1}'s query with {@code ts7683_metric_4}'s series.
 */
class Issue7683PrometheusConcurrentBodiesIT extends BaseGraphServerTest {

  private static final int WRITERS = 8;
  /** See {@link #concurrentRemoteReadsAnswerTheirOwnQuery()} for why the read side needs more than one round. */
  private static final int READ_ROUNDS = 10;

  // Metric name, value and timestamp are all distinct per writer, so a body cannot be mistaken for another one
  // by any of the three, and a failure message names which writer's body ended up where.

  private static String metric(final int w) {
    return "ts7683_metric_" + w;
  }

  private static double value(final int w) {
    return w + 0.5;
  }

  private static long timestamp(final int w) {
    return 100_000L + w;
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  remote_write
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void concurrentRemoteWritesDoNotOverwriteEachOthersBodies() throws Exception {
    final List<Integer> answers = inParallel(w -> postPromWrite(new WriteRequest(List.of(
        new TimeSeries(
            List.of(new Label("__name__", metric(w)), new Label("host", "server" + w)),
            List.of(new Sample(value(w), timestamp(w))))))));

    assertThat(answers).as("every concurrent remote_write must be accepted").containsOnly(204);

    final List<String> expected = new ArrayList<>(WRITERS);
    for (int w = 0; w < WRITERS; w++)
      expected.add(metric(w) + "=" + value(w));

    assertThat(storedSamples())
        .as("#7683: each concurrent remote_write body must be appended once and only once - a body shared through "
            + "an instance field loses some writers' samples and writes others' twice")
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  remote_read
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * The read side fails the same way and is worse to diagnose: nothing is corrupted on disk, one caller is simply
   * answered with another caller's series, and the answer is a well-formed remote_read response that a Grafana
   * panel will plot without complaint.
   * <p>
   * Seeded serially first, so every query below has exactly one right answer, and then read concurrently over
   * several rounds. The rounds are why this reproduces: a read spends far less time between
   * {@code parseRequestPayload} and {@code execute} than a write does - no schema work, no append - so its window
   * is narrower, and one round of eight is not reliably enough to land in it.
   */
  @Test
  void concurrentRemoteReadsAnswerTheirOwnQuery() throws Exception {
    for (int w = 0; w < WRITERS; w++)
      assertThat(postPromWrite(new WriteRequest(List.of(
          new TimeSeries(
              List.of(new Label("__name__", metric(w))),
              List.of(new Sample(value(w), timestamp(w)))))))).isEqualTo(204);

    final List<String> expected = new ArrayList<>(WRITERS);
    for (int w = 0; w < WRITERS; w++)
      expected.add(metric(w) + " -> " + value(w) + "@" + timestamp(w));

    for (int round = 0; round < READ_ROUNDS; round++) {
      final List<String> answered = inParallel(w -> {
        final ReadResponse response = postPromRead(new ReadRequest(List.of(
            new Query(0, 200_000, List.of(new LabelMatcher(MatchType.EQ, "__name__", metric(w)))))));

        final List<QueryResult> results = response.getResults();
        if (results.size() != 1 || results.getFirst().getTimeSeries().size() != 1)
          return metric(w) + " -> " + results.size() + " result(s), "
              + (results.isEmpty() ? "-" : results.getFirst().getTimeSeries().size()) + " series";

        final Sample sample = results.getFirst().getTimeSeries().getFirst().getSamples().getFirst();
        return metric(w) + " -> " + sample.value() + "@" + sample.timestampMs();
      });

      assertThat(answered)
          .as("#7683 (round %d): each concurrent remote_read must be answered from its OWN body - a body shared "
              + "through an instance field answers one caller with another caller's series", round)
          .containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  The structural claim the behaviour rests on
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Both behavioural tests above are consequences of this one fact, and a future binary handler that reintroduces
   * per-request state on the singleton has somewhere to fail that does not depend on winning a race.
   */
  @Test
  void theBinaryHandlerBaseClassKeepsNoPerRequestState() {
    final List<String> instanceFields = Arrays.stream(AbstractBinaryHttpHandler.class.getDeclaredFields())
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        // JaCoCo's $jacocoData is static and would already be gone; the filter is for any other instrumentation
        // that injects a field, so a coverage lane cannot turn this assertion red for a reason it is not about.
        .filter(f -> !f.isSynthetic())
        .map(Field::getName)
        .toList();

    assertThat(instanceFields)
        .as("#7683: a handler registered once on a route serves every request, so anything it keeps per request "
            + "belongs on the HttpServerExchange, not on the instance")
        .isEmpty();

    assertThat(AbstractBinaryHttpHandler.class)
        .as("both Prometheus routes inherit the binary body handling this class owns")
        .isAssignableFrom(PostPrometheusWriteHandler.class)
        .isAssignableFrom(PostPrometheusReadHandler.class);
  }

  // ---- Helpers ----

  private <T> List<T> inParallel(final PerWriter<T> body) throws Exception {
    final CyclicBarrier releaseTogether = new CyclicBarrier(WRITERS);
    final ExecutorService pool = Executors.newFixedThreadPool(WRITERS);
    try {
      final List<Callable<T>> calls = new ArrayList<>(WRITERS);
      for (int i = 0; i < WRITERS; i++) {
        final int w = i;
        calls.add(() -> {
          releaseTogether.await(30, TimeUnit.SECONDS);
          return body.apply(w);
        });
      }

      final List<T> answers = new ArrayList<>(WRITERS);
      for (final Future<T> answered : pool.invokeAll(calls, 120, TimeUnit.SECONDS))
        answers.add(answered.get());
      return answers;
    } finally {
      pool.shutdownNow();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
  }

  @FunctionalInterface
  private interface PerWriter<T> {
    T apply(int writer) throws Exception;
  }

  /**
   * Every sample the eight writers put in the database, as {@code metric=value}. Read back from the embedded
   * database rather than over remote_read, so this assertion cannot be satisfied - or broken - by the very read
   * path the other test exercises. A metric whose type was never created contributes nothing, which is exactly
   * how a lost body shows up here, and a body written twice contributes its row twice.
   * <p>
   * {@code value} alone identifies the writer ({@link #value(int)} is distinct per writer), so the timestamp is
   * not part of the fingerprint: it is stored in a DATETIME column and comes back as a {@code LocalDateTime},
   * whose conversion back to epoch milliseconds would make this assertion depend on the JVM's default zone
   * without distinguishing anything the value does not already distinguish. The remote_read test below does
   * assert the timestamp, end to end, in the units the client sent.
   */
  private List<String> storedSamples() {
    final Database database = getServer(0).getDatabase(getDatabaseName());
    final List<String> found = new ArrayList<>();
    for (int w = 0; w < WRITERS; w++) {
      final String type = metric(w);
      if (!database.getSchema().existsType(type))
        continue;
      final Iterator<Result> records = database.query("sql", "SELECT FROM " + type);
      while (records.hasNext()) {
        final Result record = records.next();
        found.add(type + "=" + record.getProperty("value"));
      }
    }
    return found;
  }

  private int postPromWrite(final WriteRequest writeRequest) throws Exception {
    final HttpURLConnection connection = createPromConnection("prom/write");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(writeRequest.encode()));
      os.flush();
    }
    return connection.getResponseCode();
  }

  private ReadResponse postPromRead(final ReadRequest readRequest) throws Exception {
    final HttpURLConnection connection = createPromConnection("prom/read");
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

  /** The port the server actually bound: a stranger already on 2480 pushes this one up the configured range. */
  private HttpURLConnection createPromConnection(final String path) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/ts/" + getDatabaseName() + "/" + path)
        .toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    return connection;
  }
}
