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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
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
import com.arcadedb.server.http.handler.prometheus.ProtobufDecoder;
import com.arcadedb.server.http.handler.prometheus.ProtobufEncoder;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Prometheus remote write (and read) edge cases over HTTP: an engine that is not available, concurrent request
 * bodies, undeclared and empty labels, the accepted content types, typed tags, and staleness markers.
 */
class PrometheusRemoteWriteEdgeCasesIT {

  /**
   * Issue #6839 (item 2): Prometheus remote-write answered the "registered but engine-unavailable" TimeSeries state
   * of #6356 with a phantom name collision.
   * <p>
   * PR #6779 converted every TimeSeries read/write call site to the {@code isEngineAvailable()} /
   * {@code requireEngine()} pair, and missed {@code PostPrometheusWriteHandler.getOrCreateType}, whose pre-existing
   * shape turns that state into the wrong error. It asked one question for two things -
   * {@code docType instanceof LocalTimeSeriesType tsType && tsType.getEngine() != null} - so a type that exists and
   * IS a TimeSeries type, merely without an engine, failed the guard and fell through to the auto-create branch,
   * which threw {@code SchemaException("Type 'X' already exists")}.
   * <p>
   * That is not a cosmetic difference. "Already exists" tells an operator to go looking for a name collision that
   * does not exist, while the message {@code requireEngine()} produces names the type AND the file whose failure to
   * open put it in this state - which is the whole reason #6356 kept the type registered instead of letting it
   * vanish. No data was damaged either way: {@code create()} throws before it mutates the schema.
   * <p>
   * The state is reached here by closing the live type's engine, which nulls the same field a failed
   * {@code initEngine()} leaves null - the field this guard reads - without needing to corrupt a file and restart
   * the server around it. The engine-side arms of that state (a corrupt {@code .ts.sealed} at schema load, and the
   * recovery from it) are pinned by {@code TimeSeriesCheckDatabaseTest} and
   * {@code SealedBlobRepairAndRefusalTest.EngineRepair}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EngineUnavailable extends BaseGraphServerTest {

    private static final String METRIC = "cpu_unavailable";

    /**
     * Off the default 2480-2489 range on purpose. {@code BaseGraphServerTest}'s own HTTP helpers address
     * {@code 248<serverIndex>} literally, so a developer machine with anything already on 2480 - a locally installed
     * ArcadeDB is the common one - silently routes those requests at the other server and the failures read as
     * authentication errors. This class builds its own URL from the port the server actually bound
     * ({@code getHttpServer().getPort()}), so it neither collides with such a server nor cares which port of the
     * range it ended up on.
     */
    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, "2490-2499");
    }

    @Test
    void aRemoteWriteToATypeWithNoEngineReportsTheRealReasonNotAPhantomNameCollision() throws Exception {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE TIMESERIES TYPE " + METRIC
          + " TIMESTAMP timestamp TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      // The healthy path first, so the request shape below is known-good and a later failure cannot be the payload.
      assertThat(postPromWrite(sampleFor(75.5, 1000)).statusCode()).isEqualTo(204);

      // Null the engine, exactly as a failed initEngine() leaves it (issue #6356).
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(METRIC);
      tsType.close();
      assertThat(tsType.isEngineAvailable()).as("the state this test is about").isFalse();

      final Response response = postPromWrite(sampleFor(80.2, 2000));

      assertThat(response.statusCode()).as("a write against a type with no storage must fail").isNotEqualTo(204);
      assertThat(response.body())
          .as("the error must name the real reason, not a name collision that does not exist")
          .contains(METRIC)
          .contains("no storage engine available")
          .doesNotContain("already exists");
    }

    // ---- Helpers ----

    private record Response(int statusCode, String body) {
    }

    private static WriteRequest sampleFor(final double value, final long timestampMs) {
      return new WriteRequest(List.of(new TimeSeries(
          List.of(new Label("__name__", METRIC), new Label("host", "server1")),
          List.of(new Sample(value, timestampMs)))));
    }

    private Response postPromWrite(final WriteRequest writeRequest) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/ts/" + getDatabaseName()
          + "/prom/write").toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty("Content-Type", "application/x-protobuf");
      connection.setRequestProperty("Content-Encoding", "snappy");
      connection.setDoOutput(true);

      try (final OutputStream os = connection.getOutputStream()) {
        os.write(Snappy.compress(writeRequest.encode()));
        os.flush();
      }

      final int statusCode = connection.getResponseCode();
      return new Response(statusCode, readBody(connection));
    }

    private static String readBody(final HttpURLConnection connection) throws Exception {
      try (final InputStream is = connection.getResponseCode() < 400 ?
          connection.getInputStream() :
          connection.getErrorStream()) {
        if (is == null)
          return "";
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final byte[] buffer = new byte[4096];
        int read;
        while ((read = is.read(buffer)) != -1)
          baos.write(buffer, 0, read);
        return baos.toString(StandardCharsets.UTF_8);
      }
    }
  }

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
  @Nested
  class ConcurrentBodies extends BaseGraphServerTest {

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
      final Database database = getServer(0).getDatabaseForOperator(getDatabaseName());
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

  /**
   * Issues #9365 and #9363 on Prometheus remote write:
   * <ul>
   * <li>#9365: a series carrying a label the existing type has no TAG column for is dropped and reported with a 400
   * naming the label (the valid series of the same request are stored), unless
   * {@code arcadedb.timeSeriesUndeclaredKeys=ignore}, which discards the label.</li>
   * <li>#9363: a label with an empty value is an absent label, so it is stored as null, the same series as one that omits
   * the label, and is never "undeclared".</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UndeclaredAndEmptyLabel extends BaseGraphServerTest {

    @Test
    void anUndeclaredLabelDropsOnlyThatSeries() throws Exception {
      final Database db = getServerDatabase(0, getDatabaseName());
      db.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "reject");

      assertThat(post(new WriteRequest(List.of(series("m9365a", 1.0, label("host", "h1")))))).isEqualTo(204);

      final int status = post(new WriteRequest(List.of(
          series("m9365a", 2.0, label("host", "h2")),
          series("m9365a", 3.0, label("host", "h3"), label("region", "eu")))));
      assertThat(status).as("the series with the undeclared label is reported").isEqualTo(400);

      assertThat(count(db, "m9365a")).as("the valid series is stored, the other is not").isEqualTo(2);
    }

    @Test
    void ignoreDiscardsTheUndeclaredLabel() throws Exception {
      final Database db = getServerDatabase(0, getDatabaseName());
      db.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "ignore");
      try {
        assertThat(post(new WriteRequest(List.of(series("m9365b", 1.0, label("host", "h1")))))).isEqualTo(204);
        assertThat(post(new WriteRequest(List.of(series("m9365b", 2.0, label("host", "h1"), label("region", "eu"))))))
            .isEqualTo(204);
        assertThat(count(db, "m9365b")).isEqualTo(2);
      } finally {
        db.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "reject");
      }
    }

    @Test
    void aLabelIsMatchedToItsTagAfterNameSanitization() throws Exception {
      final Database db = getServerDatabase(0, getDatabaseName());
      db.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "reject");

      assertThat(post(new WriteRequest(List.of(series("m9365c", 1.0, label("a_b", "x")))))).isEqualTo(204);
      // `a.b` is stored as the `a_b` tag, so it is declared
      assertThat(post(new WriteRequest(List.of(series("m9365c", 2.0, label("a.b", "y")))))).isEqualTo(204);
      assertThat(count(db, "m9365c")).isEqualTo(2);
    }

    @Test
    void anEmptyLabelValueIsStoredAsAbsent() throws Exception {
      final Database db = getServerDatabase(0, getDatabaseName());
      db.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "reject");

      assertThat(post(new WriteRequest(List.of(series("m9363", 1.0, label("host", ""), label("zone", "z1")))))).isEqualTo(204);
      // omits `host` altogether, and a later series that names it empty again: neither is undeclared
      assertThat(post(new WriteRequest(List.of(series("m9363", 2.0, label("zone", "z1")))))).isEqualTo(204);

      try (final ResultSet rs = db.query("sql", "SELECT host FROM m9363")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          // the engine reads an absent tag back as "", so null and "" are one series; what matters is the write succeeded
          assertThat(r.<String>getProperty("host")).as("empty label stored as absent").isNullOrEmpty();
        }
      }
      assertThat(count(db, "m9363")).isEqualTo(2);
    }

    private static Label label(final String name, final String value) {
      return new Label(name, value);
    }

    private static TimeSeries series(final String metric, final double value, final Label... labels) {
      final List<Label> all = new ArrayList<>();
      all.add(new Label("__name__", metric));
      all.addAll(List.of(labels));
      return new TimeSeries(all, List.of(new Sample(value, 1000)));
    }

    private static long count(final Database db, final String type) {
      try (final ResultSet rs = db.query("sql", "SELECT count(*) AS n FROM " + type)) {
        return rs.next().<Long>getProperty("n");
      }
    }

    private int post(final WriteRequest request) throws Exception {
      final byte[] compressed = Snappy.compress(request.encode());
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          "http://127.0.0.1:" + getServerHttpPort(0) + "/api/v1/ts/" + getDatabaseName() + "/prom/write").toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty("Content-Type", "application/x-protobuf");
      connection.setRequestProperty("Content-Encoding", "snappy");
      connection.setDoOutput(true);
      try (final OutputStream os = connection.getOutputStream()) {
        os.write(compressed);
        os.flush();
      }
      return connection.getResponseCode();
    }
  }

  /**
   * Issue #9624: Prometheus remote write/read never looked at the request {@code Content-Type}, so a remote-write 2.0
   * payload ({@code io.prometheus.write.v2.Request}) decoded to zero 1.0 series and was answered 204, which the sender
   * counts as delivered; and a malformed protobuf body escaped the decoder as an {@link IllegalStateException} answered
   * 500 instead of 400.
   */
  @Nested
  class ContentType extends BaseGraphServerTest {

    private static final String V1_WRITE = "application/x-protobuf;proto=prometheus.WriteRequest";
    private static final String V2_WRITE = "application/x-protobuf;proto=io.prometheus.write.v2.Request";

    /** The six hostile bodies from the issue, every one a client mistake rather than a server fault. */
    private static final String[] MALFORMED = { "0d00000000", "0affffffff0f", "0a05aabb", "1b", "ffffffffffffffffffff7f", "0902" };

    @Test
    void aRemoteWrite2ContentTypeIsAnswered415AndNothingIsWritten() throws Exception {
      final Response response = post("prom/write", Snappy.compress(v2Request("m9624v2")), V2_WRITE);
      assertThat(response.status).as("a 2.0 sender falls back to 1.0 only on a 415").isEqualTo(415);
      assertThat(response.body).contains("prometheus.WriteRequest");
      assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsType("m9624v2")).isFalse();
    }

    @Test
    void aQuotedOrCaseVariantProtoParameterIsStillRead() throws Exception {
      assertThat(post("prom/write", Snappy.compress(v2Request("m9624q")),
          "application/x-protobuf; Proto=\"io.prometheus.write.v2.Request\"").status).isEqualTo(415);
    }

    @Test
    void anEmptyProtoParameterIsAbsentSoA2BodyStillGets400() throws Exception {
      assertThat(post("prom/write", Snappy.compress(v2Request("m9624empty")), "application/x-protobuf;proto=").status).isEqualTo(400);
    }

    @Test
    void aRemoteWrite2BodyWithoutTheProtoParameterIsAnswered400Not204() throws Exception {
      final Response response = post("prom/write", Snappy.compress(v2Request("m9624nohdr")), "application/x-protobuf");
      assertThat(response.status).as("a non-empty body with no 1.0 content must not be acknowledged as delivered").isEqualTo(400);
    }

    @Test
    void anExplicitRemoteWrite1ContentTypeIsAccepted() throws Exception {
      final WriteRequest request = new WriteRequest(List.of(new TimeSeries(
          List.of(new Label("__name__", "m9624v1"), new Label("host", "h1")),
          List.of(new Sample(1.0, 1696000000000L), new Sample(2.0, 1696000015000L)))));
      assertThat(post("prom/write", Snappy.compress(request.encode()), V1_WRITE).status).isEqualTo(204);

      final Database db = getServerDatabase(0, getDatabaseName());
      try (final ResultSet rs = db.query("sql", "SELECT count(*) AS n FROM m9624v1")) {
        assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
      }
    }

    @Test
    void aMetadataOnlyRemoteWrite1RequestIsStillANoOp204() throws Exception {
      // Prometheus sends metric metadata (WriteRequest field 3) in requests of its own, with no series: valid 1.0
      final ProtobufEncoder metadata = new ProtobufEncoder();
      metadata.writeTag(1, ProtobufDecoder.WIRETYPE_VARINT);
      metadata.writeVarint(1); // COUNTER
      metadata.writeString(2, "http_requests_total");
      metadata.writeString(4, "Total HTTP requests");
      final ProtobufEncoder request = new ProtobufEncoder();
      request.writeSubMessage(3, metadata.toByteArray());

      assertThat(post("prom/write", Snappy.compress(request.toByteArray()), V1_WRITE).status).isEqualTo(204);
      assertThat(post("prom/write", Snappy.compress(request.toByteArray()), "application/x-protobuf").status).isEqualTo(204);
    }

    @Test
    void anEmptyRemoteWrite1RequestIsStillANoOp204() throws Exception {
      assertThat(post("prom/write", Snappy.compress(new byte[0]), V1_WRITE).status).isEqualTo(204);
    }

    @Test
    void aMalformedWriteBodyIsAnswered400Not500() throws Exception {
      for (final String hex : MALFORMED) {
        final Response response = post("prom/write", Snappy.compress(HexFormat.of().parseHex(hex)), V1_WRITE);
        assertThat(response.status).as("body %s", hex).isEqualTo(400);
        assertThat(response.body).as("body %s", hex).contains("Malformed protobuf");
      }
    }

    @Test
    void aMalformedReadBodyIsAnswered400Not500() throws Exception {
      for (final String hex : MALFORMED) {
        final Response response = post("prom/read", Snappy.compress(HexFormat.of().parseHex(hex)), "application/x-protobuf");
        assertThat(response.status).as("body %s", hex).isEqualTo(400);
        assertThat(response.body).as("body %s", hex).contains("Malformed protobuf");
      }
    }

    @Test
    void aReadRequestNamingAnotherProtoMessageIsAnswered415() throws Exception {
      final ReadRequest request = new ReadRequest(List.of(new Query(0, 1000,
          List.of(new LabelMatcher(MatchType.EQ, "__name__", "m9624read")))));
      assertThat(post("prom/read", Snappy.compress(request.encode()), "application/x-protobuf;proto=io.prometheus.read.v2.Request").status)
          .isEqualTo(415);
      assertThat(post("prom/read", Snappy.compress(request.encode()), "application/x-protobuf;proto=prometheus.ReadRequest").status)
          .isEqualTo(200);
    }

    /**
     * An {@code io.prometheus.write.v2.Request}: field 4 = repeated string symbols, field 5 = repeated TimeSeries
     * (field 1 = packed label refs into the symbols, field 2 = repeated Sample {1 = double value, 2 = int64 timestamp}).
     * Fields 1 to 3 are reserved precisely so that a 1.0 parser finds nothing in it.
     */
    private static byte[] v2Request(final String metric) {
      final ProtobufEncoder labelRefs = new ProtobufEncoder();
      for (final int ref : new int[] { 1, 2, 3, 4 })
        labelRefs.writeVarint(ref);

      final ProtobufEncoder series = new ProtobufEncoder();
      series.writeTag(1, ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED);
      series.writeLengthDelimited(labelRefs.toByteArray());
      for (final long ts : new long[] { 1696000000000L, 1696000015000L }) {
        final ProtobufEncoder sample = new ProtobufEncoder();
        sample.writeTag(1, ProtobufDecoder.WIRETYPE_FIXED64);
        sample.writeFixed64(42.0);
        sample.writeTag(2, ProtobufDecoder.WIRETYPE_VARINT);
        sample.writeVarint(ts);
        series.writeSubMessage(2, sample.toByteArray());
      }

      final ProtobufEncoder request = new ProtobufEncoder();
      for (final String symbol : new String[] { "", "__name__", metric, "host", "h1" })
        request.writeString(4, symbol);
      request.writeSubMessage(5, series.toByteArray());
      return request.toByteArray();
    }

    private record Response(int status, String body) {
    }

    private Response post(final String path, final byte[] body, final String contentType) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          "http://127.0.0.1:" + getServerHttpPort(0) + "/api/v1/ts/" + getDatabaseName() + "/" + path).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty("Content-Type", contentType);
      connection.setRequestProperty("Content-Encoding", "snappy");
      connection.setDoOutput(true);
      try (final OutputStream os = connection.getOutputStream()) {
        os.write(body);
      }
      final int status = connection.getResponseCode();
      final InputStream stream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final String text = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, text);
    }
  }

  /**
   * Issue #9972: Prometheus remote write hands a label over as text, so a TIMESERIES type declaring a tag as anything but STRING
   * used to fail the whole request with a server error. Every label is now read through {@code ColumnDefinition.storableValue},
   * as the line protocol reads it: a convertible label is stored in the tag's type, and an unconvertible one drops only its series,
   * answered 400 and named in {@code unreadableValues}. The sample value reaches a non-DOUBLE field the same way.
   */
  @Nested
  class TypedTag extends BaseGraphServerTest {

    @Test
    void anIntegerTagIsStoredInItsType() throws Exception {
      final Database db = getServerDatabase(0, getDatabaseName());
      db.command("sql", "CREATE TIMESERIES TYPE p9972a TIMESTAMP ts TAGS (host INTEGER) FIELDS (v DOUBLE) SHARDS 1");

      final Response response = post(new WriteRequest(List.of(series("p9972a", 1.0, 1000, label("host", "42")))));
      assertThat(response.status).as(response.body).isEqualTo(204);

      try (final ResultSet rs = db.query("sql", "SELECT host, v FROM p9972a")) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("host").intValue()).isEqualTo(42);
        assertThat(r.<Number>getProperty("v").doubleValue()).isEqualTo(1.0);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void booleanAndDateTagsAreStoredInTheirTypes() throws Exception {
      final Database db = getServerDatabase(0, getDatabaseName());
      db.command("sql", "CREATE TIMESERIES TYPE p9972b TIMESTAMP ts TAGS (ok BOOLEAN, day DATE) FIELDS (v DOUBLE) SHARDS 1");

      final Response response = post(new WriteRequest(List.of(
          series("p9972b", 1.0, 1000, label("ok", "true"), label("day", "2026-10-14")))));
      assertThat(response.status).as(response.body).isEqualTo(204);

      try (final ResultSet rs = db.query("sql", "SELECT ok, day FROM p9972b")) {
        final Result r = rs.next();
        assertThat(r.<Boolean>getProperty("ok")).as("a BOOLEAN tag sent as \"true\" used to be stored as false").isTrue();
        final Object day = r.getProperty("day");
        final LocalDate stored = day instanceof LocalDate localDate ? localDate :
            ((Date) day).toInstant().atZone(ZoneOffset.UTC).toLocalDate();
        assertThat(stored).isEqualTo(LocalDate.of(2026, 10, 14));
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void anUnconvertibleLabelDropsOnlyItsSeriesAndIsNamed() throws Exception {
      final Database db = getServerDatabase(0, getDatabaseName());
      db.command("sql", "CREATE TIMESERIES TYPE p9972c TIMESTAMP ts TAGS (host INTEGER) FIELDS (v DOUBLE) SHARDS 1");

      final Response response = post(new WriteRequest(List.of(
          series("p9972c", 1.0, 1000, label("host", "7")),
          series("p9972c", 2.0, 2000, label("host", "not-a-number")))));
      assertThat(response.status).as("refused as the client's mistake, not a server error: " + response.body).isEqualTo(400);

      final JSONObject body = new JSONObject(response.body);
      assertThat(body.getInt("dropped")).isEqualTo(1);
      assertThat(body.getInt("droppedSamples")).isEqualTo(1);
      final JSONArray unreadable = body.getJSONArray("unreadableValues");
      assertThat(unreadable.length()).isEqualTo(1);
      assertThat(unreadable.getString(0)).startsWith("p9972c.host (tag): ");
      assertThat(body.getString("error")).contains("p9972c.host");

      assertThat(count(db, "p9972c")).as("the valid series of the same request is stored").isEqualTo(1);
    }

    @Test
    void aSampleValueAFieldCannotHoldDropsOnlyThatSample() throws Exception {
      final Database db = getServerDatabase(0, getDatabaseName());
      db.command("sql", "CREATE TIMESERIES TYPE p9972d TIMESTAMP ts TAGS (host STRING) FIELDS (value INTEGER) SHARDS 1");

      final List<Label> labels = List.of(new Label("__name__", "p9972d"), label("host", "h1"));
      final Response response = post(new WriteRequest(List.of(new TimeSeries(labels, List.of(
          new Sample(5.0, 1000),
          new Sample(1e12, 2000), // out of the INTEGER range: the append would store a truncated number
          new Sample(6.0, 3000))))));
      assertThat(response.status).as(response.body).isEqualTo(400);

      final JSONObject body = new JSONObject(response.body);
      assertThat(body.getInt("dropped")).as("no whole series is dropped").isEqualTo(0);
      assertThat(body.getInt("droppedSamples")).isEqualTo(1);
      assertThat(body.getJSONArray("unreadableValues").getString(0)).startsWith("p9972d.value (field): ");

      try (final ResultSet rs = db.query("sql", "SELECT value FROM p9972d ORDER BY ts")) {
        assertThat(rs.next().<Number>getProperty("value").intValue()).isEqualTo(5);
        assertThat(rs.next().<Number>getProperty("value").intValue()).isEqualTo(6);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    private static Label label(final String name, final String value) {
      return new Label(name, value);
    }

    private static TimeSeries series(final String metric, final double value, final long timestamp, final Label... labels) {
      final List<Label> all = new ArrayList<>();
      all.add(new Label("__name__", metric));
      all.addAll(List.of(labels));
      return new TimeSeries(all, List.of(new Sample(value, timestamp)));
    }

    private static long count(final Database db, final String type) {
      try (final ResultSet rs = db.query("sql", "SELECT count(*) AS n FROM " + type)) {
        return rs.next().<Long>getProperty("n");
      }
    }

    private record Response(int status, String body) {
    }

    private Response post(final WriteRequest request) throws Exception {
      final byte[] compressed = Snappy.compress(request.encode());
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          "http://127.0.0.1:" + getServerHttpPort(0) + "/api/v1/ts/" + getDatabaseName() + "/prom/write").toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty("Content-Type", "application/x-protobuf");
      connection.setRequestProperty("Content-Encoding", "snappy");
      connection.setDoOutput(true);
      try (final OutputStream os = connection.getOutputStream()) {
        os.write(compressed);
        os.flush();
      }
      final int status = connection.getResponseCode();
      final InputStream in = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    }
  }

  // Regression: #11991 a remote-write staleness marker keeps its bits and ends the series in PromQL
  /**
   * Regression test for issue #11991 over the wire: the staleness marker a Prometheus remote write carries (the NaN with bits
   * {@code 0x7ff0000000000002}) has to reach the PromQL evaluator with those very bits, through the protobuf decoder, the
   * auto-created DOUBLE column and the store. A marker that turned into an ordinary NaN on the way would be answered as a value
   * again, and the engine-level test, which appends the marker straight to the engine, could not tell.
   */
  @Nested
  class StalenessMarker extends BaseGraphServerTest {

    /** Prometheus' value.StaleNaN, spelled out so the test pins the wire value. */
    private static final double STALE = Double.longBitsToDouble(0x7ff0000000000002L);

    @Test
    void aSeriesEndedOverRemoteWriteLeavesTheInstantVector() throws Exception {
      testEachServer(serverIndex -> {
        postPromWrite(serverIndex, new WriteRequest(List.of(
            new TimeSeries(
                List.of(new Label("__name__", "stale_up"), new Label("job", "a")),
                List.of(new Sample(1.0, 1_000), new Sample(1.0, 16_000), new Sample(STALE, 31_000))),
            new TimeSeries(
                List.of(new Label("__name__", "stale_up"), new Label("job", "b")),
                List.of(new Sample(1.0, 1_000), new Sample(1.0, 16_000), new Sample(1.0, 31_000))),
            // an ORDINARY NaN is a value: Prometheus keeps the series and answers NaN
            new TimeSeries(
                List.of(new Label("__name__", "stale_up"), new Label("job", "c")),
                List.of(new Sample(Double.NaN, 31_000))))));

        // before the marker the ended series is still there
        assertThat(jobs(serverIndex, "stale_up", 20)).containsExactlyInAnyOrder("a", "b");
        // from the marker on it is gone, the ordinary NaN stays
        assertThat(jobs(serverIndex, "stale_up", 32)).containsExactlyInAnyOrder("b", "c");

        // the marker is no sample of a range: job a has the two samples at 1s and 16s in (-28s, 32s]
        final JSONArray counts = query(serverIndex, "count_over_time(stale_up{job=\"a\"}[1m])", 32);
        assertThat(counts.length()).isEqualTo(1);
        assertThat(counts.getJSONObject(0).getJSONArray("value").getString(1)).isEqualTo("2");
      });
    }

    private Set<String> jobs(final int serverIndex, final String promQL, final long atSeconds) throws Exception {
      final JSONArray result = query(serverIndex, promQL, atSeconds);
      final Set<String> jobs = new HashSet<>();
      for (int i = 0; i < result.length(); i++)
        jobs.add(result.getJSONObject(i).getJSONObject("metric").getString("job"));
      return jobs;
    }

    private JSONArray query(final int serverIndex, final String promQL, final long atSeconds) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(serverIndex,
          "/api/v1/ts/" + getDatabaseName() + "/prom/api/v1/query?query=" + URLEncoder.encode(promQL, StandardCharsets.UTF_8) + "&time="
              + atSeconds))
          .toURL().openConnection();
      try {
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Authorization", basicAuth());
        assertThat(connection.getResponseCode()).isEqualTo(200);
        try (final InputStream is = connection.getInputStream()) {
          final JSONObject response = new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
          assertThat(response.getString("status")).isEqualTo("success");
          return response.getJSONObject("data").getJSONArray("result");
        }
      } finally {
        connection.disconnect();
      }
    }

    private void postPromWrite(final int serverIndex, final WriteRequest writeRequest) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          getServerHttpUrl(serverIndex, "/api/v1/ts/" + getDatabaseName() + "/prom/write")).toURL().openConnection();
      try {
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization", basicAuth());
        connection.setRequestProperty("Content-Type", "application/x-protobuf");
        connection.setRequestProperty("Content-Encoding", "snappy");
        connection.setDoOutput(true);
        try (final OutputStream os = connection.getOutputStream()) {
          os.write(Snappy.compress(writeRequest.encode()));
        }
        assertThat(connection.getResponseCode()).isEqualTo(204);
      } finally {
        connection.disconnect();
      }
    }

    private static String basicAuth() {
      return "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    }
  }
}
