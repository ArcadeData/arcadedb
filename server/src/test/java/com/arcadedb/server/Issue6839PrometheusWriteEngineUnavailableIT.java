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
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
 * recovery from it) are pinned by {@code Issue6340TimeSeriesCheckDatabaseTest} and
 * {@code Issue6839TsSealedBlobRecoveryTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6839PrometheusWriteEngineUnavailableIT extends BaseGraphServerTest {

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
