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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7709: {@code GET /prom/api/v1/label/{name}/values} honours {@code start} and {@code end}.
 * <p>
 * The handler read neither, so it resolved the answer over the whole retention: a Grafana picker scoped to the
 * last hour was offered every value the type had ever held, and the sibling {@code /series} endpoint - which does
 * read them - disagreed with this one about what a range means. Pinned through HTTP because that disagreement is
 * between two HTTP endpoints; the engine-level bound has its own test in
 * {@code Issue7709RangeScopedDistinctTagValuesTest}.
 * <p>
 * The compatibility half matters as much as the fix: a request carrying neither bound must keep answering over the
 * whole series, because that is every request any deployed dashboard sends today.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7709PromQLLabelValuesRangeIT extends BaseGraphServerTest {

  private static final String LOOPBACK  = "127.0.0.1";
  private static final long   BASE_TS   = 1_700_000_000_000L;
  private static final long   STEP_MS   = 10L;
  private static final long   BUCKET_MS = 1_000L;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(freePort()));
  }

  /** The defect: an old host must not be offered to a picker scoped to the recent window. */
  @Test
  void aWindowedRequestOnlyNamesTheValuesTheWindowCarries() throws Exception {
    testEachServer(serverIndex -> {
      final TimeSeriesEngine engine = createMetric(serverIndex, "ranged_metric");
      appendHosts(engine, 0, 500, "old_host");
      appendHosts(engine, 100_000, 500, "recent_host");
      engine.compactAll();

      assertThat(labelValuesOf(serverIndex, "host", null, null))
          .as("unscoped, the answer is the whole series - which is also what it always answered")
          .containsExactly("old_host", "recent_host");

      assertThat(labelValuesOf(serverIndex, "host", seconds(BASE_TS + 100_000), null))
          .containsExactly("recent_host");
      assertThat(labelValuesOf(serverIndex, "host", null, seconds(BASE_TS + 5_000)))
          .containsExactly("old_host");
      assertThat(labelValuesOf(serverIndex, "host", seconds(BASE_TS + 10_000_000), seconds(BASE_TS + 20_000_000)))
          .isEmpty();
    });
  }

  /**
   * {@code __name__} is a metric name rather than a tag value, and it takes the same bound: a type holding no
   * sample in the window is not a metric the window carries. Unscoped it still names every type, a type with no
   * sample at all included, which is what it answered before the bounds existed.
   */
  @Test
  void theMetricNameLabelIsScopedToTheWindowToo() throws Exception {
    testEachServer(serverIndex -> {
      final TimeSeriesEngine recent = createMetric(serverIndex, "recent_only_metric");
      final TimeSeriesEngine old = createMetric(serverIndex, "old_only_metric");
      createMetric(serverIndex, "sampleless_metric");

      appendHosts(old, 0, 200, "h");
      appendHosts(recent, 500_000, 200, "h");
      old.compactAll();
      recent.compactAll();

      assertThat(labelValuesOf(serverIndex, "__name__", null, null))
          .as("unscoped names every TimeSeries type, whether or not it holds a sample")
          .contains("recent_only_metric", "old_only_metric", "sampleless_metric");

      final List<String> windowed = labelValuesOf(serverIndex, "__name__", seconds(BASE_TS + 500_000), null);
      assertThat(windowed).contains("recent_only_metric");
      assertThat(windowed).doesNotContain("old_only_metric", "sampleless_metric");
    });
  }

  /** Malformed bounds answer {@code 400 bad_data}, the way the other PromQL handlers do. */
  @Test
  void aMalformedBoundIsRefusedAsBadData() throws Exception {
    testEachServer(serverIndex -> {
      createMetric(serverIndex, "refusal_metric");

      assertBadData(serverIndex, "host", "not-a-timestamp", null, "start");
      assertBadData(serverIndex, "host", null, "NaN", "end");
      // Double.parseDouble accepts these without throwing; the range endpoints refuse them and so does this one.
      assertBadData(serverIndex, "host", "Infinity", null, "finite");
      assertBadData(serverIndex, "host", "9e15", null, "epoch range");
      // An inverted range cannot have been meant, and says so instead of answering an empty list.
      assertBadData(serverIndex, "host", seconds(BASE_TS + 1_000), seconds(BASE_TS), "before start");
    });
  }

  /**
   * The sibling endpoint, found while giving this one the same bounds: {@code /series} parsed {@code start} with a
   * bare {@code Double.parseDouble}, so a malformed value left the handler as a {@code NumberFormatException} and
   * the pipeline answered {@code 500} where Prometheus answers {@code 400 bad_data} - and {@code Infinity} and
   * {@code 9e15} were accepted outright, which is the unbounded-span hazard issue #6807 closed on
   * {@code /query_range}.
   */
  @Test
  void theSeriesEndpointRefusesAMalformedBoundToo() throws Exception {
    testEachServer(serverIndex -> {
      createMetric(serverIndex, "series_refusal_metric");

      assertSeriesBadData(serverIndex, "not-a-timestamp", "start");
      assertSeriesBadData(serverIndex, "Infinity", "finite");
      assertSeriesBadData(serverIndex, "9e15", "epoch range");
    });
  }

  private void assertSeriesBadData(final int serverIndex, final String start, final String messageFragment)
      throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://" + LOOPBACK + ":" + port + "/api/v1/ts/" + getDatabaseName()
            + "/prom/api/v1/series?match[]=series_refusal_metric&start=" + start)
        .toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    assertThat(connection.getResponseCode()).isEqualTo(400);
    final JSONObject response = new JSONObject(readResponse(connection.getErrorStream()));
    assertThat(response.getString("errorType")).isEqualTo("bad_data");
    assertThat(response.getString("error")).contains(messageFragment);
  }

  // --- helpers ---

  private static String seconds(final long millis) {
    return String.valueOf(millis / 1000.0);
  }

  private TimeSeriesEngine createMetric(final int serverIndex, final String typeName) {
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());
    new TimeSeriesTypeBuilder(database)
        .withName(typeName)
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .withField("value", Type.DOUBLE)
        .withShards(1)
        .withCompactionBucketInterval(BUCKET_MS)
        .create();
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private static void appendHosts(final TimeSeriesEngine engine, final long startOffsetMs, final int count,
      final String host) throws IOException {
    final long[] timestamps = new long[count];
    final Object[] hosts = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = BASE_TS + startOffsetMs + i * STEP_MS;
      hosts[i] = host;
      values[i] = (double) i;
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  /** The {@code data} array of a label-values response, as a list of strings. */
  private List<String> labelValuesOf(final int serverIndex, final String labelName, final String start,
      final String end) throws Exception {
    final JSONObject response = call(serverIndex, labelName, start, end, 200);
    assertThat(response.getString("status")).isEqualTo("success");
    final JSONArray data = response.getJSONArray("data");
    final List<String> values = new ArrayList<>(data.length());
    for (int i = 0; i < data.length(); i++)
      values.add(data.getString(i));
    return values;
  }

  private void assertBadData(final int serverIndex, final String labelName, final String start, final String end,
      final String messageFragment) throws Exception {
    final JSONObject response = call(serverIndex, labelName, start, end, 400);
    assertThat(response.getString("status")).isEqualTo("error");
    assertThat(response.getString("errorType")).isEqualTo("bad_data");
    assertThat(response.getString("error")).contains(messageFragment);
  }

  private JSONObject call(final int serverIndex, final String labelName, final String start, final String end,
      final int expectedCode) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    final StringBuilder url = new StringBuilder("http://").append(LOOPBACK).append(':').append(port)
        .append("/api/v1/ts/").append(getDatabaseName()).append("/prom/api/v1/label/").append(labelName)
        .append("/values");
    if (start != null)
      url.append(url.indexOf("?") < 0 ? '?' : '&').append("start=").append(start);
    if (end != null)
      url.append(url.indexOf("?") < 0 ? '?' : '&').append("end=").append(end);

    final HttpURLConnection connection = (HttpURLConnection) new URI(url.toString()).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final int responseCode = connection.getResponseCode();
    assertThat(responseCode).isEqualTo(expectedCode);
    final InputStream is = responseCode >= 400 ? connection.getErrorStream() : connection.getInputStream();
    return new JSONObject(readResponse(is));
  }

  private static String readResponse(final InputStream is) throws Exception {
    if (is == null)
      return "{}";
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final byte[] buffer = new byte[4096];
    int read;
    while ((read = is.read(buffer)) != -1)
      baos.write(buffer, 0, read);
    return baos.toString(StandardCharsets.UTF_8);
  }

  /** An ephemeral port the OS has just handed out, released again immediately so the server can take it. */
  private static int freePort() {
    try (final ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName(LOOPBACK))) {
      return socket.getLocalPort();
    } catch (final IOException e) {
      throw new RuntimeException("Cannot reserve a free port for the test server", e);
    }
  }
}
