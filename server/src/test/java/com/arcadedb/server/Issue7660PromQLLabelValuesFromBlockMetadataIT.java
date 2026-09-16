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
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7660: {@code GET /prom/api/v1/label/{name}/values} answers from what each sealed block already declares,
 * instead of decompressing every sample to recompute it.
 * <p>
 * The endpoint is the reason the change exists - a Grafana datasource calls it on every dashboard load and every
 * variable refresh - so the behaviour is pinned HERE, through HTTP, and not only at the engine API that
 * {@code Issue7660DistinctTagValuesFromBlockMetadataTest} covers. Each test compares the response against the scan
 * the handler used to run, over the same one-column projection, in the same JVM on the same data: the issue
 * accepted this optimisation only on condition that the answer stay EXACT, because Prometheus documents the
 * endpoint as returning the values a label actually carries and a stale extra one is visible in a picker.
 */
class Issue7660PromQLLabelValuesFromBlockMetadataIT extends BaseGraphServerTest {

  private static final String LOOPBACK  = "127.0.0.1";
  private static final long   BASE_TS   = 1_700_000_000_000L;
  private static final long   STEP_MS   = 10L;
  /** Bucket-aligned compaction, so a few thousand samples seal into dozens of blocks rather than one. */
  private static final long   BUCKET_MS = 1_000L;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(freePort()));
  }

  /**
   * The sealed layer and the mutable bucket together, with a host that exists only in the mutable one: the
   * response is the sorted set the scan produces, element for element and in the same order.
   */
  @Test
  void theResponseIsTheOneTheScanProduced() throws Exception {
    testEachServer(serverIndex -> {
      final TimeSeriesEngine engine = createMetric(serverIndex, "picker_metric", 2);
      appendHosts(engine, 0, 2_000, 6);
      engine.compactAll();
      appendHosts(engine, 1_000_000, 50, 1, "host_only_in_the_mutable_bucket");

      assertThat(engine.getShard(0).getSealedStore().getBlockCount())
          .as("more than one sealed block, so the answer is a union ACROSS directory entries")
          .isGreaterThan(1);

      assertThat(labelValuesOf(serverIndex, "host"))
          .isEqualTo(scanReference(engine, "host"))
          .contains("host_only_in_the_mutable_bucket");
    });
  }

  /**
   * A host whose every sample retention has dropped must not come back from the endpoint. This is the
   * over-approximation the issue refused to ship: a label picker offering a series that no surviving sample
   * carries is a behaviour change a user can see.
   */
  @Test
  void aHostRetentionDroppedIsNotOfferedByThePicker() throws Exception {
    testEachServer(serverIndex -> {
      final TimeSeriesEngine engine = createMetric(serverIndex, "expiring_metric", 1);

      appendHosts(engine, 0, 500, 1, "expired_host");
      engine.compactAll();
      appendHosts(engine, 100_000, 500, 1, "surviving_host");
      engine.compactAll();

      assertThat(labelValuesOf(serverIndex, "host"))
          .as("both hosts are sealed before retention runs, so the test is not asserting an absence that was "
              + "never a presence")
          .containsExactly("expired_host", "surviving_host");

      engine.applyRetention(BASE_TS + 50_000);

      assertThat(labelValuesOf(serverIndex, "host"))
          .isEqualTo(scanReference(engine, "host"))
          .containsExactly("surviving_host");
    });
  }

  /**
   * Two types declaring the same label fold into one answer, deduplicated and sorted across both - the loop the
   * handler runs over every TimeSeries type, which a per-type engine call must not have broken.
   */
  @Test
  void twoTypesSharingALabelAreUnionedAndSorted() throws Exception {
    testEachServer(serverIndex -> {
      final TimeSeriesEngine first = createMetric(serverIndex, "metric_one", 1);
      final TimeSeriesEngine second = createMetric(serverIndex, "metric_two", 1);

      appendHosts(first, 0, 100, 1, "zulu");
      appendHosts(first, 10_000, 100, 1, "alpha");
      appendHosts(second, 0, 100, 1, "mike");
      appendHosts(second, 10_000, 100, 1, "alpha");
      first.compactAll();
      // metric_two is deliberately left uncompacted, so one type answers from declarations and the other from a scan.

      assertThat(labelValuesOf(serverIndex, "host"))
          .as("the union of both types, deduplicated on 'alpha' and sorted as one list")
          .containsExactly("alpha", "mike", "zulu");
    });
  }

  /** A FIELD is not a label, however well its name resolves against the schema. */
  @Test
  void aFieldNameIsNotALabel() throws Exception {
    testEachServer(serverIndex -> {
      final TimeSeriesEngine engine = createMetric(serverIndex, "field_named_metric", 1);
      appendHosts(engine, 0, 100, 2);
      engine.compactAll();

      assertThat(labelValuesOf(serverIndex, "value")).isEmpty();
      assertThat(labelValuesOf(serverIndex, "no_such_label")).isEmpty();
    });
  }

  // --- helpers ---

  private TimeSeriesEngine createMetric(final int serverIndex, final String typeName, final int shards) {
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());
    new TimeSeriesTypeBuilder(database)
        .withName(typeName)
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .withField("value", Type.DOUBLE)
        .withShards(shards)
        .withCompactionBucketInterval(BUCKET_MS)
        .create();
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private static void appendHosts(final TimeSeriesEngine engine, final long startOffsetMs, final int count,
      final int hostCount, final String... fixedHost) throws IOException {
    final long[] timestamps = new long[count];
    final Object[] hosts = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = BASE_TS + startOffsetMs + i * STEP_MS;
      hosts[i] = fixedHost.length > 0 ? fixedHost[0] : "host_" + (i % hostCount);
      values[i] = (double) i;
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  /**
   * The response the handler produced before this change: a one-column projection folded through
   * {@code forEachRow}, taking the value from slot 1 of the projected row, skipping a null, then sorted.
   */
  private static List<String> scanReference(final TimeSeriesEngine engine, final String tag) throws IOException {
    final int[] columnIndices = TimeSeriesGateway.resolveColumnIndices(List.of(tag), engine.getColumns());
    final Set<String> values = new LinkedHashSet<>();
    engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, columnIndices, null, null, row -> {
      if (row.length > 1 && row[1] != null)
        values.add(row[1].toString());
      return true;
    });
    final List<String> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    return sorted;
  }

  /** The {@code data} array of a label-values response, as a list of strings. */
  private List<String> labelValuesOf(final int serverIndex, final String labelName) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://" + LOOPBACK + ":" + port + "/api/v1/ts/" + getDatabaseName() + "/prom/api/v1/label/" + labelName
            + "/values")
        .toURL()
        .openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final int responseCode = connection.getResponseCode();
    final InputStream is = responseCode >= 400 ? connection.getErrorStream() : connection.getInputStream();
    final JSONObject response = new JSONObject(readResponse(is));

    assertThat(response.getString("status")).isEqualTo("success");
    final JSONArray data = response.getJSONArray("data");
    final List<String> values = new ArrayList<>(data.length());
    for (int i = 0; i < data.length(); i++)
      values.add(data.getString(i));
    return values;
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
