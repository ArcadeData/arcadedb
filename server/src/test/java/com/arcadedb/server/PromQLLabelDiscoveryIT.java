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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PromQL label and series discovery over HTTP ({@code /api/v1/labels}, {@code /api/v1/label/{name}/values}): the
 * column projection of the discovered tags, the label values answered from block metadata, and the time range a
 * label-values request honours. Each nested class binds the HTTP listener to the loopback interface on a free port.
 */
class PromQLLabelDiscoveryIT {

  /**
   * Issue #7371: the two PromQL discovery endpoints answer a question about TAG columns, so a TAG column is the only
   * thing their scan has any reason to decompress. Both now hand {@code TimeSeriesEngine.forEachRow} a projection,
   * which is not only cheaper - a sealed block keeps each column in its own byte range and
   * {@code TimeSeriesSealedStore.decompressColumns()} reads only the ranges asked for, so the DOUBLE value column of
   * a Prometheus metric is never Gorilla-decoded - but also changes the SHAPE of the row the handler reads, and
   * therefore the slot each tag value arrives in.
   * <p>
   * <b>The schema order is what makes that assertable.</b> {@code proj_metric} declares its DOUBLE FIELD
   * <em>before</em> its two TAG columns: {@code [timestamp, value, host, region]}. {@code CREATE TIMESERIES TYPE}
   * cannot spell that (its grammar takes {@code TAGS (...)} then {@code FIELDS (...)}) and the Prometheus
   * remote-write path does not produce it ({@code PostPrometheusWriteHandler} appends {@code value} last), but
   * {@link TimeSeriesTypeBuilder} accepts it and nothing in the engine forbids it. Against that layout the two
   * plausible ways to get the projection wrong both produce a visibly wrong answer:
   * <ul>
   * <li>projecting but indexing by the column's <em>schema</em> position - what the pre-#7371 code did, correctly,
   * against the full row - runs off the end of a two-element row and answers nothing;
   * <li>keeping the projected slot but dropping the projection reads {@code row[1]} out of the full row, which is
   * the DOUBLE column: {@code "10.0"}, {@code "20.0"} where {@code "h1"}, {@code "h2"} belong.
   * </ul>
   * <p>
   * The suite's shared 2480-2489 range is deliberately not used. These tests read the body of every response and
   * assert on it, so a neighbouring listener - another test class mid-shutdown, an IDE, a locally installed
   * ArcadeDB - answering even once would be read as the server under test saying something it never said.
   */
  @Nested
  class DiscoveryProjection extends BaseGraphServerTest {

    private static final String LOOPBACK    = "127.0.0.1";
    private static final String METRIC_NAME = "proj_metric";

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(allocateFreePorts(1)[0]));
    }

    /**
     * The distinct values of one TAG column, read from the slot the projection puts it in rather than from its
     * schema index. {@code host} is the second non-timestamp column of {@code proj_metric}, so the two indices
     * differ and the answer says which one was used.
     */
    @Test
    void labelValuesReadTheTagColumnAndNotItsNeighbour() throws Exception {
      testEachServer(serverIndex -> {
        createFieldBeforeTagsMetric(serverIndex);

        assertThat(labelValuesOf(serverIndex, "host"))
            .as("the distinct values of the host TAG column, not of the DOUBLE column that precedes it in the schema")
            .containsExactly("h1", "h2");

        assertThat(labelValuesOf(serverIndex, "region"))
            .as("a TAG column further from the timestamp is still read from the slot the projection puts it in")
            .containsExactly("eu", "us");
      });
    }

    /**
     * The series endpoint selects every TAG column at once, so its projected row is
     * {@code { timestamp, host, region }} and each label's name has to come from the projection rather than from the
     * schema index. A handler that dropped the projection would report the DOUBLE value column under the name
     * {@code host} and shift every other label by one.
     */
    @Test
    void seriesReadsEachTagFromItsProjectedSlot() throws Exception {
      testEachServer(serverIndex -> {
        createFieldBeforeTagsMetric(serverIndex);

        final JSONArray data = get(serverIndex, "series?match%5B%5D=" + encode(METRIC_NAME)).getJSONArray("data");

        final List<String> combinations = new ArrayList<>();
        for (int i = 0; i < data.length(); i++) {
          final JSONObject series = data.getJSONObject(i);
          assertThat(series.getString("__name__")).isEqualTo(METRIC_NAME);
          assertThat(series.has("value"))
              .as("the DOUBLE FIELD is not a label, so the projection must not carry it into the series")
              .isFalse();
          combinations.add(series.getString("host") + "/" + series.getString("region"));
        }

        assertThat(combinations)
            .as("three samples over two label combinations, each tag read from its own projected slot")
            .containsExactly("h1/eu", "h2/us");
      });
    }

    /**
     * The same projection over the layout the Prometheus remote-write path actually produces,
     * {@code [timestamp, host, rack, value]}. {@code rack} sits at schema index 2 and at projected slot 1, so a
     * handler reading the schema index off a projected row answers nothing and one reading slot 1 off an unprojected
     * row answers with {@code host}'s values instead.
     */
    @Test
    void labelValuesOfASecondPrometheusTagAreItsOwnValues() throws Exception {
      testEachServer(serverIndex -> {
        final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());

        new TimeSeriesTypeBuilder(database)
            .withName("prom_shaped")
            .withTimestamp("timestamp")
            .withTag("host", Type.STRING)
            .withTag("rack", Type.STRING)
            .withField("value", Type.DOUBLE)
            .withShards(1)
            .create();

        final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("prom_shaped")).getEngine();
        database.begin();
        engine.appendSamples(new long[] { 1000L, 2000L, 3000L },
            new Object[] { "hostA", "hostA", "hostB" },
            new Object[] { "rackA", "rackA", "rackB" },
            new Object[] { 1.0, 2.0, 3.0 });
        database.commit();

        assertThat(labelValuesOf(serverIndex, "rack"))
            .as("the second TAG column's own distinct values, not the first one's")
            .containsExactly("rackA", "rackB");
      });
    }

    /**
     * A metric whose only TAG column is asked for by a label the type does not declare stays out of the answer, and
     * the endpoint does not read a value out of whatever column happens to sit at that index.
     */
    @Test
    void labelValuesForAColumnThatIsNotATagIsEmpty() throws Exception {
      testEachServer(serverIndex -> {
        createFieldBeforeTagsMetric(serverIndex);

        assertThat(labelValuesOf(serverIndex, "value"))
            .as("a FIELD is not a label, however well its name resolves against the schema")
            .isEmpty();
      });
    }

    /**
     * The same two answers once the samples are in the SEALED layer, which is the layer the projection was added
     * for: {@code TimeSeriesSealedStore.decompressColumns()} reads only the byte ranges of the columns the
     * projection names, so the DOUBLE column is not Gorilla-decoded at all. The mutable-bucket path above and this
     * one build their rows in different code ({@code TimeSeriesBucket.readRow} vs {@code decompressColumns}), so
     * both have to be asserted or a projection correct in one of them ships broken in the other.
     */
    @Test
    void theSameAnswersComeBackOutOfTheSealedLayer() throws Exception {
      testEachServer(serverIndex -> {
        createFieldBeforeTagsMetric(serverIndex);
        engineOf(serverIndex, METRIC_NAME).compactAll();

        assertThat(labelValuesOf(serverIndex, "host"))
            .as("a compacted type answers with its tag values, not with the column next to them")
            .containsExactly("h1", "h2");

        final JSONArray data = get(serverIndex, "series?match%5B%5D=" + encode(METRIC_NAME)).getJSONArray("data");
        final List<String> combinations = new ArrayList<>();
        for (int i = 0; i < data.length(); i++)
          combinations.add(data.getJSONObject(i).getString("host") + "/" + data.getJSONObject(i).getString("region"));

        assertThat(combinations)
            .as("the sealed layer enumerates the same two label combinations")
            .containsExactly("h1/eu", "h2/us");
      });
    }

    /**
     * A metric with no TAG column at all. {@code TimeSeriesGateway.resolveColumnIndices} answers {@code null} -
     * "every column" - for an empty request, which is the opposite of what this needs, so the series handler spells
     * out the empty projection instead. What must not change is the answer: one series, carrying nothing but
     * {@code __name__}, and only because the metric has samples in the range.
     */
    @Test
    void aMetricWithNoTagColumnsIsOneUnlabelledSeries() throws Exception {
      testEachServer(serverIndex -> {
        final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());

        new TimeSeriesTypeBuilder(database)
            .withName("untagged")
            .withTimestamp("timestamp")
            .withField("value", Type.DOUBLE)
            .withShards(1)
            .create();

        database.begin();
        engineOf(serverIndex, "untagged").appendSamples(new long[] { 1000L, 2000L }, new Object[] { 1.0, 2.0 });
        database.commit();

        // Both layers: the empty projection takes a different branch out of decompressColumns() (an empty column
        // array) than a non-empty one, so asserting only the mutable bucket would leave that branch unrun.
        for (final String layer : new String[] { "mutable", "sealed" }) {
          if ("sealed".equals(layer))
            engineOf(serverIndex, "untagged").compactAll();

          final JSONArray data = get(serverIndex, "series?match%5B%5D=" + encode("untagged")).getJSONArray("data");

          assertThat(data.length())
              .as("one series, deduplicated across both samples (" + layer + " layer)")
              .isEqualTo(1);
          final JSONObject series = data.getJSONObject(0);
          assertThat(series.getString("__name__")).isEqualTo("untagged");
          assertThat(series.toMap().keySet())
              .as("a type with no TAG column contributes no labels, and the DOUBLE FIELD is not one (" + layer + ")")
              .containsExactly("__name__");
        }
      });
    }

    /**
     * The response order the handler states and the sort the label endpoint applies, on the layout this class uses.
     * Both survive the projection because neither is derived from it - the series order comes from the earliest
     * timestamp each combination was observed at, and the label order from a sort of the distinct set - but a
     * projection that reordered the columns would move both, so they are pinned here rather than assumed.
     * <p>
     * The later series is written FIRST, in its own append, so a handler answering in traversal order would put it
     * first and this assertion would catch it.
     */
    @Test
    void theResponseOrderSurvivesTheProjection() throws Exception {
      testEachServer(serverIndex -> {
        final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());

        new TimeSeriesTypeBuilder(database)
            .withName("ordered_metric")
            .withTimestamp("timestamp")
            .withField("value", Type.DOUBLE)
            .withTag("host", Type.STRING)
            .withShards(1)
            .create();

        final TimeSeriesEngine engine = engineOf(serverIndex, "ordered_metric");
        database.begin();
        engine.appendSamples(new long[] { 5000L, 6000L }, new Object[] { 1.0, 2.0 }, new Object[] { "later", "later" });
        database.commit();
        database.begin();
        engine.appendSamples(new long[] { 1000L, 2000L }, new Object[] { 3.0, 4.0 },
            new Object[] { "earlier", "earlier" });
        database.commit();

        final JSONArray data = get(serverIndex, "series?match%5B%5D=" + encode("ordered_metric")).getJSONArray("data");
        final List<String> hosts = new ArrayList<>();
        for (int i = 0; i < data.length(); i++)
          hosts.add(data.getJSONObject(i).getString("host"));

        assertThat(hosts)
            .as("earliest sample first, whatever order the rows were written or are traversed in")
            .containsExactly("earlier", "later");

        assertThat(labelValuesOf(serverIndex, "host"))
            .as("the label endpoint still sorts the distinct set it returns")
            .containsExactly("earlier", "later");
      });
    }

    /**
     * The assumption the previous code rested on, made false. Both handlers used to read a tag value out of
     * {@code row[<the column's SCHEMA index>]}, which lines up with the row only while the TIMESTAMP column is
     * declared first - both files said so in a comment, and nothing in {@link TimeSeriesTypeBuilder} enforces it.
     * <p>
     * {@code mid_ts_metric} declares {@code [host, ts, region]}. The scan builds {@code { ts, host, region }},
     * because the timestamp is written at the head of the row whatever its declared position
     * ({@code TimeSeriesBucket.readRow} assigns {@code result[0]} before walking the columns at all), so the old
     * code read {@code host} out of slot 0 and answered with TIMESTAMPS - {@code "1000"}, {@code "2000"} - while
     * {@code region}, two slots along, came back right. Resolving the slot through
     * {@code TimeSeriesGateway.selectedColumns()} removes the assumption rather than restating it.
     * <p>
     * This is the one assertion in this class that is red on {@code main}.
     */
    @Test
    void aTimestampDeclaredAfterATagDoesNotShiftTheAnswer() throws Exception {
      testEachServer(serverIndex -> {
        final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());

        new TimeSeriesTypeBuilder(database)
            .withName("mid_ts_metric")
            .withTag("host", Type.STRING)
            .withTimestamp("ts")
            .withTag("region", Type.STRING)
            .withShards(1)
            .create();

        database.begin();
        engineOf(serverIndex, "mid_ts_metric").appendSamples(new long[] { 1000L, 2000L },
            new Object[] { "h1", "h2" },
            new Object[] { "eu", "us" });
        database.commit();

        assertThat(labelValuesOf(serverIndex, "host"))
            .as("the host TAG's values, not the timestamps that the row starts with")
            .containsExactly("h1", "h2");

        final JSONArray data = get(serverIndex, "series?match%5B%5D=" + encode("mid_ts_metric")).getJSONArray("data");
        final List<String> combinations = new ArrayList<>();
        for (int i = 0; i < data.length(); i++)
          combinations.add(data.getJSONObject(i).getString("host") + "/" + data.getJSONObject(i).getString("region"));

        assertThat(combinations)
            .as("two samples, two label combinations, neither of them carrying a timestamp as a label value")
            .containsExactly("h1/eu", "h2/us");
      });
    }

    private TimeSeriesEngine engineOf(final int serverIndex, final String typeName) {
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    }

    /**
     * Declares {@code proj_metric} with its DOUBLE FIELD ahead of its two TAG columns and writes three samples over
     * two label combinations. Schema order is {@code [timestamp, value, host, region]}.
     */
    private void createFieldBeforeTagsMetric(final int serverIndex) throws Exception {
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());

      new TimeSeriesTypeBuilder(database)
          .withName(METRIC_NAME)
          .withTimestamp("timestamp")
          .withField("value", Type.DOUBLE)
          .withTag("host", Type.STRING)
          .withTag("region", Type.STRING)
          .withShards(1)
          .create();

      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(METRIC_NAME)).getEngine();

      database.begin();
      engine.appendSamples(new long[] { 1000L, 2000L, 3000L },
          new Object[] { 10.0, 20.0, 30.0 },
          new Object[] { "h1", "h1", "h2" },
          new Object[] { "eu", "eu", "us" });
      database.commit();
    }

    /** The {@code data} array of a label-values response, as a list of strings. */
    private List<String> labelValuesOf(final int serverIndex, final String labelName) throws Exception {
      final JSONObject response = get(serverIndex, "label/" + labelName + "/values");
      assertThat(response.getString("status")).isEqualTo("success");
      final JSONArray data = response.getJSONArray("data");
      final List<String> values = new ArrayList<>(data.length());
      for (int i = 0; i < data.length(); i++)
        values.add(data.getString(i));
      return values;
    }

    private JSONObject get(final int serverIndex, final String path) throws Exception {
      final int port = getServer(serverIndex).getHttpServer().getPort();
      final HttpURLConnection connection = (HttpURLConnection) new URI(
          "http://" + LOOPBACK + ":" + port + "/api/v1/ts/" + getDatabaseName() + "/prom/api/v1/" + path)
          .toURL()
          .openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final int responseCode = connection.getResponseCode();
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

    private static String encode(final String value) {
      return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
  }

  /**
   * Issue #7660: {@code GET /prom/api/v1/label/{name}/values} answers from what each sealed block already declares,
   * instead of decompressing every sample to recompute it.
   * <p>
   * The endpoint is the reason the change exists - a Grafana datasource calls it on every dashboard load and every
   * variable refresh - so the behaviour is pinned HERE, through HTTP, and not only at the engine API that
   * {@code TagFilterTest} covers. Each test compares the response against the scan
   * the handler used to run, over the same one-column projection, in the same JVM on the same data: the issue
   * accepted this optimisation only on condition that the answer stay EXACT, because Prometheus documents the
   * endpoint as returning the values a label actually carries and a stale extra one is visible in a picker.
   */
  @Nested
  class LabelValuesFromBlockMetadata extends BaseGraphServerTest {

    private static final String LOOPBACK  = "127.0.0.1";
    private static final long   BASE_TS   = 1_700_000_000_000L;
    private static final long   STEP_MS   = 10L;
    /** Bucket-aligned compaction, so a few thousand samples seal into dozens of blocks rather than one. */
    private static final long   BUCKET_MS = 1_000L;

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(allocateFreePorts(1)[0]));
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
  }

  /**
   * Issue #7709: {@code GET /prom/api/v1/label/{name}/values} honours {@code start} and {@code end}.
   * <p>
   * The handler read neither, so it resolved the answer over the whole retention: a Grafana picker scoped to the
   * last hour was offered every value the type had ever held, and the sibling {@code /series} endpoint - which does
   * read them - disagreed with this one about what a range means. Pinned through HTTP because that disagreement is
   * between two HTTP endpoints; the engine-level bound has its own test in
   * {@code TagFilterTest}.
   * <p>
   * The compatibility half matters as much as the fix: a request carrying neither bound must keep answering over the
   * whole series, because that is every request any deployed dashboard sends today.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LabelValuesRange extends BaseGraphServerTest {

    private static final String LOOPBACK  = "127.0.0.1";
    private static final long   BASE_TS   = 1_700_000_000_000L;
    private static final long   STEP_MS   = 10L;
    private static final long   BUCKET_MS = 1_000L;

    @Override
    protected void onServerConfiguration(final ContextConfiguration config) {
      super.onServerConfiguration(config);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(allocateFreePorts(1)[0]));
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
  }
}
