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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import com.arcadedb.server.http.handler.prometheus.ProtobufDecoder;
import com.arcadedb.server.http.handler.prometheus.ProtobufEncoder;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9624: Prometheus remote write/read never looked at the request {@code Content-Type}, so a remote-write 2.0
 * payload ({@code io.prometheus.write.v2.Request}) decoded to zero 1.0 series and was answered 204, which the sender
 * counts as delivered; and a malformed protobuf body escaped the decoder as an {@link IllegalStateException} answered
 * 500 instead of 400.
 */
class Issue9624PrometheusRemoteWriteContentTypeIT extends BaseGraphServerTest {

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
