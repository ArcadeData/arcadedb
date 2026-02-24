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
package com.arcadedb.server.http.handler.prometheus;

import java.util.ArrayList;
import java.util.List;

/**
 * POJOs for Prometheus remote_write / remote_read protobuf messages.
 * Field numbers match the official prometheus.proto definitions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PrometheusTypes {

  private PrometheusTypes() {
  }

  // ---- WriteRequest (field 1 = repeated TimeSeries timeseries) ----

  public static final class WriteRequest {
    private final List<TimeSeries> timeSeries;

    public WriteRequest(final List<TimeSeries> timeSeries) {
      this.timeSeries = timeSeries;
    }

    public List<TimeSeries> getTimeSeries() {
      return timeSeries;
    }

    public static WriteRequest decode(final byte[] data) {
      final ProtobufDecoder decoder = new ProtobufDecoder(data);
      final List<TimeSeries> series = new ArrayList<>();
      while (decoder.hasRemaining()) {
        final int tag = decoder.readTag();
        if (ProtobufDecoder.fieldNumber(tag) == 1 && ProtobufDecoder.wireType(tag) == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          series.add(TimeSeries.decode(decoder.readLengthDelimited()));
        else
          decoder.skipField(ProtobufDecoder.wireType(tag));
      }
      return new WriteRequest(series);
    }

    public byte[] encode() {
      final ProtobufEncoder enc = new ProtobufEncoder();
      for (final TimeSeries ts : timeSeries)
        enc.writeSubMessage(1, ts.encode());
      return enc.toByteArray();
    }
  }

  // ---- TimeSeries (field 1 = repeated Label, field 2 = repeated Sample) ----

  public static final class TimeSeries {
    private final List<Label>  labels;
    private final List<Sample> samples;

    public TimeSeries(final List<Label> labels, final List<Sample> samples) {
      this.labels = labels;
      this.samples = samples;
    }

    public List<Label> getLabels() {
      return labels;
    }

    public List<Sample> getSamples() {
      return samples;
    }

    public String getMetricName() {
      for (final Label l : labels)
        if ("__name__".equals(l.name()))
          return l.value();
      return null;
    }

    public static TimeSeries decode(final byte[] data) {
      final ProtobufDecoder decoder = new ProtobufDecoder(data);
      final List<Label> labels = new ArrayList<>();
      final List<Sample> samples = new ArrayList<>();
      while (decoder.hasRemaining()) {
        final int tag = decoder.readTag();
        final int field = ProtobufDecoder.fieldNumber(tag);
        final int wire = ProtobufDecoder.wireType(tag);
        if (field == 1 && wire == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          labels.add(Label.decode(decoder.readLengthDelimited()));
        else if (field == 2 && wire == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          samples.add(Sample.decode(decoder.readLengthDelimited()));
        else
          decoder.skipField(wire);
      }
      return new TimeSeries(labels, samples);
    }

    public byte[] encode() {
      final ProtobufEncoder enc = new ProtobufEncoder();
      for (final Label l : labels)
        enc.writeSubMessage(1, l.encode());
      for (final Sample s : samples)
        enc.writeSubMessage(2, s.encode());
      return enc.toByteArray();
    }
  }

  // ---- Label (field 1 = name string, field 2 = value string) ----

  public record Label(String name, String value) {

    public static Label decode(final byte[] data) {
      final ProtobufDecoder decoder = new ProtobufDecoder(data);
      String name = "";
      String value = "";
      while (decoder.hasRemaining()) {
        final int tag = decoder.readTag();
        final int field = ProtobufDecoder.fieldNumber(tag);
        final int wire = ProtobufDecoder.wireType(tag);
        if (field == 1 && wire == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          name = decoder.readString();
        else if (field == 2 && wire == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          value = decoder.readString();
        else
          decoder.skipField(wire);
      }
      return new Label(name, value);
    }

    public byte[] encode() {
      final ProtobufEncoder enc = new ProtobufEncoder();
      enc.writeString(1, name);
      enc.writeString(2, value);
      return enc.toByteArray();
    }
  }

  // ---- Sample (field 1 = double value, field 2 = int64 timestamp_ms) ----

  public record Sample(double value, long timestampMs) {

    public static Sample decode(final byte[] data) {
      final ProtobufDecoder decoder = new ProtobufDecoder(data);
      double value = 0;
      long timestampMs = 0;
      while (decoder.hasRemaining()) {
        final int tag = decoder.readTag();
        final int field = ProtobufDecoder.fieldNumber(tag);
        final int wire = ProtobufDecoder.wireType(tag);
        if (field == 1 && wire == ProtobufDecoder.WIRETYPE_FIXED64)
          value = decoder.readFixed64AsDouble();
        else if (field == 2 && wire == ProtobufDecoder.WIRETYPE_VARINT)
          timestampMs = decoder.readVarint();
        else
          decoder.skipField(wire);
      }
      return new Sample(value, timestampMs);
    }

    public byte[] encode() {
      final ProtobufEncoder enc = new ProtobufEncoder();
      enc.writeTag(1, ProtobufDecoder.WIRETYPE_FIXED64);
      enc.writeFixed64(value);
      enc.writeTag(2, ProtobufDecoder.WIRETYPE_VARINT);
      enc.writeVarint(timestampMs);
      return enc.toByteArray();
    }
  }

  // ---- ReadRequest (field 1 = repeated Query queries) ----

  public static final class ReadRequest {
    private final List<Query> queries;

    public ReadRequest(final List<Query> queries) {
      this.queries = queries;
    }

    public List<Query> getQueries() {
      return queries;
    }

    public static ReadRequest decode(final byte[] data) {
      final ProtobufDecoder decoder = new ProtobufDecoder(data);
      final List<Query> queries = new ArrayList<>();
      while (decoder.hasRemaining()) {
        final int tag = decoder.readTag();
        if (ProtobufDecoder.fieldNumber(tag) == 1 && ProtobufDecoder.wireType(tag) == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          queries.add(Query.decode(decoder.readLengthDelimited()));
        else
          decoder.skipField(ProtobufDecoder.wireType(tag));
      }
      return new ReadRequest(queries);
    }

    public byte[] encode() {
      final ProtobufEncoder enc = new ProtobufEncoder();
      for (final Query q : queries)
        enc.writeSubMessage(1, q.encode());
      return enc.toByteArray();
    }
  }

  // ---- Query (field 1 = int64 start_timestamp_ms, field 2 = int64 end_timestamp_ms, field 3 = repeated LabelMatcher) ----

  public static final class Query {
    private final long               startTimestampMs;
    private final long               endTimestampMs;
    private final List<LabelMatcher> matchers;

    public Query(final long startTimestampMs, final long endTimestampMs, final List<LabelMatcher> matchers) {
      this.startTimestampMs = startTimestampMs;
      this.endTimestampMs = endTimestampMs;
      this.matchers = matchers;
    }

    public long getStartTimestampMs() {
      return startTimestampMs;
    }

    public long getEndTimestampMs() {
      return endTimestampMs;
    }

    public List<LabelMatcher> getMatchers() {
      return matchers;
    }

    public static Query decode(final byte[] data) {
      final ProtobufDecoder decoder = new ProtobufDecoder(data);
      long startTs = 0;
      long endTs = 0;
      final List<LabelMatcher> matchers = new ArrayList<>();
      while (decoder.hasRemaining()) {
        final int tag = decoder.readTag();
        final int field = ProtobufDecoder.fieldNumber(tag);
        final int wire = ProtobufDecoder.wireType(tag);
        if (field == 1 && wire == ProtobufDecoder.WIRETYPE_VARINT)
          startTs = decoder.readVarint();
        else if (field == 2 && wire == ProtobufDecoder.WIRETYPE_VARINT)
          endTs = decoder.readVarint();
        else if (field == 3 && wire == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          matchers.add(LabelMatcher.decode(decoder.readLengthDelimited()));
        else
          decoder.skipField(wire);
      }
      return new Query(startTs, endTs, matchers);
    }

    public byte[] encode() {
      final ProtobufEncoder enc = new ProtobufEncoder();
      enc.writeTag(1, ProtobufDecoder.WIRETYPE_VARINT);
      enc.writeVarint(startTimestampMs);
      enc.writeTag(2, ProtobufDecoder.WIRETYPE_VARINT);
      enc.writeVarint(endTimestampMs);
      for (final LabelMatcher m : matchers)
        enc.writeSubMessage(3, m.encode());
      return enc.toByteArray();
    }
  }

  // ---- LabelMatcher (field 1 = MatchType type, field 2 = name, field 3 = value) ----

  public enum MatchType {
    EQ(0), NEQ(1), RE(2), NRE(3);

    private final int value;

    MatchType(final int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public static MatchType fromValue(final int v) {
      return switch (v) {
        case 0 -> EQ;
        case 1 -> NEQ;
        case 2 -> RE;
        case 3 -> NRE;
        default -> EQ;
      };
    }
  }

  public record LabelMatcher(MatchType type, String name, String value) {

    public static LabelMatcher decode(final byte[] data) {
      final ProtobufDecoder decoder = new ProtobufDecoder(data);
      MatchType type = MatchType.EQ;
      String name = "";
      String value = "";
      while (decoder.hasRemaining()) {
        final int tag = decoder.readTag();
        final int field = ProtobufDecoder.fieldNumber(tag);
        final int wire = ProtobufDecoder.wireType(tag);
        if (field == 1 && wire == ProtobufDecoder.WIRETYPE_VARINT)
          type = MatchType.fromValue((int) decoder.readVarint());
        else if (field == 2 && wire == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          name = decoder.readString();
        else if (field == 3 && wire == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          value = decoder.readString();
        else
          decoder.skipField(wire);
      }
      return new LabelMatcher(type, name, value);
    }

    public byte[] encode() {
      final ProtobufEncoder enc = new ProtobufEncoder();
      enc.writeTag(1, ProtobufDecoder.WIRETYPE_VARINT);
      enc.writeVarint(type.getValue());
      enc.writeString(2, name);
      enc.writeString(3, value);
      return enc.toByteArray();
    }
  }

  // ---- ReadResponse (field 1 = repeated QueryResult results) ----

  public static final class ReadResponse {
    private final List<QueryResult> results;

    public ReadResponse(final List<QueryResult> results) {
      this.results = results;
    }

    public List<QueryResult> getResults() {
      return results;
    }

    public static ReadResponse decode(final byte[] data) {
      final ProtobufDecoder decoder = new ProtobufDecoder(data);
      final List<QueryResult> results = new ArrayList<>();
      while (decoder.hasRemaining()) {
        final int tag = decoder.readTag();
        if (ProtobufDecoder.fieldNumber(tag) == 1 && ProtobufDecoder.wireType(tag) == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          results.add(QueryResult.decode(decoder.readLengthDelimited()));
        else
          decoder.skipField(ProtobufDecoder.wireType(tag));
      }
      return new ReadResponse(results);
    }

    public byte[] encode() {
      final ProtobufEncoder enc = new ProtobufEncoder();
      for (final QueryResult qr : results)
        enc.writeSubMessage(1, qr.encode());
      return enc.toByteArray();
    }
  }

  // ---- QueryResult (field 1 = repeated TimeSeries timeseries) ----

  public static final class QueryResult {
    private final List<TimeSeries> timeSeries;

    public QueryResult(final List<TimeSeries> timeSeries) {
      this.timeSeries = timeSeries;
    }

    public List<TimeSeries> getTimeSeries() {
      return timeSeries;
    }

    public static QueryResult decode(final byte[] data) {
      final ProtobufDecoder decoder = new ProtobufDecoder(data);
      final List<TimeSeries> series = new ArrayList<>();
      while (decoder.hasRemaining()) {
        final int tag = decoder.readTag();
        if (ProtobufDecoder.fieldNumber(tag) == 1 && ProtobufDecoder.wireType(tag) == ProtobufDecoder.WIRETYPE_LENGTH_DELIMITED)
          series.add(TimeSeries.decode(decoder.readLengthDelimited()));
        else
          decoder.skipField(ProtobufDecoder.wireType(tag));
      }
      return new QueryResult(series);
    }

    public byte[] encode() {
      final ProtobufEncoder enc = new ProtobufEncoder();
      for (final TimeSeries ts : timeSeries)
        enc.writeSubMessage(1, ts.encode());
      return enc.toByteArray();
    }
  }
}
