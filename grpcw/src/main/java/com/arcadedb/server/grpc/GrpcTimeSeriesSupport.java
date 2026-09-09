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
package com.arcadedb.server.grpc;

import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.LineProtocolParser.Precision;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.TypeResolution;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates the {@code TimeSeries*} protobuf shapes to and from what {@link TimeSeriesGateway} takes, so
 * {@link ArcadeDbGrpcService} carries only the RPC plumbing (issue #7305).
 * <p>
 * Nothing here decides ingest or query semantics: it converts, and it turns a gateway resolution failure into
 * the gRPC status that says the same thing as the HTTP endpoint's 400.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GrpcTimeSeriesSupport {

  private GrpcTimeSeriesSupport() {
  }

  /**
   * Maps the proto precision enum to the engine's. Note that the proto's zero value is MILLISECONDS, not the
   * NANOSECONDS the HTTP line-protocol endpoint defaults to: this API is typed, so its default is the unit the
   * engine stores rather than the unit InfluxDB's text format happens to use.
   */
  static Precision toPrecision(final TimeSeriesPrecision precision) {
    return switch (precision) {
      case TS_PRECISION_NANOSECONDS -> Precision.NANOSECONDS;
      case TS_PRECISION_MICROSECONDS -> Precision.MICROSECONDS;
      case TS_PRECISION_SECONDS -> Precision.SECONDS;
      // MILLISECONDS is the zero value; UNRECOGNIZED means a newer client sent a unit this server does not
      // know, and silently treating that as milliseconds would store timestamps off by a factor of a million.
      case TS_PRECISION_MILLISECONDS -> Precision.MILLISECONDS;
      case UNRECOGNIZED -> throw Status.INVALID_ARGUMENT
          .withDescription("Unrecognized TimeSeriesPrecision; this server understands ns, us, ms and s")
          .asRuntimeException();
    };
  }

  /**
   * Converts points to the engine's {@link Sample}s.
   * <p>
   * Tag values are converted to their string form, which is what {@code LineProtocolParser} produces for the
   * HTTP path: {@link Sample} models tags as {@code Map<String, String>}, and the tag column coerces the text
   * to its declared type on the way in. Sending gRPC's typed value through unchanged would make the two
   * protocols store a tag differently, which is exactly the divergence this whole change exists to prevent.
   * Field values keep their gRPC type, as the parser's typed field values do.
   *
   * @param points      the points to convert
   * @param defaultType measurement for points that name none; may be empty when every point names its own
   * @param precision   unit of {@code TimeSeriesPoint.timestamp}
   */
  static List<Sample> toSamples(final List<TimeSeriesPoint> points, final String defaultType,
      final TimeSeriesPrecision precision) {
    final Precision unit = toPrecision(precision);
    final List<Sample> samples = new ArrayList<>(points.size());

    for (final TimeSeriesPoint point : points) {
      final String measurement = point.getType().isEmpty() ? defaultType : point.getType();
      if (measurement == null || measurement.isEmpty())
        throw Status.INVALID_ARGUMENT
            .withDescription("Every TimeSeriesPoint needs a type, either on the point or on the request")
            .asRuntimeException();

      final Map<String, String> tags = new LinkedHashMap<>();
      for (final Map.Entry<String, GrpcValue> tag : point.getTagsMap().entrySet()) {
        final Object value = GrpcTypeConverter.fromGrpcValue(tag.getValue());
        if (value != null)
          // The proto types tags as map<string, GrpcValue>, so nothing at the wire boundary stops a bytes,
          // list or map value arriving here. Refuse it - String.valueOf(byte[]) is an object identity, stored
          // as a different meaningless tag on every run. IllegalArgumentException, which GrpcErrorMapper turns
          // into INVALID_ARGUMENT, matching how this class refuses an unrecognized precision or aggregation.
          tags.put(tag.getKey(),
              String.valueOf(TimeSeriesGateway.requireStorableTagValue(tag.getKey(), value)));
      }

      final Map<String, Object> fields = new LinkedHashMap<>();
      for (final Map.Entry<String, GrpcValue> field : point.getFieldsMap().entrySet()) {
        final Object value = GrpcTypeConverter.fromGrpcValue(field.getValue());
        if (value != null)
          // An absent field is how a sample says "no measurement for this column", the same thing line
          // protocol says by simply not carrying the field.
          fields.put(field.getKey(), value);
      }

      final long timestampMs;
      try {
        timestampMs = unit.toMillis(point.getTimestamp());
      } catch (final ArithmeticException e) {
        throw Status.INVALID_ARGUMENT.withDescription(
                "Timestamp " + point.getTimestamp() + " overflows when converted to milliseconds at the requested precision")
            .asRuntimeException();
      }

      samples.add(new Sample(measurement, tags, fields, timestampMs));
    }

    return samples;
  }

  /**
   * Turns a failed {@link TimeSeriesGateway#resolveForRead} into the status the client receives. The three
   * cases keep the codes a client can act on apart: a name that is not in the schema is NOT_FOUND, a name that
   * is in it but is the wrong kind of type is the caller's mistake (INVALID_ARGUMENT), and a type whose
   * storage failed to load is a server-side state the same request would succeed against once repaired
   * (FAILED_PRECONDITION).
   */
  static RuntimeException resolutionFailure(final String typeName, final TypeResolution resolved) {
    return switch (resolved.failure()) {
      case NOT_FOUND -> Status.NOT_FOUND
          .withDescription("Type '" + typeName + "' does not exist").asRuntimeException();
      case NOT_TIME_SERIES -> Status.INVALID_ARGUMENT
          .withDescription("Type '" + typeName + "' is not a TimeSeries type").asRuntimeException();
      case ENGINE_UNAVAILABLE -> Status.FAILED_PRECONDITION
          .withDescription("TimeSeries type '" + typeName + "' has no storage engine available: "
              + resolved.unavailableReason()).asRuntimeException();
    };
  }

  /**
   * Reads a {@link TimeSeriesTagFilter} as the plain map {@link TimeSeriesGateway#buildTagFilter} takes.
   * Returns an empty map when no filter was sent, which selects everything.
   */
  static Map<String, Object> toTagMap(final TimeSeriesTagFilter filter) {
    if (filter == null || filter.getEqualsCount() == 0)
      return Map.of();

    final Map<String, Object> tags = new LinkedHashMap<>();
    for (final Map.Entry<String, GrpcValue> tag : filter.getEqualsMap().entrySet())
      tags.put(tag.getKey(), GrpcTypeConverter.fromGrpcValue(tag.getValue()));
    return tags;
  }

  /**
   * Maps the proto aggregation enum to the engine's. The proto zero value is UNSPECIFIED and is refused rather
   * than defaulted, so a client that forgot the field is told which field it forgot instead of silently
   * receiving sums.
   */
  static AggregationType toAggregationType(final TimeSeriesAggregationType type) {
    return switch (type) {
      case TS_AGG_SUM -> AggregationType.SUM;
      case TS_AGG_AVG -> AggregationType.AVG;
      case TS_AGG_MIN -> AggregationType.MIN;
      case TS_AGG_MAX -> AggregationType.MAX;
      case TS_AGG_COUNT -> AggregationType.COUNT;
      case TS_AGG_UNSPECIFIED, UNRECOGNIZED -> throw Status.INVALID_ARGUMENT
          .withDescription("TimeSeriesAggregationRequest.type is required and must be one of SUM, AVG, MIN, MAX, COUNT")
          .asRuntimeException();
    };
  }

  /**
   * Encodes one sample or aggregate value. A value that stands for "no measurement" - a non-finite double or
   * float, which is what an absent MIN/MAX answers - is encoded with no kind set, so it decodes to null. It is
   * deliberately NOT sent as {@code double_value} NaN, which a client would read as a number, and it matches
   * what the HTTP endpoints put in the JSON (issue #7043).
   */
  static GrpcValue toSampleValue(final Object value) {
    if (value == null || TimeSeriesGateway.isAbsentSampleValue(value))
      return GrpcValue.newBuilder().build();
    return GrpcTypeConverter.toGrpcValue(value);
  }

  /** Encodes a whole row of sample values. */
  static TimeSeriesRow toRow(final Object[] values) {
    final TimeSeriesRow.Builder row = TimeSeriesRow.newBuilder();
    for (final Object value : values)
      row.addValues(toSampleValue(value));
    return row.build();
  }
}
