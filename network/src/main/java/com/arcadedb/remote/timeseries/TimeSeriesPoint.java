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
package com.arcadedb.remote.timeseries;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One time-series sample to ingest, in the protocol-independent shape both remote clients take (issue #7305):
 * {@code RemoteDatabase} sends it as InfluxDB Line Protocol over HTTP, {@code RemoteGrpcDatabase} as a typed
 * {@code TimeSeriesPoint} message over gRPC.
 * <p>
 * The timestamp is always epoch <b>milliseconds</b> - the unit the engine stores - so the same point written
 * through either client lands on the same sample.
 *
 * @param type        the TIMESERIES type (the InfluxDB "measurement") this sample belongs to
 * @param timestampMs the sample timestamp, epoch milliseconds
 * @param tags        tag columns by name; a column the type does not declare is ignored by the server
 * @param fields      field columns by name; at least one is required
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record TimeSeriesPoint(String type, long timestampMs, Map<String, Object> tags, Map<String, Object> fields) {

  public TimeSeriesPoint {
    if (type == null || type.isBlank())
      throw new IllegalArgumentException("A time-series point needs a type");
    if (fields == null || fields.isEmpty())
      throw new IllegalArgumentException("A time-series point on type '" + type + "' needs at least one field");
    tags = tags == null ? Map.of() : tags;
  }

  /** A point with no tags. */
  public static TimeSeriesPoint of(final String type, final long timestampMs, final Map<String, Object> fields) {
    return new TimeSeriesPoint(type, timestampMs, Map.of(), fields);
  }

  /**
   * Builds a point from alternating key/value pairs, tags and fields already separated. Convenience for tests
   * and for call sites that would otherwise assemble two maps by hand.
   */
  public static TimeSeriesPoint of(final String type, final long timestampMs, final Map<String, Object> tags,
      final Object... fieldKeyValuePairs) {
    if (fieldKeyValuePairs.length % 2 != 0)
      throw new IllegalArgumentException("Field key/value pairs must come in pairs");
    // LinkedHashMap, not Map.of: the caller's order is the order the line-protocol body will carry, which keeps
    // a body diffable against what the caller wrote, and Map.of would also reject a null value with a less
    // helpful message than the writer's.
    final Map<String, Object> fields = new LinkedHashMap<>();
    for (int i = 0; i < fieldKeyValuePairs.length; i += 2)
      fields.put(String.valueOf(fieldKeyValuePairs[i]), fieldKeyValuePairs[i + 1]);
    return new TimeSeriesPoint(type, timestampMs, tags, fields);
  }
}
