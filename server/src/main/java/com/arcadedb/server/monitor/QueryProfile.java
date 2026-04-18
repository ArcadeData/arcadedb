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
package com.arcadedb.server.monitor;

import com.arcadedb.serializer.json.JSONObject;

/**
 * Accumulates the per-phase wall-clock cost of a query execution across any
 * protocol (HTTP, Postgres wire, gRPC, ...): deserialization of the incoming
 * payload, engine execution (query plus materialization of all rows) and
 * serialization of the response.
 * <p>
 * Values are captured with {@link System#nanoTime()} and exposed as both raw
 * nanoseconds (lossless) and fractional milliseconds (human-readable).
 * Addition is cumulative, so the same phase can be timed across multiple
 * sections if needed.
 * <p>
 * A static {@link ThreadLocal} is provided so that inner layers (for example
 * {@link ProfilingResultSet}) can pick up the current per-request profile
 * without having to plumb it through method signatures. Callers are
 * responsible for pairing {@link #pushCurrent(QueryProfile)} with
 * {@link #popCurrent()} in a {@code try/finally}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryProfile {
  private static final ThreadLocal<QueryProfile> CURRENT = new ThreadLocal<>();

  private long deserializationNanos;
  private long engineNanos;
  private long serializationNanos;

  public void addDeserializationNanos(final long nanos) {
    deserializationNanos += nanos;
  }

  public void addEngineNanos(final long nanos) {
    engineNanos += nanos;
  }

  public void addSerializationNanos(final long nanos) {
    serializationNanos += nanos;
  }

  public long getDeserializationNanos() {
    return deserializationNanos;
  }

  public long getEngineNanos() {
    return engineNanos;
  }

  public long getSerializationNanos() {
    return serializationNanos;
  }

  public long getOverheadNanos() {
    return deserializationNanos + serializationNanos;
  }

  public long getTotalNanos() {
    return deserializationNanos + engineNanos + serializationNanos;
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("deserializationMs", nanosToMs(deserializationNanos));
    json.put("engineMs", nanosToMs(engineNanos));
    json.put("serializationMs", nanosToMs(serializationNanos));
    json.put("overheadMs", nanosToMs(getOverheadNanos()));
    json.put("totalMs", nanosToMs(getTotalNanos()));
    json.put("deserializationNanos", deserializationNanos);
    json.put("engineNanos", engineNanos);
    json.put("serializationNanos", serializationNanos);
    json.put("overheadNanos", getOverheadNanos());
    json.put("totalNanos", getTotalNanos());
    return json;
  }

  public static QueryProfile current() {
    return CURRENT.get();
  }

  public static void pushCurrent(final QueryProfile profile) {
    CURRENT.set(profile);
  }

  public static void popCurrent() {
    CURRENT.remove();
  }

  private static double nanosToMs(final long nanos) {
    return Math.round(nanos / 1_000.0) / 1_000.0;
  }
}
