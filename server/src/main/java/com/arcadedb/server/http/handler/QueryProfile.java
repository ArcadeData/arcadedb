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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;

/**
 * Accumulates the per-phase wall-clock cost of an HTTP command execution:
 * deserialization of the incoming payload, engine execution (query plus
 * materialization of all rows) and serialization of the response.
 * <p>
 * Values are captured with {@link System#nanoTime()} and exposed as both raw
 * nanoseconds (lossless) and fractional milliseconds (human-readable).
 * Addition is cumulative, so the same phase can be timed across multiple
 * sections if needed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryProfile {
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

  private static double nanosToMs(final long nanos) {
    return Math.round(nanos / 1_000.0) / 1_000.0;
  }
}
