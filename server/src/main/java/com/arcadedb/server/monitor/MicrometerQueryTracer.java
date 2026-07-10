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

import com.arcadedb.database.QueryTracer;
import com.arcadedb.log.LogManager;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import java.util.logging.Level;

/**
 * Server-side {@link QueryTracer} that opens a span-only Micrometer {@link Observation}
 * ({@code arcadedb.query}) on the shared {@link ObservationRegistry} for every query/command at the
 * engine boundary - so all wire protocols emit spans from one instrumentation point. Low-cardinality
 * tags carry {@code protocol/db/language/type}; the query text is a high-cardinality span attribute
 * only (never a metric tag). When the optional tracing plugin has not attached a tracer the registry
 * is a no-op, so this opens nothing and allocates nothing. Latency metrics stay on the dedicated
 * {@code arcadedb.query.duration} timer ({@link MicrometerQueryMetricsRecorder}).
 */
public final class MicrometerQueryTracer implements QueryTracer {
  private final ObservationRegistry observationRegistry;

  public MicrometerQueryTracer(final ObservationRegistry observationRegistry) {
    this.observationRegistry = observationRegistry;
  }

  @Override
  public Span beginQuery(final String protocol, final String database, final String language, final String type,
      final String query) {
    // No tracer attached -> no handlers -> skip entirely (zero allocation on the hot path).
    if (observationRegistry.isNoop())
      return Span.NO_OP;

    final Observation observation = Observation.createNotStarted("arcadedb.query", observationRegistry)
        .lowCardinalityKeyValue("protocol", protocol != null ? protocol : "internal")
        .lowCardinalityKeyValue("db", database != null ? database : "none")
        .lowCardinalityKeyValue("language", language != null ? language : "unknown")
        .lowCardinalityKeyValue("type", type != null ? type : "query")
        .highCardinalityKeyValue("db.statement", query != null ? query : "");
    observation.start();
    final Observation.Scope scope = observation.openScope();

    return () -> {
      // A failure finalizing the optional span must never disturb the query path.
      try {
        scope.close();
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.WARNING, "Error closing query tracing scope", t);
      }
      try {
        observation.stop();
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.WARNING, "Error stopping query tracing observation", t);
      }
    };
  }
}
