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

import com.arcadedb.database.QueryMetricsRecorder;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Server-side {@link QueryMetricsRecorder} that emits the always-on {@code arcadedb.query.duration}
 * RED timer tagged by originating protocol, database, language and type. Registered when server
 * metrics are enabled; otherwise the engine keeps the no-op recorder and pays no timing cost.
 */
public final class MicrometerQueryMetricsRecorder implements QueryMetricsRecorder {
  // Cache of resolved arcadedb.query.duration timers keyed by protocol|db|language|type. record() runs
  // for every query/command from every wire protocol; caching removes the per-call Timer.Builder/Tags/
  // Meter.Id allocation and registry lookup on the hot path. The key space is bounded because every tag
  // is low-cardinality (finite protocols/languages/types and the finite set of database names).
  private static final ConcurrentHashMap<String, Timer> QUERY_TIMERS = new ConcurrentHashMap<>();

  @Override
  public void record(final String protocol, final String database, final String language, final String type,
      final long durationNanos) {
    queryTimer(protocol, database, language, type).record(durationNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Resolves (and caches) the {@code arcadedb.query.duration} timer for the given bounded tag tuple.
   * Package-private for direct unit testing.
   */
  static Timer queryTimer(final String protocol, final String database, final String language, final String type) {
    return QUERY_TIMERS.computeIfAbsent(protocol + '|' + database + '|' + language + '|' + type,
        k -> Timer.builder("arcadedb.query.duration")
            .description("Query/command execution duration")
            .tag("protocol", protocol)
            .tag("db", database)
            .tag("language", language)
            .tag("type", type)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry));
  }
}
