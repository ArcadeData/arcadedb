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
import com.arcadedb.query.QueryEngineManager;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Server-side {@link QueryMetricsRecorder} that emits the always-on {@code arcadedb.query.duration}
 * RED timer tagged by originating protocol, database, language and type. Registered when server
 * metrics are enabled; otherwise the engine keeps the no-op recorder and pays no timing cost.
 */
public final class MicrometerQueryMetricsRecorder implements QueryMetricsRecorder {
  // Constant language tag used for any value that does not name a query engine registered on this JVM. The
  // language is whatever the caller asked to run - db.query("<language>", ...), the {lang} path segment of
  // the HTTP API, the Postgres portal language - and the timer is recorded whether or not the query
  // succeeded, so echoing it verbatim registered one permanent percentile-histogram Timer per invented name:
  // the unbounded-tag leak of #5025/#6805, on this meter (issue #7122).
  private static final String UNKNOWN_LANGUAGE_TAG = "unknown";
  // Constant db tag substituted once the timer cache is full. With protocol, type and language all bounded
  // above, the database name is the one part of the tuple that can still grow (create/drop churn), so it is
  // what collapses when the ceiling is reached.
  private static final String OVERFLOW_DB_TAG      = "other";
  // Maximum number of distinct values allowed in the "language" tag of arcadedb.query.duration.
  // ArcadeDBServer.startMetrics() installs the matching MeterFilter from this constant, so the registry-side
  // bound and the collapse above stay one number. Comfortably above the number of engines ArcadeDB can
  // register (sql, sqlscript, java, cypher, opencypher, gremlin, mongo, graphql, redis plus whichever
  // GraalVM polyglot languages are on the classpath).
  public static final  int    MAX_LANGUAGE_TAG_VALUES  = 50;
  // The one value above and beyond a registered language name that the tag can carry: UNKNOWN_LANGUAGE_TAG.
  // MeterFilter.maximumAllowableTags counts EVERY distinct value, so the filter limit has to include it, or
  // a JVM that had used the whole budget on real languages would have its own collapse value denied -
  // exactly the meter that exists to keep cardinality down.
  public static final  int    RESERVED_LANGUAGE_TAG_VALUES = 1;
  // Maximum number of distinct DATABASE NAMES allowed in the "db" tag of arcadedb.query.duration, matching
  // the budget the HTTP RED timer gives the same tag. Far above any realistic per-server database count.
  public static final  int    MAX_DB_TAG_VALUES        = 1_000;
  // The one value above and beyond a database name that the tag can carry: OVERFLOW_DB_TAG.
  public static final  int    RESERVED_DB_TAG_VALUES   = 1;
  // Ceiling on the number of cached tuples: a MeterFilter can deny the meter but cannot stop computeIfAbsent
  // from retaining the key, so the cache needs a bound of its own. Sized as a multiple of MAX_DB_TAG_VALUES
  // so a deployment with the maximum admissible number of databases still gets per-database RED visibility
  // across ten protocol/language/type combinations each before anything collapses onto OVERFLOW_DB_TAG - the
  // collapse is a backstop against unbounded growth, not a routine operating mode. Note this is a soft
  // ceiling: the size test and the computeIfAbsent are not one atomic step, so concurrent misses right at the
  // boundary can overshoot it by a bounded handful of entries before the collapse engages.
  static final         int    MAX_QUERY_TIMERS         = MAX_DB_TAG_VALUES * 10;
  // Cache of resolved arcadedb.query.duration timers keyed by protocol|db|language|type. record() runs
  // for every query/command from every wire protocol; caching removes the per-call Timer.Builder/Tags/
  // Meter.Id allocation and registry lookup on the hot path. The key space is bounded because every tag is
  // low-cardinality: protocol and type are small enumerations of constants set by the wire listeners, the
  // language is validated against the registered engines below, and db is a database name - backed by the
  // MAX_QUERY_TIMERS ceiling for the churn case.
  private static final ConcurrentHashMap<String, Timer> QUERY_TIMERS = new ConcurrentHashMap<>();

  @Override
  public void record(final String protocol, final String database, final String language, final String type,
      final long durationNanos) {
    queryTimer(protocol, database, language, type).record(durationNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Drops every cached timer. A cached {@link Timer} is bound to the registries backing
   * {@code Metrics.globalRegistry} when it was built, so it must not outlive them: recording into a meter
   * whose backing registry is gone silently discards the sample. Called when the server dismantles the
   * metrics subsystem, so the next server generation rebuilds its timers from scratch.
   */
  public static void invalidateTimerCache() {
    QUERY_TIMERS.clear();
  }

  /** Number of tuples currently cached. Package-private for direct unit testing of the ceiling. */
  static int cachedTimerCount() {
    return QUERY_TIMERS.size();
  }

  /**
   * Resolves (and caches) the {@code arcadedb.query.duration} timer for the given tag tuple, bounding the
   * two halves of it that are not small enumerations.
   * <p>
   * The already-seen tuple is answered by the first lookup, so the hot path is exactly one concatenation and
   * one hash lookup, as before. Only a miss pays for the bounds, and it pays before anything is retained:
   * an unregistered {@code language} collapses onto {@link #UNKNOWN_LANGUAGE_TAG} and past
   * {@link #MAX_QUERY_TIMERS} entries the {@code db} half collapses onto {@link #OVERFLOW_DB_TAG}, so
   * neither an invented language nor database-name churn can grow the cache (issue #7122). Recurses at most
   * twice - once per collapse - because each collapsed value is itself in the bounded set.
   * <p>
   * Package-private for direct unit testing.
   */
  static Timer queryTimer(final String protocol, final String database, final String language, final String type) {
    final String key = protocol + '|' + database + '|' + language + '|' + type;
    final Timer cached = QUERY_TIMERS.get(key);
    if (cached != null)
      return cached;

    final String boundedLanguage = languageTag(language);
    if (!boundedLanguage.equals(language))
      return queryTimer(protocol, database, boundedLanguage, type);

    if (QUERY_TIMERS.size() >= MAX_QUERY_TIMERS && !OVERFLOW_DB_TAG.equals(database))
      return queryTimer(protocol, OVERFLOW_DB_TAG, language, type);

    return QUERY_TIMERS.computeIfAbsent(key,
        k -> Timer.builder("arcadedb.query.duration")
            .description("Query/command execution duration")
            .tag("protocol", protocol)
            .tag("db", database)
            .tag("language", language)
            .tag("type", type)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry));
  }

  /**
   * Bounded {@code language} tag: the value is echoed only when it names a query engine registered on this
   * JVM, and collapses to the constant {@link #UNKNOWN_LANGUAGE_TAG} otherwise. Normalised to lower case the
   * way {@code QueryEngineManager.getEngine()} normalises it, so the case permutations of a real language
   * name cannot multiply the tag space either (issue #7122). Package-private so
   * {@link MicrometerQueryTracer} bounds the same key the same way, and for direct unit testing.
   */
  static String languageTag(final String language) {
    if (language == null)
      return UNKNOWN_LANGUAGE_TAG;
    final String normalized = language.toLowerCase(Locale.ENGLISH);
    return QueryEngineManager.getInstance().isLanguageRegistered(normalized) ? normalized : UNKNOWN_LANGUAGE_TAG;
  }
}
