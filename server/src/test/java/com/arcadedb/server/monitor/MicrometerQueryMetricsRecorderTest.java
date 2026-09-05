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

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for the per-tuple timer cache in {@link MicrometerQueryMetricsRecorder} (issue #5025):
 * {@code record()} runs on the query hot path for every wire protocol, so a repeated tag tuple must
 * reuse the same cached {@link Timer} instead of rebuilding the builder/tags/id each time.
 * <p>
 * Also covers the bounds that keep that cache - and the meters it registers - finite (issue #7122):
 * the {@code language} tag arrives from the caller, so an unregistered value must collapse onto a
 * constant before it is ever used as a cache key, and the cache as a whole must have a ceiling.
 */
class MicrometerQueryMetricsRecorderTest {
  private static final String OVERFLOW_PROTOCOL = "overflow-test";

  @AfterEach
  void dropTimersRegisteredByThisTest() {
    // The cache and Metrics.globalRegistry are both JVM-wide, so the overflow test below would otherwise
    // leave its fill behind for every later test sharing the fork.
    MicrometerQueryMetricsRecorder.invalidateTimerCache();
    final List<Meter> mine = new ArrayList<>(
        Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", OVERFLOW_PROTOCOL).meters());
    mine.forEach(Metrics.globalRegistry::remove);
  }

  @Test
  void queryTimerIsCachedPerTagTuple() {
    final Timer first = MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "sql", "query");
    final Timer second = MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "sql", "query");

    // Same tag tuple -> the exact same cached Timer instance (cache hit, no per-call allocation).
    assertThat(second).isSameAs(first);

    // A different tuple resolves to a distinct timer.
    final Timer command = MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "sql", "command");
    assertThat(command).isNotSameAs(first);
  }

  @Test
  void recordUsesTheCachedTimer() {
    final MicrometerQueryMetricsRecorder recorder = new MicrometerQueryMetricsRecorder();
    recorder.record("bolt", "graph", "cypher", "query", 1_000L);

    // The tuple recorded above must resolve to an already-cached timer (same instance returned).
    final Timer cached = MicrometerQueryMetricsRecorder.queryTimer("bolt", "graph", "cypher", "query");
    assertThat(MicrometerQueryMetricsRecorder.queryTimer("bolt", "graph", "cypher", "query")).isSameAs(cached);
  }

  @Test
  void anUnregisteredLanguageCollapsesToABoundedConstant() {
    // Issue #7122: the language reaches the recorder straight from the caller - db.query("<language>", ...),
    // the {lang} segment of the HTTP API - so echoing it verbatim registered one permanent
    // percentile-histogram Timer, plus one permanent cache entry, per invented name.
    final Timer bogus = MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "not-a-language-12345", "query");

    assertThat(bogus.getId().getTag("language")).isEqualTo("unknown");

    // Two different invented names must land on the ONE collapsed timer, not on two.
    assertThat(MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "not-a-language-67890", "query"))
        .isSameAs(bogus);
  }

  @Test
  void anUnregisteredLanguageIsNeverRetainedAsACacheKey() {
    // A MeterFilter can deny the meter but cannot stop computeIfAbsent from retaining the key, so the
    // collapse has to happen before anything is cached - otherwise the cache leaks even when the registry
    // does not (the third part of the #6805 fix).
    MicrometerQueryMetricsRecorder.invalidateTimerCache();

    for (int i = 0; i < 500; i++)
      MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "bogus" + i, "query");

    // Exactly one entry: protocol|graph|unknown|query.
    assertThat(MicrometerQueryMetricsRecorder.cachedTimerCount()).isEqualTo(1);
  }

  @Test
  void aRegisteredLanguageKeepsItsOwnTagValue() {
    // sql is always registered, so the tag must stay useful - the collapse is a backstop, not the norm.
    assertThat(MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "sql", "query").getId().getTag("language"))
        .isEqualTo("sql");
  }

  @Test
  void languageTagIsCaseNormalized() {
    // getEngine() lowercases before resolving, so "SQL" runs the sql engine. Without the same normalisation
    // here every case permutation of a real language would be its own tag value and its own cache entry.
    final Timer lower = MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "sql", "query");

    assertThat(MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "SQL", "query")).isSameAs(lower);
    assertThat(MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "SqL", "query")).isSameAs(lower);
  }

  @Test
  void aNullLanguageCollapsesInsteadOfBeingTaggedNull() {
    assertThat(MicrometerQueryMetricsRecorder.queryTimer("http", "graph", null, "query").getId().getTag("language"))
        .isEqualTo("unknown");
  }

  @Test
  void theDatabaseTagCollapsesOnceTheCacheCeilingIsReached() {
    // With the language bounded, database-name churn (create/drop in a loop) is the one dimension left that
    // can still grow the cache, so past the ceiling it collapses onto a constant.
    MicrometerQueryMetricsRecorder.invalidateTimerCache();

    for (int i = 0; i < MicrometerQueryMetricsRecorder.MAX_QUERY_TIMERS; i++)
      MicrometerQueryMetricsRecorder.queryTimer(OVERFLOW_PROTOCOL, "db" + i, "sql", "query");

    assertThat(MicrometerQueryMetricsRecorder.cachedTimerCount())
        .isGreaterThanOrEqualTo(MicrometerQueryMetricsRecorder.MAX_QUERY_TIMERS);

    final Timer overflowed = MicrometerQueryMetricsRecorder.queryTimer(OVERFLOW_PROTOCOL, "one-database-too-many",
        "sql", "query");
    assertThat(overflowed.getId().getTag("db")).isEqualTo("other");

    // The collapsed tuple space is finite, so it is itself cacheable: a second overflowing name reuses it.
    assertThat(MicrometerQueryMetricsRecorder.queryTimer(OVERFLOW_PROTOCOL, "another-one-too-many", "sql", "query"))
        .isSameAs(overflowed);
  }
}
