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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.soak.SoakSampler;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A short real soak: 12 minutes of the mixed phase at XS, the first 2 minutes left out as warm-up and the 10 that remain cut into
 * heap floor windows of 90 s (six, with a margin), so each of the three slopes has the six samples it needs. It checks that the
 * soak runs to its end and publishes its three slopes; whether they stay under their limits is for a real 2 h run at M, a 12
 * minute run from a cold server at XS says little about a plateau.
 */
@Tag("slow")
class SoakWorkloadIT {
  private static final Duration WINDOW       = Duration.ofMinutes(12);
  private static final Duration FLOOR_WINDOW = Duration.ofSeconds(90);
  private static final Duration WARMUP       = Duration.ofMinutes(2);

  private static ServerUnderTest server;

  @BeforeAll
  static void start() {
    server = ITSupport.startServer();
  }

  @AfterAll
  static void stop() {
    if (server != null)
      server.close();
  }

  @Test
  void aShortSoakRunsToItsEndAndPublishesItsThreeSlopes() throws Exception {
    final SoakWorkload soak = new SoakWorkload(WINDOW, FLOOR_WINDOW, WARMUP);
    final WorkloadContext ctx = new WorkloadContext(server, Scale.XS, 42L, new JSONObject());
    soak.prepare(ctx, SoakWorkload.CASE);
    List<Metric> metrics;
    String failure = null;
    try {
      metrics = soak.measure(ctx, SoakWorkload.CASE).perRep();
    } catch (final CaseFailedWithMetrics e) {
      metrics = e.metrics();
      failure = e.getMessage();
    } finally {
      keepSeries(soak);
      soak.teardown(ctx);
    }

    // ran to its end: sampled the whole window, and nothing failed but a slope over its limit (a loop failure, a wrong count or
    // a series too short for a slope is named otherwise)
    final SoakSampler sampler = soak.lastSampler();
    final int floors = sampler.heapFloors().length;
    // at least the floor windows the window holds after its warm-up; the drain after the window may complete one more
    assertThat(floors).isGreaterThanOrEqualTo((int) (WINDOW.minus(WARMUP).toSeconds() / FLOOR_WINDOW.toSeconds()));
    // the samples cover the warm-up and every floor window that was produced
    assertThat(sampler.samples()).isGreaterThanOrEqualTo(
        (int) ((WARMUP.toSeconds() + floors * FLOOR_WINDOW.toSeconds()) / SoakSampler.PERIOD.toSeconds()));
    if (failure != null)
      for (final String part : failure.split("; "))
        assertThat(part).as(failure).contains("over its limit");
    for (final String name : new String[] { SoakWorkload.HEAP_METRIC, SoakWorkload.THREADS_METRIC, SoakWorkload.FILES_METRIC }) {
      final Metric slope = metrics.stream().filter(m -> m.name().equals(name)).findFirst().orElse(null);
      assertThat(slope).as(name).isNotNull();
      assertThat(slope.unit()).isEqualTo("per_hour");
      assertThat(Double.isFinite(slope.median())).as(name).isTrue();
    }
    assertThat(metrics).extracting(Metric::name).contains("ingest_p99_ms", "vector_churn_p99_ms", "max_commit_gap_ms");
  }

  /** The series the slopes were drawn from, kept in the module's target/ for whoever reads the run: a few kB. */
  private static void keepSeries(final SoakWorkload soak) throws IOException {
    final SoakSampler sampler = soak.lastSampler();
    if (sampler == null)
      return;
    final JSONObject series = new JSONObject().put("samples", sampler.samples()).put("expectedDocs", soak.expectedDocs())
        .put("heapFloors", pairs(sampler.heapFloors())).put("threads", pairs(sampler.threads()))
        .put("files", pairs(sampler.files())).put("clientHeapUsedBytes", usedHeap());
    final Path file = Path.of("target", "soak-it-series.json");
    Files.writeString(file, series.toString(2));
  }

  private static JSONArray pairs(final double[][] pairs) {
    final JSONArray array = new JSONArray();
    for (final double[] pair : pairs)
      array.put(new JSONArray().put(pair[0]).put(pair[1]));
    return array;
  }

  private static long usedHeap() {
    final Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }
}
