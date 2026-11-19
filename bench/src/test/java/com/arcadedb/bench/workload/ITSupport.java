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

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.BenchRunner;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.serializer.json.JSONObject;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers of the workload ITs: the server start and the metric lookup.
 */
final class ITSupport {
  static final String IMAGE = System.getProperty("arcadedb.bench.image", "arcadedata/arcadedb:latest");

  private ITSupport() {
  }

  static ServerUnderTest startServer() {
    return ServerUnderTest.start(IMAGE, "", 4, "2g", Map.of());
  }

  static List<CaseResult> run(final Workload w, final ServerUnderTest server, final JSONObject params) {
    return new BenchRunner(2, 1, Duration.ofMinutes(5)).run(w, new WorkloadContext(server, Scale.XS, 42L, params), n -> true);
  }

  static Metric metric(final CaseResult result, final String name) {
    return result.metrics().stream().filter(m -> m.name().equals(name)).findFirst().orElse(null);
  }
}
