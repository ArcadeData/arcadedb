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
package com.arcadedb.bench.runner;

import com.arcadedb.bench.workload.Scale;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * A named group of benchmark cases.
 */
public interface Workload {
  String name();

  /** Every case of the workload, at any scale. */
  List<String> cases();

  /**
   * The cases a run at this scale measures. A case that cannot measure anything at a scale (shorter than its own jitter) is left
   * out there rather than recorded as noise.
   */
  default List<String> cases(final Scale scale) {
    return cases();
  }

  /**
   * The measured repetitions of each case of this workload, overriding the run's; empty to take the run's. A workload whose
   * single repetition is long (a whole ramp) declares fewer.
   */
  default OptionalInt reps() {
    return OptionalInt.empty();
  }

  /** The discarded warm-ups of each case of this workload, overriding the run's; empty to take the run's. */
  default OptionalInt warmup() {
    return OptionalInt.empty();
  }

  void prepare(WorkloadContext ctx, String caseName) throws Exception;

  /**
   * Runs one repetition and returns one value per metric.
   */
  Measurement measure(WorkloadContext ctx, String caseName) throws Exception;

  /**
   * Server settings this workload needs (setting name to value), for example the plugins of a wire protocol. A key declared here
   * replaces the default of the same name, it does not extend it: declaring {@code arcadedb.server.plugins} drops the plugins the
   * default list holds unless they are named again. The server of each workload is started with them, and so is its replacement after a death.
   */
  default Map<String, String> serverSettings() {
    return Map.of();
  }

  default void teardown(WorkloadContext ctx) {
  }
}
