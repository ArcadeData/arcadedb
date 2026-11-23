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

import com.arcadedb.bench.model.Metric;

import java.util.List;

/**
 * Thrown by a workload when a case must be recorded as FAILED although it measured something: the runner keeps the attached
 * metrics on the failed case.
 */
public class CaseFailedWithMetrics extends Exception {
  private final transient List<Metric> metrics;

  public CaseFailedWithMetrics(final String message, final List<Metric> metrics) {
    super(message);
    this.metrics = metrics;
  }

  public List<Metric> metrics() {
    return metrics;
  }
}
