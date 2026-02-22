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
package com.arcadedb.engine.timeseries.promql;

import java.util.List;
import java.util.Map;

/**
 * Sealed interface representing PromQL evaluation results.
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public sealed interface PromQLResult {

  record ScalarResult(double value, long timestampMs) implements PromQLResult {
  }

  record VectorSample(Map<String, String> labels, double value, long timestampMs) {
  }

  record InstantVector(List<VectorSample> samples) implements PromQLResult {
  }

  record RangeSeries(Map<String, String> labels, List<double[]> values) {
  }

  record RangeVector(List<RangeSeries> series) implements PromQLResult {
  }

  record MatrixSeries(Map<String, String> labels, List<double[]> values) {
  }

  record MatrixResult(List<MatrixSeries> series) implements PromQLResult {
  }
}
