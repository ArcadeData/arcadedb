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
 * The metrics of one repetition: one value per metric, and the base64 latency histogram when the workload recorded one.
 */
public record Measurement(List<Metric> perRep, String histogram) {
  public Measurement(final List<Metric> perRep) {
    this(perRep, null);
  }

  public static Measurement of(final List<Metric> perRep, final LatencyRecorder recorder) {
    return new Measurement(perRep, recorder.encodedHistogram());
  }
}
