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
package com.arcadedb.bench.gate;

import java.util.Comparator;

/**
 * One line of the performance history: a (machine, variant, scale, workload, case, metric) tuple. The metric {@link #STATUS} is the
 * line of the case's own status.
 */
public record LineKey(String machine, String variant, String scale, String workload, String caseName, String metric)
    implements Comparable<LineKey> {

  public static final String STATUS = "_status";

  private static final Comparator<LineKey> ORDER = Comparator.comparing(LineKey::machine).thenComparing(LineKey::variant)
      .thenComparing(LineKey::scale).thenComparing(LineKey::workload).thenComparing(LineKey::caseName).thenComparing(LineKey::metric);

  /** Stable key of the line, used in the issue markers. */
  public String id() {
    return machine + "/" + variant + "/" + scale + "/" + workload + "/" + caseName + "/" + metric;
  }

  @Override
  public int compareTo(final LineKey other) {
    return ORDER.compare(this, other);
  }
}
