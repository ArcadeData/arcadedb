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

import java.time.Instant;

/**
 * One measurement of a line. {@code ref} is the measured version: the commit of a nightly run, the tag of a release run. {@code t} is
 * where the code is (commit date, else the run's start), {@code runAt} when it was measured. A {@link LineKey#STATUS} point has a NaN
 * median and no values. {@code hostNotQuiet} is set when the host slept or was suspended during the case: such a point is not valid,
 * and the gate neither judges it nor takes it as a baseline.
 */
public record Point(String trigger, String ref, String commit, String tag, Instant t, Instant runAt, String runId, String benchCommit,
                    String status, double median, double[] values, String unit, String better, boolean hostNotQuiet) {

  /** A point of a case during which the host was quiet. */
  public Point(final String trigger, final String ref, final String commit, final String tag, final Instant t, final Instant runAt,
      final String runId, final String benchCommit, final String status, final double median, final double[] values, final String unit,
      final String better) {
    this(trigger, ref, commit, tag, t, runAt, runId, benchCommit, status, median, values, unit, better, false);
  }

  public boolean valid() {
    return "ok".equals(status) && !hostNotQuiet;
  }
}
