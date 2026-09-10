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
package com.arcadedb.remote.timeseries;

import java.util.ArrayList;
import java.util.List;

/**
 * What a time-series write did (issue #7305).
 * <p>
 * A write is <b>not</b> atomic on either protocol: each measurement's batch commits its own shard transaction
 * as it is appended, so {@code dropped &gt; 0} is a partial-write signal and not a rollback - the samples
 * counted in {@link #written()} are already durable. Every point is either written or named in exactly one of
 * the three type lists, so {@code written + dropped == received}.
 *
 * @param received           points handed to the server
 * @param written            samples appended
 * @param dropped            samples discarded
 * @param unknownTypes       measurements with no such type: create it first with CREATE TIMESERIES TYPE
 * @param nonTimeSeriesTypes measurements naming a type that is not a TIMESERIES type
 * @param unavailableTypes   measurements naming a TIMESERIES type whose storage engine failed to load
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record TimeSeriesWriteSummary(long received, long written, long dropped, List<String> unknownTypes,
                                     List<String> nonTimeSeriesTypes, List<String> unavailableTypes) {

  public TimeSeriesWriteSummary {
    unknownTypes = unknownTypes == null ? List.of() : List.copyOf(unknownTypes);
    nonTimeSeriesTypes = nonTimeSeriesTypes == null ? List.of() : List.copyOf(nonTimeSeriesTypes);
    unavailableTypes = unavailableTypes == null ? List.of() : List.copyOf(unavailableTypes);
  }

  /** Whether every point handed in was appended. */
  public boolean isComplete() {
    return dropped == 0;
  }

  /**
   * Adds another summary to this one, for a client that split one logical write across several requests. The
   * type lists are unioned in first-occurrence order, and a type named by more than one chunk appears once.
   */
  public TimeSeriesWriteSummary plus(final TimeSeriesWriteSummary other) {
    return new TimeSeriesWriteSummary(received + other.received, written + other.written, dropped + other.dropped,
        union(unknownTypes, other.unknownTypes), union(nonTimeSeriesTypes, other.nonTimeSeriesTypes),
        union(unavailableTypes, other.unavailableTypes));
  }

  private static List<String> union(final List<String> a, final List<String> b) {
    if (b.isEmpty())
      return a;
    final List<String> merged = new ArrayList<>(a);
    for (final String value : b)
      if (!merged.contains(value))
        merged.add(value);
    return merged;
  }

  /** The empty summary, the identity of {@link #plus(TimeSeriesWriteSummary)}. */
  public static TimeSeriesWriteSummary empty() {
    return new TimeSeriesWriteSummary(0, 0, 0, List.of(), List.of(), List.of());
  }
}
