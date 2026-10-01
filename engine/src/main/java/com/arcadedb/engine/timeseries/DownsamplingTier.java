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
package com.arcadedb.engine.timeseries;

/**
 * Defines a downsampling tier: data older than {@code afterMs} gets downsampled
 * to {@code granularityMs} resolution (averaging numeric fields per time bucket).
 * <p>
 * The buckets are multiples of the granularity counted from the Unix epoch, shifted by {@code offsetMs} (issue #8798,
 * see {@link TimeBucketGrid}): zero keeps the epoch-aligned grid, and {@code -8 hours} makes daily buckets start at
 * local midnight in UTC+8. It should be the same offset the queries over the type bucket with, so a downsampled
 * block does not straddle two of their buckets. Blocks already downsampled to this granularity are not re-bucketed, so
 * changing the offset of an existing tier only affects data downsampled afterwards.
 *
 * @param afterMs       age threshold in milliseconds (must be > 0)
 * @param granularityMs target resolution in milliseconds (must be > 0)
 * @param offsetMs      shift of the bucket grid from the epoch, in milliseconds; may be negative, zero for the default grid
 */
public record DownsamplingTier(long afterMs, long granularityMs, long offsetMs) {

  public DownsamplingTier(final long afterMs, final long granularityMs) {
    this(afterMs, granularityMs, 0L);
  }

  public DownsamplingTier {
    if (afterMs <= 0)
      throw new IllegalArgumentException("afterMs must be > 0, got " + afterMs);
    if (granularityMs <= 0)
      throw new IllegalArgumentException("granularityMs must be > 0, got " + granularityMs);
  }
}
