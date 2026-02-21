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
 *
 * @param afterMs       age threshold in milliseconds (must be > 0)
 * @param granularityMs target resolution in milliseconds (must be > 0)
 */
public record DownsamplingTier(long afterMs, long granularityMs) {

  public DownsamplingTier {
    if (afterMs <= 0)
      throw new IllegalArgumentException("afterMs must be > 0, got " + afterMs);
    if (granularityMs <= 0)
      throw new IllegalArgumentException("granularityMs must be > 0, got " + granularityMs);
  }
}
