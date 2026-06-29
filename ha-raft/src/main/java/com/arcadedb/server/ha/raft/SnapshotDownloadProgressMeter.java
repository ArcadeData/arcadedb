/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

/**
 * Generates throttled progress lines for a snapshot download. The HTTP response carries no reliable
 * content length (the body is a streamed ZIP), so progress is reported as cumulative bytes downloaded
 * and instantaneous throughput rather than a percentage or ETA. Pure and clock-free: the caller passes
 * the running byte count and current time; {@link #lineIfDue} returns a message at most once per
 * configured interval, otherwise {@code null}. The first call establishes the baseline and never logs.
 */
public final class SnapshotDownloadProgressMeter {

  private static final double BYTES_PER_MB = 1024.0 * 1024.0;

  private final String databaseName;
  private final long   intervalMs;

  private boolean started      = false;
  private long    lastLogMs    = 0;
  private long    lastLogBytes = 0;

  public SnapshotDownloadProgressMeter(final String databaseName, final long intervalMs) {
    this.databaseName = databaseName;
    this.intervalMs = intervalMs;
  }

  public String lineIfDue(final long cumulativeBytes, final long nowMs) {
    if (!started) {
      started = true;
      lastLogMs = nowMs;
      lastLogBytes = cumulativeBytes;
      return null;
    }
    final long sinceMs = nowMs - lastLogMs;
    if (sinceMs < intervalMs)
      return null;

    final double mbSoFar = cumulativeBytes / BYTES_PER_MB;
    final double mbPerSec = (cumulativeBytes - lastLogBytes) / BYTES_PER_MB * 1000.0 / Math.max(1L, sinceMs);
    lastLogMs = nowMs;
    lastLogBytes = cumulativeBytes;
    return String.format("snapshot download db=%s: %.1f MB so far (%.1f MB/s)", databaseName, mbSoFar, mbPerSec);
  }
}
