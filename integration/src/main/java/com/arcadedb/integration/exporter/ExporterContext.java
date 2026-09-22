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
package com.arcadedb.integration.exporter;

import java.util.concurrent.atomic.AtomicLong;

public class ExporterContext {
  public final AtomicLong documents      = new AtomicLong();
  public final AtomicLong vertices       = new AtomicLong();
  public final AtomicLong edges          = new AtomicLong();
  /**
   * Records that threw while being serialized and were skipped rather than aborting the whole export
   * (issue #6471). Incremented by the format implementation's per-record catch blocks; {@link Exporter}
   * surfaces a non-zero count as a failing outcome once the export completes.
   */
  public final AtomicLong skippedRecords = new AtomicLong();
  /**
   * TIMESERIES samples written to the export (issue #7032). A TimeSeries type owns no record bucket, so its rows
   * are counted here rather than under {@link #documents}.
   */
  public final AtomicLong timeSeriesSamples = new AtomicLong();
  /**
   * Sealed TIMESERIES blocks a retention pass removed from under the export's own read (issue #8166).
   * <p>
   * Counted and reported rather than made a failure, unlike {@link #skippedRecords}. Retention dropping blocks
   * older than the policy while a long export runs is legitimate, and the samples in them are genuinely gone
   * rather than somewhere else - so the export is not incomplete against the database as it now stands, it is
   * merely not the snapshot an operator reading the summary may assume. What it must not be is SILENT, which is
   * what it was: the engine already counted the blocks, and the only reader of that count anywhere in the tree
   * was the PromQL/HTTP metrics surface, while {@code EXPORT DATABASE} passed no metrics at all.
   * <p>
   * The other way a block can leave the directory mid-read - a DOWNSAMPLE coarsening it - does not reach this
   * counter: the engine raises {@code TimeSeriesWalkCoarsenedException} for it, because those rows were replaced
   * rather than removed and no mixed-resolution answer is a consistent one.
   */
  public final AtomicLong vanishedTimeSeriesBlocks = new AtomicLong();
  public       long       startedOn;
  public       long       lastLapOn;
  public       long       lastDocuments;
  public       long       lastVertices;
  public       long       lastEdges;
}
