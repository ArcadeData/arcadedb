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
package com.arcadedb.utility;

/**
 * What a scan says about the read-ahead it was allowed (issue #9404): how many of its batches it read while the queries
 * running held most of the query heap budget, which shrinks the batch (down to one record) and so slows the scan. A
 * profiled query shows it, so a slow scan can be traced to a low budget.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface ScanPressureReporter {
  /** The batches this scan read with its read-ahead reduced by the query heap budget or the pool of read-ahead. */
  long getBudgetShrunkBatches();

  /**
   * Gives back to the JVM-wide pool of read-ahead what the scan holds, for a scan its caller stops before the end (a LIMIT, a failure):
   * the garbage collector would do it, but only when it gets to the scan. The scan can still be read; its next batch reserves again.
   */
  void releaseReadAhead();

  /** The text a profiled plan shows for a scan that read batches under pressure, or an empty string when none. */
  static String describe(final Object scan) {
    return scan instanceof ScanPressureReporter reporter ? describe(reporter.getBudgetShrunkBatches()) : "";
  }

  /** The same for a count a caller added up over several scans. */
  static String describe(final long shrunk) {
    if (shrunk > 0)
      return " [read-ahead reduced in " + shrunk + (shrunk == 1 ? " batch" : " batches") + " by memory pressure]";
    return "";
  }
}
