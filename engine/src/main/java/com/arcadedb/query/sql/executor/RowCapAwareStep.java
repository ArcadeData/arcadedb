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
package com.arcadedb.query.sql.executor;

import com.arcadedb.query.ResultRowCapScope;

/**
 * A step of a statement's plan that holds rows, or reads a source that materializes them, and can stop at the rows the caller of the
 * statement reads (see {@link ResultRowCapScope}, issue #12016). The planner marks the step it proved an upper bound safe for
 * ({@code boundByRowCap(skip)}: nothing downstream drops a row, and the statement has no LIMIT of its own) and the cap reaches it
 * once the plan is built or taken from the cache, through {@link InternalExecutionPlan#applyRowCap(int)}. The plan itself does not
 * depend on the cap, so a cached plan is the one of the statement without it.
 * <p>
 * The cap is never read by a decision that picks the shape of the plan (a DISTINCT as a GROUP BY, the columnar projection of a GROUP
 * BY, an index walk for an ORDER BY): those take the statement's own LIMIT, and the cap is not one.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
interface RowCapAwareStep {
  /**
   * Hands the step the rows the caller reads at most, before the first row is pulled.
   *
   * @param rowCap the rows the caller reads after the SKIP of the statement, positive
   */
  void applyRowCap(int rowCap);

  /**
   * The rows a step marked for the cap reads at most: its SKIP plus the cap, or {@code -1} when the cap does not bound it.
   *
   * @param capSkip the SKIP the cap is counted after, {@code -1} when the planner did not mark the step
   * @param rowCap  the cap the caller set, {@code 0} for none
   */
  static int bound(final int capSkip, final int rowCap) {
    if (capSkip < 0 || rowCap <= 0)
      return -1;
    return (int) Math.min(Integer.MAX_VALUE, (long) capSkip + rowCap);
  }
}
