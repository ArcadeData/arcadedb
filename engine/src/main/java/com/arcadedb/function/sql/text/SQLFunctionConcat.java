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
package com.arcadedb.function.sql.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;

public class SQLFunctionConcat extends SQLAggregatedFunction {
  public static final String        NAME                 = "concat";
  // WHAT A CHARACTER OF THE TEXT TAKES ON THE HEAP: THE ARRAY OF THE BUILDER HOLDS A BYTE PER LATIN-1 CHARACTER AND GROWS BY
  // DOUBLING, SO IT CAN BE TWICE AS LONG AS THE TEXT IN IT
  private static final int          HEAP_BYTES_PER_CHAR  = 2;
  private             StringBuilder sb;
  // THE VALUES APPENDED SO FAR, WHICH THE ELEMENT CAP COUNTS
  private             long          held;
  // THE LENGTH OF THE TEXT THAT WAS CHARGED TO THE HEAP BUDGET ALREADY
  private             int           chargedLength;

  public SQLFunctionConcat() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (sb == null) {
      sb = new StringBuilder();
    } else {
      if (params.length > 1)
        sb.append(params[1]);
    }
    sb.append(params[0]);
    ++held;
    return null;
  }

  /** The text is kept until the aggregation ends: the values appended are counted and the bytes of the text charged. */
  @Override
  public boolean holdsEveryValue() {
    return true;
  }

  @Override
  public long getHeldValues() {
    return held;
  }

  /**
   * Charges the text appended since the last call, delimiter included, rather than an estimate of the values it was given: what
   * a value adds depends on how it is written as text, and the size of the values varies from one row to the next.
   */
  @Override
  public long chargeHeld(final OperationHeapLimit limit, final long heldBefore, final Object value) {
    if (sb == null)
      return heldBefore;
    limit.check(held);
    final int length = sb.length();
    limit.charge((long) (length - chargedLength) * HEAP_BYTES_PER_CHAR);
    chargedLength = length;
    return held;
  }

  @Override
  public Object getResult() {
    return sb != null ? sb.toString() : null;
  }

  @Override
  public String getSyntax() {
    return "concat(<field>, [<delim>])";
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }
}
