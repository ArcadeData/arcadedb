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
package com.arcadedb.function.agg;

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Cypher avg() aggregation function.
 * Always returns a Double result matching Neo4j/OpenCypher semantics,
 * unlike the SQL avg() which preserves the input numeric type.
 * Skips nulls. A non-numeric, non-null input is a client-facing type error rather than being
 * silently ignored - see issue #5799, the same class of defect as the numeric math family fixed
 * in issue #5484.
 * <p>
 * The average of durations is a duration: their component-wise sum divided by their count, exactly, as
 * {@code duration / n} divides (issue #10198). Numbers and durations do not mix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherAvgFunction implements StatelessFunction {
  private static final String         DOMAIN      = "an INTEGER, a FLOAT or a DURATION";
  private              double         sum         = 0.0;
  private              long           count       = 0;
  private              CypherDuration durationSum;

  @Override
  public String getName() {
    return "avg";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final Object value = args[0];
    if (value instanceof Number number) {
      if (durationSum != null)
        throw mixedInput();
      sum += number.doubleValue();
      count++;
    } else if (value instanceof CypherDuration duration) {
      if (durationSum == null && count > 0)
        throw mixedInput();
      durationSum = durationSum == null ? duration : durationSum.add(duration);
      count++;
    } else if (value != null)
      throw CypherFunctionHelper.typeMismatch("avg", DOMAIN, value);
    return null;
  }

  private static IllegalArgumentException mixedInput() {
    return new IllegalArgumentException("avg() cannot mix numbers and durations");
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public boolean canMergePartials() {
    return true;
  }

  @Override
  public void mergePartial(final StatelessFunction other) {
    final CypherAvgFunction that = (CypherAvgFunction) other;
    if (that.count == 0)
      return;
    if (count > 0 && (durationSum == null) != (that.durationSum == null))
      throw mixedInput();
    sum += that.sum;
    count += that.count;
    if (that.durationSum != null)
      durationSum = durationSum == null ? that.durationSum : durationSum.add(that.durationSum);
  }

  @Override
  public Object getAggregatedResult() {
    if (count == 0)
      return null;
    if (durationSum != null)
      return durationSum.divide(count);
    return sum / count;
  }
}
