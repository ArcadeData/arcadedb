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
package com.arcadedb.function.coll;

import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.LongRangeList;

import java.util.List;

/**
 * coll.sum(list) - Returns the sum of a list of numbers.
 * <p>
 * The sum of an arithmetic progression is {@code n * (first + last) / 2}, so a range is answered without walking it
 * (issue #6403).
 */
public class CollSum extends AbstractCollFunction {
  @Override
  protected String getSimpleName() {
    return "sum";
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
  public String getDescription() {
    return "Returns the sum of a list of numbers";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final List<Object> list = asList(args[0]);
    if (list == null)
      return null;

    final LongRangeList range = asRange(list);
    if (range != null)
      return rangeSum(range);

    double sum = 0.0;
    for (final Object item : list) {
      final Number number = CypherFunctionHelper.requireNumberArgument(item, getName());
      if (number != null)
        sum += number.doubleValue();
    }
    return sum;
  }

  /**
   * The closed form {@code n * (first + last) / 2}, evaluated in {@code double} because that is the type this
   * function answers in anyway. Both endpoints are widened before they are added: their long sum can overflow even
   * though each is a long, and a wrapped sum would answer a plausible wrong number rather than fail.
   */
  static double rangeSum(final LongRangeList range) {
    final int size = range.size();
    if (size == 0)
      return 0.0;
    return ((double) range.get(0) + (double) range.get(size - 1)) * size / 2.0;
  }
}
