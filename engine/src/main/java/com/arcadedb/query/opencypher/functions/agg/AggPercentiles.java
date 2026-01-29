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
package com.arcadedb.query.opencypher.functions.agg;

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * agg.percentiles(list, percentiles) - Return the percentile values of a list of numbers.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class AggPercentiles extends AbstractAggFunction {
  @Override
  protected String getSimpleName() {
    return "percentiles";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Return the percentile values of a list of numbers";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null || args[1] == null)
      return null;

    final List<Double> values = toDoubleList(args[0]);
    final List<Double> percentiles = toDoubleList(args[1]);

    if (values.isEmpty())
      return Collections.emptyList();

    Collections.sort(values);

    final List<Double> result = new ArrayList<>();
    for (final Double percentile : percentiles) {
      if (percentile < 0 || percentile > 1) {
        throw new IllegalArgumentException("Percentile must be between 0 and 1: " + percentile);
      }

      final double index = percentile * (values.size() - 1);
      final int lower = (int) Math.floor(index);
      final int upper = (int) Math.ceil(index);

      if (lower == upper) {
        result.add(values.get(lower));
      } else {
        final double fraction = index - lower;
        result.add(values.get(lower) * (1 - fraction) + values.get(upper) * fraction);
      }
    }

    return result;
  }
}
