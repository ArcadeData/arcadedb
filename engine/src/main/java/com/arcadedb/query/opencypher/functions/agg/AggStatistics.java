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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * agg.statistics(list) - Return full statistics for a list of numbers.
 *
 * @author ArcadeDB Team
 */
public class AggStatistics extends AbstractAggFunction {
  @Override
  protected String getSimpleName() {
    return "statistics";
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
    return "Return full statistics (count, min, max, sum, mean, stdev) for a list of numbers";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final List<Double> values = toDoubleList(args[0]);

    final Map<String, Object> stats = new HashMap<>();

    if (values.isEmpty()) {
      stats.put("count", 0L);
      stats.put("min", null);
      stats.put("max", null);
      stats.put("sum", 0.0);
      stats.put("mean", null);
      stats.put("stdev", null);
      return stats;
    }

    final long count = values.size();
    double sum = 0.0;
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;

    for (final Double value : values) {
      sum += value;
      min = Math.min(min, value);
      max = Math.max(max, value);
    }

    final double mean = sum / count;

    double sumSquaredDiff = 0.0;
    for (final Double value : values) {
      sumSquaredDiff += Math.pow(value - mean, 2);
    }
    final double stdev = Math.sqrt(sumSquaredDiff / count);

    // Calculate median
    Collections.sort(values);
    final double median;
    if (count % 2 == 1) {
      median = values.get((int) (count / 2));
    } else {
      median = (values.get((int) (count / 2) - 1) + values.get((int) (count / 2))) / 2.0;
    }

    stats.put("count", count);
    stats.put("min", min);
    stats.put("max", max);
    stats.put("sum", sum);
    stats.put("mean", mean);
    stats.put("stdev", stdev);
    stats.put("median", median);

    return stats;
  }
}
