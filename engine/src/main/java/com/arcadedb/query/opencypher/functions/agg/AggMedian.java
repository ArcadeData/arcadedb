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
import java.util.List;

/**
 * agg.median(list) - Return the median value of a list of numbers.
 *
 * @author ArcadeDB Team
 */
public class AggMedian extends AbstractAggFunction {
  @Override
  protected String getSimpleName() {
    return "median";
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
    return "Return the median value of a list of numbers";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final List<Double> values = toDoubleList(args[0]);

    if (values.isEmpty())
      return null;

    Collections.sort(values);

    final int size = values.size();
    if (size % 2 == 1) {
      return values.get(size / 2);
    } else {
      return (values.get(size / 2 - 1) + values.get(size / 2)) / 2.0;
    }
  }
}
