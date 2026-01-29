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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * agg.minItems(values, items) - Return all items corresponding to the minimum value.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class AggMinItems extends AbstractAggFunction {
  @Override
  protected String getSimpleName() {
    return "minItems";
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
    return "Return all items corresponding to the minimum value";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null || args[1] == null)
      return null;

    final List<Double> values = toDoubleList(args[0]);
    final List<Object> items = toObjectList(args[1]);

    if (values.isEmpty() || items.isEmpty() || values.size() != items.size()) {
      final Map<String, Object> result = new HashMap<>();
      result.put("value", null);
      result.put("items", new ArrayList<>());
      return result;
    }

    double minValue = Double.MAX_VALUE;
    final List<Object> minItems = new ArrayList<>();

    for (int i = 0; i < values.size(); i++) {
      final double value = values.get(i);
      if (value < minValue) {
        minValue = value;
        minItems.clear();
        minItems.add(items.get(i));
      } else if (value == minValue) {
        minItems.add(items.get(i));
      }
    }

    final Map<String, Object> result = new HashMap<>();
    result.put("value", minValue);
    result.put("items", minItems);
    return result;
  }
}
