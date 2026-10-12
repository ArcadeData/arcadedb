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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for aggregation functions.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractAggFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "agg." + getSimpleName();
  }

  protected abstract String getSimpleName();

  /**
   * Converts an input to a list of numbers: the numeric elements of a list argument, or the input itself when it is a single
   * number. What a list argument is comes from {@link CypherFunctionHelper#asListOrNull}, the one answer the {@code coll.*} and
   * Cypher list functions share: every Java array is one, {@code float[]} and {@code short[]} included, which is how a
   * {@code ARRAY_OF_FLOATS}/{@code ARRAY_OF_SHORTS} property and every {@code vector.*} function hand their values over (#9777).
   */
  protected List<Double> toDoubleList(final Object input) {
    if (input == null)
      return new ArrayList<>();

    final List<Object> list = CypherFunctionHelper.asListOrNull(input);
    if (list == null) {
      final List<Double> result = new ArrayList<>(1);
      if (input instanceof Number number)
        result.add(number.doubleValue());
      return result;
    }

    final List<Double> result = new ArrayList<>(list.size());
    for (final Object item : list)
      if (item instanceof Number number)
        result.add(number.doubleValue());
    return result;
  }

  /**
   * Returns {value, items} for the items paired with the extreme (minimum or maximum) of the parallel value list.
   * The two raw lists are walked together and a position whose value is not a number (null included) is skipped as a
   * pair, so a missing measurement never shifts the pairing. The length guard applies to the RAW lists: a length
   * mismatch is a real mis-pairing and yields {value: null, items: []}, as does a list with no numeric value.
   */
  protected Map<String, Object> extremeItems(final Object valuesArg, final Object itemsArg, final boolean max) {
    final List<Object> values = toObjectList(valuesArg);
    final List<Object> items = toObjectList(itemsArg);

    final List<Object> extremeItems = new ArrayList<>();
    Double extreme = null;

    if (!values.isEmpty() && values.size() == items.size()) {
      for (int i = 0; i < values.size(); i++) {
        if (!(values.get(i) instanceof Number number))
          continue;

        final double value = number.doubleValue();
        if (Double.isNaN(value))
          continue; // never less, greater or equal to anything: taking it as the extreme would pin the result to it

        if (extreme == null || (max ? value > extreme : value < extreme)) {
          extreme = value;
          extremeItems.clear();
          extremeItems.add(items.get(i));
        } else if (value == extreme)
          extremeItems.add(items.get(i));
      }
    }

    final Map<String, Object> result = new HashMap<>();
    result.put("value", extreme);
    result.put("items", extremeItems);
    return result;
  }

  /**
   * Converts an input to a new list of objects: the elements of a list argument (see {@link #toDoubleList} for what one is), or
   * a one-element list holding the input when it is anything else. A new list, because a caller may sort it or hand out a view
   * of it, and neither must reach the caller's own collection.
   */
  protected List<Object> toObjectList(final Object input) {
    if (input == null)
      return new ArrayList<>();

    final List<Object> list = CypherFunctionHelper.asListOrNull(input);
    if (list == null) {
      final List<Object> result = new ArrayList<>(1);
      result.add(input);
      return result;
    }
    return new ArrayList<>(list);
  }
}
