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

import com.arcadedb.query.opencypher.functions.CypherFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Abstract base class for aggregation functions.
 *
 * @author ArcadeDB Team
 */
public abstract class AbstractAggFunction implements CypherFunction {
  @Override
  public String getName() {
    return "agg." + getSimpleName();
  }

  protected abstract String getSimpleName();

  /**
   * Converts an input to a list of numbers.
   */
  protected List<Double> toDoubleList(final Object input) {
    final List<Double> result = new ArrayList<>();
    if (input == null)
      return result;

    if (input instanceof Collection) {
      for (final Object item : (Collection<?>) input) {
        if (item instanceof Number) {
          result.add(((Number) item).doubleValue());
        }
      }
    } else if (input.getClass().isArray()) {
      if (input instanceof double[]) {
        for (final double d : (double[]) input) {
          result.add(d);
        }
      } else if (input instanceof int[]) {
        for (final int i : (int[]) input) {
          result.add((double) i);
        }
      } else if (input instanceof long[]) {
        for (final long l : (long[]) input) {
          result.add((double) l);
        }
      } else if (input instanceof Object[]) {
        for (final Object item : (Object[]) input) {
          if (item instanceof Number) {
            result.add(((Number) item).doubleValue());
          }
        }
      }
    } else if (input instanceof Number) {
      result.add(((Number) input).doubleValue());
    }

    return result;
  }

  /**
   * Converts an input to a list of objects.
   */
  protected List<Object> toObjectList(final Object input) {
    final List<Object> result = new ArrayList<>();
    if (input == null)
      return result;

    if (input instanceof Collection) {
      result.addAll((Collection<?>) input);
    } else if (input.getClass().isArray()) {
      if (input instanceof Object[]) {
        for (final Object item : (Object[]) input) {
          result.add(item);
        }
      } else if (input instanceof int[]) {
        for (final int i : (int[]) input) {
          result.add(i);
        }
      } else if (input instanceof long[]) {
        for (final long l : (long[]) input) {
          result.add(l);
        }
      } else if (input instanceof double[]) {
        for (final double d : (double[]) input) {
          result.add(d);
        }
      }
    } else {
      result.add(input);
    }

    return result;
  }
}
