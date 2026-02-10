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
package com.arcadedb.query.opencypher.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Map;

/**
 * point.withinBBox(point, lowerLeft, upperRight) - checks if a point is within a bounding box.
 * All 3 arguments are maps with x/y or longitude/latitude.
 */
public class PointWithinBBoxFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "point.withinBBox";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 3)
      throw new CommandExecutionException("point.withinBBox() requires exactly 3 arguments: point, lowerLeft, upperRight");
    if (args[0] == null || args[1] == null || args[2] == null)
      return null;
    final double[] point = extractCoordinates(args[0]);
    final double[] lowerLeft = extractCoordinates(args[1]);
    final double[] upperRight = extractCoordinates(args[2]);
    return lowerLeft[0] <= point[0] && point[0] <= upperRight[0]
        && lowerLeft[1] <= point[1] && point[1] <= upperRight[1];
  }

  private double[] extractCoordinates(final Object value) {
    if (value instanceof Map<?, ?> map) {
      // Try x/y first, then longitude/latitude
      if (map.containsKey("x") && map.containsKey("y"))
        return new double[] { ((Number) map.get("x")).doubleValue(), ((Number) map.get("y")).doubleValue() };
      if (map.containsKey("longitude") && map.containsKey("latitude"))
        return new double[] { ((Number) map.get("longitude")).doubleValue(), ((Number) map.get("latitude")).doubleValue() };
      throw new CommandExecutionException("Point must have x/y or longitude/latitude properties");
    }
    throw new CommandExecutionException("point.withinBBox() arguments must be point values (maps with x/y or longitude/latitude)");
  }
}
