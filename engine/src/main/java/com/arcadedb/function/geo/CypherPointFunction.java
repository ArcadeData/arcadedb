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
package com.arcadedb.function.geo;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cypher {@code point(map)} function.
 *
 * <p>Constructs a point from a map of coordinate properties. Supports:</p>
 * <ul>
 *   <li>WGS-84 2D: {@code point({longitude: x, latitude: y})}</li>
 *   <li>WGS-84 3D: {@code point({longitude: x, latitude: y, height: z})}</li>
 *   <li>Cartesian 2D: {@code point({x: a, y: b})}</li>
 *   <li>Cartesian 3D: {@code point({x: a, y: b, z: c})}</li>
 * </ul>
 * <p>Also supports an ArcadeDB-specific 2-arg positional form {@code point(x, y)}, equivalent to
 * {@code point({longitude: x, latitude: y})} per the universal GIS {@code (x, y)} convention. Neo4j
 * has no such positional form.</p>
 * <p>The returned map contains the coordinate keys and a {@code crs} field indicating
 * the coordinate reference system.</p>
 * <p>Numeric coordinate keys that resolve to a {@link String} are coerced to {@link Number}
 * when the string parses cleanly as a decimal (e.g. a node whose {@code lat} property was
 * declared as {@link com.arcadedb.schema.Type#STRING}). When coercion is impossible the
 * function raises a {@link CommandExecutionException} naming the offending key and value
 * rather than leaking a raw {@link ClassCastException} (issue #4305).</p>
 */
public class CypherPointFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "point";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args == null || args.length == 0 || args.length > 2)
      throw new CommandExecutionException("point() requires either one map argument (point({...})) or two numeric arguments (point(x, y))");

    // 2-arg positional form: point(x, y) ≡ point(longitude, latitude) → WGS-84 2D.
    // Follows the universal GIS convention where the first ordinate is x (longitude) and the
    // second is y (latitude). Neo4j has no positional form; this is an ArcadeDB extension (issue #4578).
    if (args.length == 2) {
      if (args[0] == null || args[1] == null)
        return null;
      final double x = coerceCoordinate("x", args[0]);
      final double y = coerceCoordinate("y", args[1]);
      final Map<String, Object> result = new HashMap<>(Map.of(
          "longitude", x,
          "latitude", y,
          "x", x,
          "y", y,
          "crs", "WGS-84",
          "srid", 4326));
      return result;
    }

    if (args[0] == null)
      return null;
    if (!(args[0] instanceof Map))
      throw new CommandExecutionException("point() argument must be a map with coordinate properties");
    final Map<?, ?> map = (Map<?, ?>) args[0];

    final Map<String, Object> result = new LinkedHashMap<>();

    if (map.containsKey("longitude") || map.containsKey("latitude")) {
      // WGS-84 coordinate system
      final Object lon = map.get("longitude");
      final Object lat = map.get("latitude");
      if (lon == null || lat == null)
        return null;
      final double x = coerceCoordinate("longitude", lon);
      final double y = coerceCoordinate("latitude", lat);
      result.put("longitude", x);
      result.put("latitude", y);
      result.put("x", x);
      result.put("y", y);
      addOptionalZ(result, map);
      if (result.containsKey("z"))
        result.put("height", result.get("z"));
      result.put("crs", result.containsKey("z") ? "WGS-84-3D" : "WGS-84");
      result.put("srid", result.containsKey("z") ? 4979 : 4326);
    } else if (map.containsKey("x") || map.containsKey("y")) {
      // Cartesian coordinate system
      final Object xv = map.get("x");
      final Object yv = map.get("y");
      if (xv == null || yv == null)
        return null;
      final double x = coerceCoordinate("x", xv);
      final double y = coerceCoordinate("y", yv);
      result.put("x", x);
      result.put("y", y);
      addOptionalZ(result, map);
      final Object crsObj = map.get("crs");
      if (crsObj != null)
        result.put("crs", crsObj.toString());
      else
        result.put("crs", result.containsKey("z") ? "cartesian-3D" : "cartesian");
      if (map.containsKey("srid")) {
        final Object sridObj = map.get("srid");
        if (!(sridObj instanceof Number))
          throw new CommandExecutionException(
              "point() 'srid' must be numeric, found " + describe(sridObj));
        result.put("srid", ((Number) sridObj).intValue());
      }
    } else {
      throw new CommandExecutionException("point() map must contain x/y or longitude/latitude properties");
    }

    return result;
  }

  private void addOptionalZ(final Map<String, Object> result, final Map<?, ?> map) {
    if (map.containsKey("z")) {
      final Object zv = map.get("z");
      if (zv != null)
        result.put("z", coerceCoordinate("z", zv));
    } else if (map.containsKey("height")) {
      final Object hv = map.get("height");
      if (hv != null)
        result.put("z", coerceCoordinate("height", hv));
    }
  }

  /**
   * Returns the numeric value of {@code value}, coercing a numeric {@link String} when the
   * underlying property is typed as STRING but the contents are a clean decimal literal.
   * Throws a {@link CommandExecutionException} that names {@code key} and {@code value}
   * when coercion is impossible, so the user sees a clear error instead of a raw
   * {@link ClassCastException} (issue #4305).
   */
  private static double coerceCoordinate(final String key, final Object value) {
    if (value instanceof Number n)
      return n.doubleValue();
    if (value instanceof String s) {
      try {
        return Double.parseDouble(s.trim());
      } catch (final NumberFormatException ignored) {
        throw new CommandExecutionException(
            "point() '" + key + "' must be numeric, found String value '" + s + "'");
      }
    }
    throw new CommandExecutionException(
        "point() '" + key + "' must be numeric, found " + describe(value));
  }

  private static String describe(final Object value) {
    if (value == null)
      return "null";
    return value.getClass().getSimpleName() + " value '" + value + "'";
  }
}
