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
 * <p>The returned map contains the coordinate keys and a {@code crs} field indicating
 * the coordinate reference system.</p>
 */
public class CypherPointFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "point";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args == null || args.length == 0 || args.length > 2)
      throw new CommandExecutionException("point() requires either one map argument (point({...})) or two numeric arguments (point(latitude, longitude))");

    // 2-arg positional form: point(latitude, longitude) → WGS-84 2D
    if (args.length == 2) {
      if (args[0] == null || args[1] == null)
        return null;
      if (!(args[0] instanceof Number) || !(args[1] instanceof Number))
        throw new CommandExecutionException("point() with two arguments requires numeric latitude and longitude");
      final double lat = ((Number) args[0]).doubleValue();
      final double lon = ((Number) args[1]).doubleValue();
      final Map<String, Object> result = new LinkedHashMap<>();
      result.put("latitude", lat);
      result.put("longitude", lon);
      result.put("x", lon);
      result.put("y", lat);
      result.put("crs", "WGS-84");
      result.put("srid", 4326);
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
      final double x = ((Number) lon).doubleValue();
      final double y = ((Number) lat).doubleValue();
      result.put("longitude", x);
      result.put("latitude", y);
      result.put("x", x);
      result.put("y", y);
      addOptionalZ(result, map);
      result.put("crs", result.containsKey("z") ? "WGS-84-3D" : "WGS-84");
      result.put("srid", result.containsKey("z") ? 4979 : 4326);
    } else if (map.containsKey("x") || map.containsKey("y")) {
      // Cartesian coordinate system
      final Object xv = map.get("x");
      final Object yv = map.get("y");
      if (xv == null || yv == null)
        return null;
      final double x = ((Number) xv).doubleValue();
      final double y = ((Number) yv).doubleValue();
      result.put("x", x);
      result.put("y", y);
      addOptionalZ(result, map);
      final Object crsObj = map.get("crs");
      if (crsObj != null)
        result.put("crs", crsObj.toString());
      else
        result.put("crs", result.containsKey("z") ? "cartesian-3D" : "cartesian");
      if (map.containsKey("srid"))
        result.put("srid", ((Number) map.get("srid")).intValue());
    } else {
      throw new CommandExecutionException("point() map must contain x/y or longitude/latitude properties");
    }

    return result;
  }

  private void addOptionalZ(final Map<String, Object> result, final Map<?, ?> map) {
    if (map.containsKey("z")) {
      final Object zv = map.get("z");
      if (zv != null)
        result.put("z", ((Number) zv).doubleValue());
    } else if (map.containsKey("height")) {
      final Object hv = map.get("height");
      if (hv != null)
        result.put("z", ((Number) hv).doubleValue());
    }
  }
}
