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

import java.util.Map;

/**
 * Cypher {@code point.distance(point1, point2)} function.
 *
 * <p>Computes the distance between two points. Uses Haversine formula for WGS-84
 * geographic points (result in meters), and Euclidean distance for Cartesian points.</p>
 */
public class CypherPointDistanceFunction implements StatelessFunction {
  private static final double EARTH_RADIUS_M = 6371000.0;

  @Override
  public String getName() {
    return "point.distance";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args == null || args.length != 2)
      throw new CommandExecutionException("point.distance() requires exactly 2 arguments");
    if (args[0] == null || args[1] == null)
      return null;
    if (!(args[0] instanceof Map) || !(args[1] instanceof Map))
      throw new CommandExecutionException("point.distance() arguments must be point values (maps)");
    final Map<?, ?> p1 = (Map<?, ?>) args[0];
    final Map<?, ?> p2 = (Map<?, ?>) args[1];

    final boolean geo1 = isGeographic(p1);
    final boolean geo2 = isGeographic(p2);

    // Points in different coordinate reference systems (WGS-84 vs Cartesian) are not comparable.
    // Matching the Neo4j reference implementation, return null instead of silently computing a
    // meaningless Euclidean distance over the lon/lat numerics (issue #4577).
    if (geo1 != geo2)
      return null;

    // WGS-84: use Haversine formula
    if (geo1) {
      final Number lon1n = coordinate(p1, "longitude", "x");
      final Number lat1n = coordinate(p1, "latitude", "y");
      final Number lon2n = coordinate(p2, "longitude", "x");
      final Number lat2n = coordinate(p2, "latitude", "y");
      if (lat1n == null || lon1n == null || lat2n == null || lon2n == null)
        return null;
      return haversineDistance(lat1n.doubleValue(), lon1n.doubleValue(), lat2n.doubleValue(), lon2n.doubleValue());
    }

    // Cartesian: use Euclidean distance
    final Number x1n = (Number) p1.get("x");
    final Number y1n = (Number) p1.get("y");
    final Number x2n = (Number) p2.get("x");
    final Number y2n = (Number) p2.get("y");
    if (x1n == null || y1n == null || x2n == null || y2n == null)
      return null;
    final double dx = x2n.doubleValue() - x1n.doubleValue();
    final double dy = y2n.doubleValue() - y1n.doubleValue();
    double sumSq = dx * dx + dy * dy;
    final Number z1n = (Number) p1.get("z");
    final Number z2n = (Number) p2.get("z");
    if ((z1n == null) != (z2n == null))
      return null;
    if (z1n != null) {
      final double dz = z2n.doubleValue() - z1n.doubleValue();
      sumSq += dz * dz;
    }
    return Math.sqrt(sumSq);
  }

  /**
   * Returns {@code true} when the point is in a geographic (WGS-84) coordinate reference system.
   * Prefers the explicit {@code crs} field set by {@code point()}; falls back to the presence of
   * {@code longitude}/{@code latitude} keys for raw maps that carry no {@code crs}.
   */
  private static boolean isGeographic(final Map<?, ?> p) {
    final Object crs = p.get("crs");
    if (crs instanceof String s) {
      if (s.startsWith("WGS-84"))
        return true;
      if (s.startsWith("cartesian"))
        return false;
    }
    return p.containsKey("longitude") && p.containsKey("latitude");
  }

  /**
   * Reads a numeric ordinate, preferring {@code primaryKey} (e.g. {@code longitude}) and falling
   * back to {@code fallbackKey} (e.g. {@code x}) so WGS-84 points constructed via either key style
   * resolve correctly. Returns {@code null} when neither key holds a {@link Number}.
   */
  private static Number coordinate(final Map<?, ?> p, final String primaryKey, final String fallbackKey) {
    final Object v = p.get(primaryKey);
    if (v instanceof Number n)
      return n;
    final Object f = p.get(fallbackKey);
    return f instanceof Number n ? n : null;
  }

  private double haversineDistance(final double lat1, final double lon1, final double lat2, final double lon2) {
    final double dLat = Math.toRadians(lat2 - lat1);
    final double dLon = Math.toRadians(lon2 - lon1);
    final double a = Math.pow(Math.sin(dLat / 2), 2)
        + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
        * Math.pow(Math.sin(dLon / 2), 2);
    return 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS_M;
  }
}
