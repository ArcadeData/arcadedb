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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.LinkedHashMap;
import java.util.Locale;
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
 * function raises a {@link CommandSemanticException} naming the offending key and value
 * rather than leaking a raw {@link ClassCastException} (issue #4305) or reporting a client
 * argument error as an internal server fault (issue #5794).</p>
 */
public class CypherPointFunction implements StatelessFunction {
  private static final int SRID_CARTESIAN_2D = 7203;
  private static final int SRID_CARTESIAN_3D = 9157;
  private static final int SRID_WGS84_2D     = 4326;
  private static final int SRID_WGS84_3D     = 4979;

  @Override
  public String getName() {
    return "point";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);

    // 2-arg positional form: point(x, y) ≡ point(longitude, latitude) → WGS-84 2D.
    // Follows the universal GIS convention where the first ordinate is x (longitude) and the
    // second is y (latitude). Neo4j has no positional form; this is an ArcadeDB extension (issue #4578).
    if (args.length == 2) {
      if (args[0] == null || args[1] == null)
        return null;
      final double x = coerceCoordinate("x", args[0]);
      final double y = coerceCoordinate("y", args[1]);
      final Map<String, Object> result = new LinkedHashMap<>();
      result.put("longitude", x);
      result.put("latitude", y);
      result.put("x", x);
      result.put("y", y);
      result.put("crs", "WGS-84");
      result.put("srid", SRID_WGS84_2D);
      return result;
    }

    if (args[0] == null)
      return null;
    // A non-map argument is determined entirely by the supplied argument, so it is a client error (HTTP 400)
    // rather than a CommandExecutionException (HTTP 500). See issue #5910.
    if (!(args[0] instanceof Map))
      throw new CommandSemanticException("point() argument must be a map with coordinate properties");
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
      result.put("srid", result.containsKey("z") ? SRID_WGS84_3D : SRID_WGS84_2D);
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
      Integer srid = null;
      if (map.containsKey("srid")) {
        final Object sridObj = map.get("srid");
        // Same rationale as the non-map argument above: a non-numeric srid is a client error (issue #5910).
        if (!(sridObj instanceof Number))
          throw new CommandSemanticException(
              "point() 'srid' must be numeric, found " + describe(sridObj));
        srid = ((Number) sridObj).intValue();
      }
      String crs = crsObj != null ? crsObj.toString() : null;
      // Mirror Neo4j (issue #3993): the srid and the crs name always travel together, each derived from the other.
      // An explicit crs and srid that disagree are a client error, as in Neo4j.
      if (srid != null && crs != null) {
        final Integer crsSrid = sridOfCrs(crs);
        if (crsSrid != null && !crsSrid.equals(srid))
          throw new CommandSemanticException("point() 'crs' " + crs + " and 'srid' " + srid + " do not match");
      }
      // The dimension of a known crs/srid must match the supplied coordinates, as in Neo4j.
      final boolean has3d = result.containsKey("z");
      final Integer knownSrid = srid != null ? srid : crs != null ? sridOfCrs(crs) : null;
      if (knownSrid != null && isKnownSrid(knownSrid) && is3dSrid(knownSrid) != has3d)
        throw new CommandSemanticException(
            "point() " + (has3d ? "3D coordinates" : "2D coordinates") + " do not match the dimension of srid " + knownSrid);
      // An unknown crs name is kept as given, without an srid, as before.
      if (srid == null) {
        if (crs == null)
          srid = has3d ? SRID_CARTESIAN_3D : SRID_CARTESIAN_2D;
        else
          srid = sridOfCrs(crs);
      }
      if (crs == null)
        crs = crsOfSrid(srid, result.containsKey("z"));
      result.put("crs", crs);
      if (srid != null)
        result.put("srid", srid);
    } else {
      // Missing recognized coordinate keys is a client error (issue #5910), same rationale as above.
      throw new CommandSemanticException("point() map must contain x/y or longitude/latitude properties");
    }

    return result;
  }

  /** Neo4j SRID of a well-known CRS name, or null when the name is not one of the four Neo4j defines. */
  private static Integer sridOfCrs(final String crs) {
    return switch (crs.toLowerCase(Locale.ROOT)) {
      case "cartesian" -> SRID_CARTESIAN_2D;
      case "cartesian-3d" -> SRID_CARTESIAN_3D;
      case "wgs-84" -> SRID_WGS84_2D;
      case "wgs-84-3d" -> SRID_WGS84_3D;
      default -> null;
    };
  }

  private static boolean isKnownSrid(final int srid) {
    return srid == SRID_CARTESIAN_2D || srid == SRID_CARTESIAN_3D || srid == SRID_WGS84_2D || srid == SRID_WGS84_3D;
  }

  private static boolean is3dSrid(final int srid) {
    return srid == SRID_CARTESIAN_3D || srid == SRID_WGS84_3D;
  }

  /** CRS name of a well-known Neo4j SRID; an unknown SRID falls back to the cartesian name of the given dimension. */
  private static String crsOfSrid(final int srid, final boolean is3d) {
    return switch (srid) {
      case SRID_WGS84_2D -> "WGS-84";
      case SRID_WGS84_3D -> "WGS-84-3D";
      case SRID_CARTESIAN_3D -> "cartesian-3D";
      case SRID_CARTESIAN_2D -> "cartesian";
      default -> is3d ? "cartesian-3D" : "cartesian";
    };
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
   * Throws a {@link CommandSemanticException} that names {@code key} and {@code value}
   * when coercion is impossible, so the user sees a clear client-facing error (HTTP 400)
   * instead of a raw {@link ClassCastException} (issue #4305) or an internal-fault-looking
   * {@link CommandExecutionException} (HTTP 500, issue #5794): a non-numeric coordinate is
   * determined entirely by the supplied argument, so it is a client error either way.
   */
  private static double coerceCoordinate(final String key, final Object value) {
    if (value instanceof Number n)
      return n.doubleValue();
    if (value instanceof String s) {
      try {
        return Double.parseDouble(s.trim());
      } catch (final NumberFormatException ignored) {
        throw new CommandSemanticException(
            "point() '" + key + "' must be numeric, found String value '" + s + "'");
      }
    }
    throw new CommandSemanticException(
        "point() '" + key + "' must be numeric, found " + describe(value));
  }

  private static String describe(final Object value) {
    if (value == null)
      return "null";
    return value.getClass().getSimpleName() + " value '" + value + "'";
  }
}
