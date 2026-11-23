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
import com.arcadedb.query.opencypher.executor.CypherValues;
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
 * <p>Also supports an ArcadeDB-specific 2-arg positional form {@code point(x, y)}, equivalent to
 * {@code point({longitude: x, latitude: y})} per the universal GIS {@code (x, y)} convention. Neo4j
 * has no such positional form.</p>
 * <p>For the cartesian (x/y) form the {@code srid} and {@code crs} name are derived from each other (cartesian 7203 and
 * cartesian-3d 9157, wgs-84 4326 and wgs-84-3d 4979), known names are matched case-insensitively and stored in canonical
 * spelling, an unknown crs name is kept without an srid and an unknown srid gets the cartesian name (both unlike Neo4j,
 * which rejects them), and a disagreeing pair or a dimension that does not match the srid is rejected (issue #3993).</p>
 * <p>A map with {@code x}/{@code y} is built from them even when it also holds {@code longitude}/{@code latitude}, as in
 * Neo4j, and a geographic srid or crs (4326, 4979, {@code wgs-84}, {@code wgs-84-3d}) given with {@code x}/{@code y}
 * builds the same point as {@code longitude}/{@code latitude}/{@code height} do: it carries both spellings of its
 * coordinates. A coordinate must be finite, and a map with only one of the two coordinates of its style is not a point
 * (issue #12276).</p>
 * <p>The returned map contains the coordinate keys and a {@code crs} field indicating
 * the coordinate reference system, spelled as Neo4j spells it: {@code cartesian}, {@code cartesian-3d}, {@code wgs-84},
 * {@code wgs-84-3d} (issue #11113). Points written before that fix carry {@code WGS-84}, {@code WGS-84-3D} or
 * {@code cartesian-3D}, so a reader of a crs name compares it case-insensitively ({@link #isGeographicCrs},
 * {@link #sridOfCrs}).</p>
 * <p>Numeric coordinate keys that resolve to a {@link String} are coerced to {@link Number}
 * when the string parses cleanly as a decimal (e.g. a node whose {@code lat} property was
 * declared as {@link com.arcadedb.schema.Type#STRING}). When coercion is impossible the
 * function raises a {@link CommandSemanticException} naming the offending key and value
 * rather than leaking a raw {@link ClassCastException} (issue #4305) or reporting a client
 * argument error as an internal server fault (issue #5794).</p>
 */
public class CypherPointFunction implements StatelessFunction {
  public static final String CRS_CARTESIAN_2D = "cartesian";
  public static final String CRS_CARTESIAN_3D = "cartesian-3d";
  public static final String CRS_WGS84_2D     = "wgs-84";
  public static final String CRS_WGS84_3D     = "wgs-84-3d";

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
      return geographic(coerceCoordinate("x", args[0]), coerceCoordinate("y", args[1]), null);
    }

    if (args[0] == null)
      return null;
    // A non-map argument is determined entirely by the supplied argument, so it is a client error (HTTP 400)
    // rather than a CommandExecutionException (HTTP 500). See issue #5910.
    if (!(args[0] instanceof Map))
      throw new CommandSemanticException("point() argument must be a map with coordinate properties");
    final Map<?, ?> map = (Map<?, ?>) args[0];

    // x/y take precedence over longitude/latitude, as in Neo4j: a map with both is built from x/y, and is a geographic point
    // only when its crs or srid says so (issue #12276)
    if (map.containsKey("x") || map.containsKey("y"))
      return fromXY(map);
    if (map.containsKey("longitude") || map.containsKey("latitude")) {
      final Object lon = requireKey(map, "longitude");
      final Object lat = requireKey(map, "latitude");
      if (lon == null || lat == null)
        return null;
      return geographic(coerceCoordinate("longitude", lon), coerceCoordinate("latitude", lat), optionalZ(map));
    }
    // Missing recognized coordinate keys is a client error (issue #5910), same rationale as above.
    throw new CommandSemanticException("point() map must contain x/y or longitude/latitude properties");
  }

  private static Map<String, Object> fromXY(final Map<?, ?> map) {
    final Object xv = requireKey(map, "x");
    final Object yv = requireKey(map, "y");
    if (xv == null || yv == null)
      return null;
    final double x = coerceCoordinate("x", xv);
    final double y = coerceCoordinate("y", yv);
    final Double z = optionalZ(map);
    final Object crsObj = map.get("crs");
    if (crsObj != null && !(crsObj instanceof String))
      throw new CommandSemanticException("point() 'crs' must be a string, found " + describe(crsObj));
    Integer srid = null;
    if (map.containsKey("srid")) {
      final Object sridObj = map.get("srid");
      // Same rationale as the non-map argument above: a non-numeric srid is a client error (issue #5910).
      if (!(sridObj instanceof Number))
        throw new CommandSemanticException("point() 'srid' must be numeric, found " + describe(sridObj));
      final double sridValue = ((Number) sridObj).doubleValue();
      if (sridValue != Math.rint(sridValue) || sridValue < 0 || sridValue > Integer.MAX_VALUE)
        throw new CommandSemanticException("point() 'srid' must be a non-negative integer, found " + describe(sridObj));
      srid = (int) sridValue;
    }
    String crs = (String) crsObj;
    // Mirror Neo4j (issue #3993): the srid and the crs name always travel together, each derived from the other.
    final boolean has3d = z != null;
    final Integer crsSrid = crs != null ? sridOfCrs(crs) : null;
    // An explicit crs and srid that disagree are a client error, as in Neo4j. A known srid also contradicts an
    // unrecognised crs name, since the pair must travel together.
    if (srid != null && crs != null && (crsSrid != null ? !crsSrid.equals(srid) : isKnownSrid(srid)))
      throw new CommandSemanticException("point() 'crs' " + crs + " and 'srid' " + srid + " do not match");
    if (srid == null) {
      if (crs == null)
        srid = has3d ? SRID_CARTESIAN_3D : SRID_CARTESIAN_2D;
      else
        srid = crsSrid;
    }
    // The dimension of a known crs/srid must match the supplied coordinates, as in Neo4j.
    if (srid != null && isKnownSrid(srid) && is3dSrid(srid) != has3d)
      throw new CommandSemanticException(
          "point() " + (has3d ? "3D" : "2D") + " coordinates do not match the dimension of srid " + srid);
    // A geographic point built from x/y has its longitude/latitude (and height) too, as in Neo4j (issue #12276)
    if (srid != null && (srid == SRID_WGS84_2D || srid == SRID_WGS84_3D))
      return geographic(x, y, z);
    final Map<String, Object> result = new LinkedHashMap<>();
    result.put("x", x);
    result.put("y", y);
    if (has3d)
      result.put("z", z);
    // A known name is stored in canonical spelling so case-sensitive consumers (point.distance, withinBBox, Bolt) agree
    // with the srid; an unknown name is kept as given, without an srid.
    if (crs == null || crsSrid != null)
      crs = crsOfSrid(srid, has3d);
    result.put("crs", crs);
    if (srid != null)
      result.put("srid", srid);
    return result;
  }

  private static Map<String, Object> geographic(final double longitude, final double latitude, final Double height) {
    final double x = wrapLongitude(longitude);
    final double y = checkLatitude(latitude);
    final Map<String, Object> result = new LinkedHashMap<>();
    result.put("longitude", x);
    result.put("latitude", y);
    result.put("x", x);
    result.put("y", y);
    if (height != null) {
      result.put("z", height);
      result.put("height", height);
    }
    result.put("crs", height != null ? CRS_WGS84_3D : CRS_WGS84_2D);
    result.put("srid", height != null ? SRID_WGS84_3D : SRID_WGS84_2D);
    return result;
  }

  /** The value of a coordinate key the map must have: a missing key is a client error, as in Neo4j (issue #12276). */
  private static Object requireKey(final Map<?, ?> map, final String key) {
    if (!map.containsKey(key))
      throw invalidPoint(map);
    return map.get(key);
  }

  /** Names the keys of the map, a capped and cleaned list of them: the message travels to the client and into the log. */
  private static CommandSemanticException invalidPoint(final Map<?, ?> map) {
    return new CommandSemanticException("A map with keys " + CypherValues.describeKeys(map) + " is not describing a valid point");
  }

  /**
   * Rejects a WGS-84 latitude outside [-90, 90] with a client error, as Neo4j does (issue #11690).
   */
  private static double checkLatitude(final double latitude) {
    if (latitude > 90 || latitude < -90)
      throw new CommandSemanticException(
          "Cannot create WGS-84 point with invalid latitude " + latitude + ": valid range is [-90, 90]");
    return latitude;
  }

  /**
   * Wraps a WGS-84 longitude into [-180, 180] the way Neo4j does (adding or subtracting whole turns), so 181 becomes
   * -179 (issue #11690). Non-finite values are left alone.
   */
  static double wrapLongitude(final double longitude) {
    if (longitude <= 180 && longitude >= -180 || !Double.isFinite(longitude))
      return longitude;
    return longitude > 180 ? longitude - 360 * Math.ceil((longitude - 180) / 360) : longitude + 360 * Math.ceil((-180 - longitude) / 360);
  }

  /**
   * Renders a point the way Neo4j renders it in {@code toString()}: {@code point({x: 1.0, y: 2.0, crs: 'cartesian'})},
   * with the z coordinate between the y one and the crs when the point has one. The srid is not part of the rendering:
   * Neo4j prints the name of the coordinate reference system, and the srid is its numeric name (issue #12397).
   * <p>
   * The coordinates are printed as the doubles they are, the way Neo4j prints them: a point built by {@code point()}
   * holds doubles, so {@code point({x: 1, y: 2})} renders as {@code x: 1.0, y: 2.0}.
   *
   * @param point the point map, as {@code point()} builds it
   *
   * @return the rendered point
   */
  public static String renderPoint(final Map<?, ?> point) {
    final StringBuilder rendered = new StringBuilder("point({");
    rendered.append("x: ").append(coordinate(point.get("x")));
    rendered.append(", y: ").append(coordinate(point.get("y")));
    final Object z = point.get("z");
    if (z != null)
      rendered.append(", z: ").append(coordinate(z));
    final Object crs = point.get("crs");
    rendered.append(", crs: '").append(crs == null ? "" : crs).append("'})");
    return rendered.toString();
  }

  private static String coordinate(final Object value) {
    return value instanceof Number number ? Double.toString(number.doubleValue()) : String.valueOf(value);
  }

  /**
   * Neo4j SRID of a well-known CRS name in any letter case, or null when the name is not one of the four Neo4j defines.
   */
  public static Integer sridOfCrs(final String crs) {
    if (crs.equalsIgnoreCase(CRS_CARTESIAN_2D))
      return SRID_CARTESIAN_2D;
    if (crs.equalsIgnoreCase(CRS_CARTESIAN_3D))
      return SRID_CARTESIAN_3D;
    if (crs.equalsIgnoreCase(CRS_WGS84_2D))
      return SRID_WGS84_2D;
    if (crs.equalsIgnoreCase(CRS_WGS84_3D))
      return SRID_WGS84_3D;
    return null;
  }

  /**
   * Whether a crs name is one of the two WGS-84 names, in any letter case: a point stored before issue #11113 carries
   * {@code WGS-84}, one built since {@code wgs-84}.
   */
  public static boolean isGeographicCrs(final String crs) {
    return crs.equalsIgnoreCase(CRS_WGS84_2D) || crs.equalsIgnoreCase(CRS_WGS84_3D);
  }

  /** Whether a crs name is one of the two cartesian names, in any letter case (see {@link #isGeographicCrs}). */
  public static boolean isCartesianCrs(final String crs) {
    return crs.equalsIgnoreCase(CRS_CARTESIAN_2D) || crs.equalsIgnoreCase(CRS_CARTESIAN_3D);
  }

  private static boolean isKnownSrid(final int srid) {
    return srid == SRID_CARTESIAN_2D || srid == SRID_CARTESIAN_3D || srid == SRID_WGS84_2D || srid == SRID_WGS84_3D;
  }

  private static boolean is3dSrid(final int srid) {
    return srid == SRID_CARTESIAN_3D || srid == SRID_WGS84_3D;
  }

  /** CRS name of a well-known Neo4j SRID; an unknown SRID falls back to the cartesian name of the given dimension. */
  public static String crsOfSrid(final int srid, final boolean is3d) {
    return switch (srid) {
      case SRID_WGS84_2D -> CRS_WGS84_2D;
      case SRID_WGS84_3D -> CRS_WGS84_3D;
      case SRID_CARTESIAN_3D -> CRS_CARTESIAN_3D;
      case SRID_CARTESIAN_2D -> CRS_CARTESIAN_2D;
      default -> is3d ? CRS_CARTESIAN_3D : CRS_CARTESIAN_2D;
    };
  }

  private static Double optionalZ(final Map<?, ?> map) {
    if (map.containsKey("z")) {
      final Object zv = map.get("z");
      return zv != null ? coerceCoordinate("z", zv) : null;
    }
    final Object hv = map.get("height");
    return hv != null ? coerceCoordinate("height", hv) : null;
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
    final double coordinate;
    if (value instanceof Number n)
      coordinate = n.doubleValue();
    else if (value instanceof String s) {
      try {
        coordinate = Double.parseDouble(s.trim());
      } catch (final NumberFormatException ignored) {
        throw new CommandSemanticException(
            "point() '" + key + "' must be numeric, found String value '" + s + "'");
      }
    } else
      throw new CommandSemanticException(
          "point() '" + key + "' must be numeric, found " + describe(value));
    // As in Neo4j (issue #12276): an infinite or NaN coordinate made point.distance() answer Infinity or NaN
    if (!Double.isFinite(coordinate))
      throw new CommandSemanticException("Cannot create a point with non-finite coordinate values");
    return coordinate;
  }

  private static String describe(final Object value) {
    if (value == null)
      return "null";
    return value.getClass().getSimpleName() + " value '" + value + "'";
  }
}
