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
package com.arcadedb.function.sql.geo;

import com.arcadedb.log.LogManager;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.operation.distance.DistanceOp;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.ShapeIO;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;

/**
 * Geospatial utility class.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class GeoUtils {
  static final JtsSpatialContextFactory FACTORY         = newFactory();
  static final JtsSpatialContext        SPATIAL_CONTEXT = new JtsSpatialContext(FACTORY);
  /** Mean Earth radius of the haversine formula {@code geo.distance()} answers with, in kilometres. */
  public static final double EARTH_RADIUS_KM = 6371.0;

  // WKTReader/WKTWriter are not thread-safe; use ThreadLocal to avoid per-call allocation
  private static final ThreadLocal<WKTReader> WKT_READER = ThreadLocal.withInitial(WKTReader::new);
  private static final ThreadLocal<WKTWriter> WKT_WRITER = ThreadLocal.withInitial(WKTWriter::new);

  /**
   * Geodetic (lon/lat), with {@link DatelineRule#none}: a polygon means the area its coordinates enclose, read on the
   * longitude/latitude plane as OGC WKT, PostGIS {@code geometry}, Shapely and GeoJSON (RFC 7946) read it.
   * <p>
   * Spatial4j's default, {@link DatelineRule#width180}, assumed no polygon is wider than 180 degrees and read a wider
   * one as crossing the antimeridian: a band from -100 to 100 of longitude was taken for the 160 degrees around the
   * back of the globe, a polar cap from -180 to 180 collapsed to nothing, and a wide polygon that is not a rectangle
   * failed to parse at all (issue #10210). A shape crossing the antimeridian is written either as a MULTIPOLYGON split
   * at 180, or with longitudes past it ({@code 170 .. 190}), which {@link #parseGeometry} splits there.
   */
  private static JtsSpatialContextFactory newFactory() {
    final JtsSpatialContextFactory factory = new JtsSpatialContextFactory();
    factory.datelineRule = DatelineRule.none;
    return factory;
  }

  public static SpatialContextFactory getFactory() {
    return FACTORY;
  }

  public static SpatialContext getSpatialContext() {
    return SPATIAL_CONTEXT;
  }

  public static double getDoubleValue(final Object param) {
    if (!(param instanceof Number))
      throw new IllegalArgumentException("Expected a numeric value, got: " + (param == null ? "null" : param.getClass().getSimpleName()));
    return ((Number) param).doubleValue();
  }

  /**
   * Reads the x/y of one vertex given to a geometry constructor ({@code geo.lineString()}, {@code geo.polygon()}): a
   * Spatial4j point, an {@code [x, y]} pair, or whatever a {@code point()} call returns (the WKT string {@code POINT (x y)})
   * or a Cypher point map.
   *
   * @throws IllegalArgumentException if the value is not a point
   */
  public static double[] pointXY(final Object point) {
    if (point instanceof Point p)
      return new double[] { p.getX(), p.getY() };
    if (point instanceof List<?> list) {
      if (list.size() < 2)
        throw new IllegalArgumentException("Invalid point element: " + point);
      try {
        return new double[] { getDoubleValue(list.get(0)), getDoubleValue(list.get(1)) };
      } catch (final ClassCastException | IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid point element: " + point, e);
      }
    }
    if (point instanceof String || point instanceof Map<?, ?>) {
      final Shape shape;
      try {
        shape = parseGeometry(point);
      } catch (final IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid point element: " + point, e);
      }
      if (shape instanceof Point p)
        return new double[] { p.getX(), p.getY() };
    }
    throw new IllegalArgumentException("Invalid point element: " + point);
  }

  /**
   * Parse a value that can be either a Spatial4j Shape or a WKT string into a Shape.
   * Returns null if the value is null.
   */
  public static Shape parseGeometry(final Object value) {
    if (value == null)
      return null;
    if (value instanceof Shape shape)
      return shape;
    // Cypher point() returns a Map with x/y or longitude/latitude keys
    if (value instanceof Map<?, ?> map) {
      final double x;
      final double y;
      if (map.containsKey("x") && map.containsKey("y")) {
        x = coordinate(map.get("x"), "x");
        y = coordinate(map.get("y"), "y");
      } else if (map.containsKey("longitude") && map.containsKey("latitude")) {
        x = coordinate(map.get("longitude"), "longitude");
        y = coordinate(map.get("latitude"), "latitude");
      } else {
        throw new IllegalArgumentException("Cannot parse geometry from map: missing x/y or longitude/latitude keys");
      }
      return SPATIAL_CONTEXT.getShapeFactory().pointXY(x, y);
    }
    final String wkt = value.toString().trim();
    if (wkt.isEmpty())
      return null;
    try {
      return SPATIAL_CONTEXT.getFormats().getReader(ShapeIO.WKT).read(wkt);
    } catch (final Exception e) {
      final Shape wrapped = splitAtAntimeridian(wkt);
      if (wrapped != null)
        return wrapped;
      throw new IllegalArgumentException("Cannot parse geometry from: " + wkt + " (" + e.getMessage() + ")", e);
    }
  }

  /**
   * Reads a WKT whose longitudes run past +/-180 (up to one turn of the globe either way) as the same shape cut at the
   * antimeridian, each part moved back into [-180, 180]: {@code POLYGON ((170 0, 190 0, 190 10, 170 10, 170 0))} is the
   * 20 degrees across the antimeridian, two polygons on either side of it. The way to write a crossing shape as one
   * polygon now that a wide polygon is read as written (issue #10210), as PostGIS's {@code ST_WrapX} does.
   *
   * @return null when the WKT is not such a shape: unreadable, a latitude out of range, a longitude past one turn
   */
  private static Shape splitAtAntimeridian(final String wkt) {
    final Geometry geometry;
    try {
      geometry = WKT_READER.get().read(wkt);
    } catch (final ParseException e) {
      return null;
    }
    final Envelope envelope = geometry.getEnvelopeInternal();
    if (envelope.isNull() || (envelope.getMinX() >= -180 && envelope.getMaxX() <= 180) || envelope.getMinY() < -90
        || envelope.getMaxY() > 90 || envelope.getMinX() < -540 || envelope.getMaxX() > 540)
      return null;

    try {
      final GeometryFactory factory = geometry.getFactory();
      Geometry result = null;
      for (int shift = -360; shift <= 360; shift += 360) {
        // the part that lands inside [-180, 180] once moved by 'shift'
        final Geometry part = geometry.intersection(factory.toGeometry(new Envelope(-180 - shift, 180 - shift, -90, 90)));
        if (part.isEmpty())
          continue;
        final Geometry moved = AffineTransformation.translationInstance(shift, 0).transform(part);
        result = result == null ? moved : result.union(moved);
      }
      return result == null ? null : SPATIAL_CONTEXT.getShapeFactory().makeShapeFromGeometry(result);
    } catch (final RuntimeException e) {
      // an invalid shape (a self-intersecting polygon) fails in the overlay: not a geometry, like any other unreadable one
      LogManager.instance().log(GeoUtils.class, Level.FINE, "Cannot split the geometry '%s' at the antimeridian: %s", wkt,
          e.getMessage());
      return null;
    }
  }

  /**
   * The great-circle distance between two shapes, in degrees of arc: 0 when they intersect, otherwise the distance
   * between their two closest points. {@code geo.dWithin} and {@code geo.distance} used to measure between the CENTRES
   * of the shapes, so a point inside a polygon was far from it and a point near its edge farther than it is (issue #10210).
   * <p>
   * The closest points are found on the longitude/latitude plane, the way the predicates read a shape, with the second
   * shape also tried one turn either way so that two shapes facing each other across the antimeridian are measured the
   * short way round. A circle is its centre less its radius. The distance between those two points is the great-circle
   * one, so it is exact for a point and, for a larger shape, close to the shortest distance on the sphere but not always
   * equal to it: the plane picks the nearest edge in degrees, and a degree of longitude is shorter the nearer the pole.
   */
  public static double distanceDegrees(final Shape a, final Shape b) {
    if (a instanceof Circle circle)
      return Math.max(0.0, distanceDegrees(circle.getCenter(), b) - circle.getRadius());
    if (b instanceof Circle circle)
      return Math.max(0.0, distanceDegrees(a, circle.getCenter()) - circle.getRadius());
    if (a instanceof Point p1 && b instanceof Point p2)
      return haversineDegrees(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    if (a.relate(b).intersects())
      return 0.0;

    final JtsShapeFactory shapeFactory = SPATIAL_CONTEXT.getShapeFactory();
    final Geometry ga = shapeFactory.getGeometryFrom(a);
    final Geometry gb = shapeFactory.getGeometryFrom(b);
    double min = Double.MAX_VALUE;
    for (int shift = -360; shift <= 360; shift += 360) {
      final Geometry moved = shift == 0 ? gb : AffineTransformation.translationInstance(shift, 0).transform(gb);
      final Coordinate[] closest = DistanceOp.nearestPoints(ga, moved);
      min = Math.min(min, haversineDegrees(closest[0].x, closest[0].y, closest[1].x - shift, closest[1].y));
    }
    return min;
  }

  /** The central angle between two lon/lat points, in degrees, by the haversine formula. */
  public static double haversineDegrees(final double lon1, final double lat1, final double lon2, final double lat2) {
    return Math.toDegrees(haversineRadians(lon1, lat1, lon2, lat2));
  }

  /** The central angle between two lon/lat points, in radians, by the haversine formula. */
  public static double haversineRadians(final double lon1, final double lat1, final double lon2, final double lat2) {
    final double deltaLat = Math.toRadians(lat2 - lat1);
    final double deltaLon = Math.toRadians(lon2 - lon1);
    final double h = Math.pow(Math.sin(deltaLat / 2), 2)
        + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.pow(Math.sin(deltaLon / 2), 2);
    return 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
  }

  /**
   * Reads one coordinate of a point map. A non-numeric entry is an unparseable geometry like any other, so it has to
   * leave as {@link IllegalArgumentException}: callers that skip a value they cannot parse - a WHERE clause dropping a
   * row whose column holds junk - catch that, and a raw {@link ClassCastException} would escape them and fail the whole
   * query instead of the one row (issue #5600).
   */
  private static double coordinate(final Object value, final String key) {
    if (!(value instanceof Number number))
      throw new IllegalArgumentException(
          "Cannot parse geometry from map: '" + key + "' must be a number, got: " + (value == null ?
              "null" :
              value.getClass().getSimpleName()));
    return number.doubleValue();
  }

  /**
   * Convert a Shape to WKT string using the Spatial4j WKT writer.
   * Returns null if the shape is null.
   */
  public static String toWKT(final Shape shape) {
    if (shape == null)
      return null;
    return SPATIAL_CONTEXT.getFormats().getWriter(ShapeIO.WKT).toString(shape);
  }

  /**
   * Parse a WKT string or Shape into a JTS Geometry for advanced operations (buffer, envelope, etc.).
   * Returns null if the value is null.
   * <p>
   * When the input is a {@link JtsGeometry}, the underlying JTS geometry is returned directly to
   * avoid lossy WKT round-trips (Spatial4j may write Rectangle shapes as ENVELOPE WKT which is
   * not understood by the JTS WKT reader).
   * </p>
   */
  public static Geometry parseJtsGeometry(final Object value) {
    if (value == null)
      return null;
    // Fast path: JtsGeometry wraps the JTS Geometry directly — no WKT round-trip needed
    if (value instanceof JtsGeometry jtsShape)
      return jtsShape.getGeom();
    // For Rectangle (bounding box shapes), build the polygon manually to avoid ENVELOPE WKT
    if (value instanceof Rectangle rect)
      return buildPolygonFromRect(rect);
    final String wkt;
    if (value instanceof Shape shape)
      wkt = toWKT(shape);
    else
      wkt = value.toString().trim();
    if (wkt == null || wkt.isEmpty())
      return null;
    // If the WKT is ENVELOPE format (Spatial4j-specific), convert to polygon
    if (wkt.startsWith("ENVELOPE"))
      return parseEnvelopeWkt(wkt);
    try {
      return WKT_READER.get().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Cannot parse JTS geometry from WKT: " + wkt, e);
    }
  }

  /**
   * Builds a rectangular JTS Polygon from a Spatial4j Rectangle.
   */
  private static Geometry buildPolygonFromRect(final Rectangle rect) {
    final double minX = rect.getMinX();
    final double maxX = rect.getMaxX();
    final double minY = rect.getMinY();
    final double maxY = rect.getMaxY();
    final String wkt = "POLYGON ((" +
        formatCoord(minX) + " " + formatCoord(minY) + ", " +
        formatCoord(maxX) + " " + formatCoord(minY) + ", " +
        formatCoord(maxX) + " " + formatCoord(maxY) + ", " +
        formatCoord(minX) + " " + formatCoord(maxY) + ", " +
        formatCoord(minX) + " " + formatCoord(minY) + "))";
    try {
      return WKT_READER.get().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Cannot build polygon from rectangle: " + rect, e);
    }
  }

  /**
   * Parses Spatial4j's ENVELOPE(minX, maxX, maxY, minY) into a JTS polygon.
   */
  private static Geometry parseEnvelopeWkt(final String envelopeWkt) {
    // Format: ENVELOPE (minX, maxX, maxY, minY)
    final int start = envelopeWkt.indexOf('(');
    final int end = envelopeWkt.lastIndexOf(')');
    if (start < 0 || end < 0)
      throw new IllegalArgumentException("Invalid ENVELOPE WKT: " + envelopeWkt);
    final String[] parts = envelopeWkt.substring(start + 1, end).split(",");
    if (parts.length != 4)
      throw new IllegalArgumentException("Invalid ENVELOPE WKT: " + envelopeWkt);
    final double minX = Double.parseDouble(parts[0].trim());
    final double maxX = Double.parseDouble(parts[1].trim());
    final double maxY = Double.parseDouble(parts[2].trim());
    final double minY = Double.parseDouble(parts[3].trim());
    final String wkt = "POLYGON ((" +
        formatCoord(minX) + " " + formatCoord(minY) + ", " +
        formatCoord(maxX) + " " + formatCoord(minY) + ", " +
        formatCoord(maxX) + " " + formatCoord(maxY) + ", " +
        formatCoord(minX) + " " + formatCoord(maxY) + ", " +
        formatCoord(minX) + " " + formatCoord(minY) + "))";
    try {
      return WKT_READER.get().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Cannot parse ENVELOPE as polygon: " + envelopeWkt, e);
    }
  }

  /**
   * Convert a JTS Geometry to WKT string.
   */
  public static String jtsToWKT(final Geometry geometry) {
    if (geometry == null)
      return null;
    return WKT_WRITER.get().write(geometry);
  }

  /**
   * Format a double for WKT output: no trailing zeros, uses dot decimal separator.
   */
  public static String formatCoord(final double value) {
    // Remove trailing zeros while using US locale for decimal point
    final String s = String.format(Locale.US, "%.10f", value);
    // Strip trailing zeros after decimal point
    int end = s.length();
    while (end > 1 && s.charAt(end - 1) == '0') end--;
    if (s.charAt(end - 1) == '.') end--;
    return s.substring(0, end);
  }
}
