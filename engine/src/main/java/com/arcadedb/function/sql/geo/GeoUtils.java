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

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.ShapeIO;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.util.Locale;

/**
 * Geospatial utility class.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class GeoUtils {
  static final JtsSpatialContextFactory FACTORY         = new JtsSpatialContextFactory();
  static final JtsSpatialContext        SPATIAL_CONTEXT = new JtsSpatialContext(FACTORY);

  // WKTReader/WKTWriter are not thread-safe; use ThreadLocal to avoid per-call allocation
  private static final ThreadLocal<WKTReader> WKT_READER = ThreadLocal.withInitial(WKTReader::new);
  private static final ThreadLocal<WKTWriter> WKT_WRITER = ThreadLocal.withInitial(WKTWriter::new);

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
   * Parse a value that can be either a Spatial4j Shape or a WKT string into a Shape.
   * Returns null if the value is null.
   */
  public static Shape parseGeometry(final Object value) {
    if (value == null)
      return null;
    if (value instanceof Shape shape)
      return shape;
    final String wkt = value.toString().trim();
    if (wkt.isEmpty())
      return null;
    try {
      return SPATIAL_CONTEXT.getFormats().getReader(ShapeIO.WKT).read(wkt);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot parse geometry from: " + wkt, e);
    }
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
