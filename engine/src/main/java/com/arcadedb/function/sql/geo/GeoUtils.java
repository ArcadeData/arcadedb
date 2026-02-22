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
import org.locationtech.spatial4j.shape.Shape;

import java.util.Locale;

/**
 * Geospatial utility class.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class GeoUtils {
  static final JtsSpatialContextFactory FACTORY         = new JtsSpatialContextFactory();
  static final JtsSpatialContext        SPATIAL_CONTEXT = new JtsSpatialContext(FACTORY);

  public static SpatialContextFactory getFactory() {
    return FACTORY;
  }

  public static SpatialContext getSpatialContext() {
    return SPATIAL_CONTEXT;
  }

  public static double getDoubleValue(final Object param) {
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
   */
  public static Geometry parseJtsGeometry(final Object value) {
    if (value == null)
      return null;
    final String wkt;
    if (value instanceof Shape shape)
      wkt = toWKT(shape);
    else
      wkt = value.toString().trim();
    if (wkt == null || wkt.isEmpty())
      return null;
    try {
      return new WKTReader().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Cannot parse JTS geometry from WKT: " + wkt, e);
    }
  }

  /**
   * Convert a JTS Geometry to WKT string.
   */
  public static String jtsToWKT(final Geometry geometry) {
    if (geometry == null)
      return null;
    return new WKTWriter().write(geometry);
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
