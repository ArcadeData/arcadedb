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
package com.arcadedb.query.sql.function.geo;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.schema.Type;

/**
 * Haversine formula to compute the distance between 2 gro points.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionDistance extends SQLFunctionAbstract {
  public static final String NAME = "distance";

  private final static double EARTH_RADIUS = 6371;

  public SQLFunctionDistance() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 2)
      throw new IllegalArgumentException("distance() requires at least 2 parameters");

    double distance;
    final double[] values = new double[4];
    String unit = "km";
    boolean cypherStyle = false;

    // Support two forms:
    // 1. Cypher/Neo4j style: distance(point1, point2) or distance(point1, point2, unit)
    //    - Returns distance in meters by default (Neo4j compatibility)
    // 2. SQL style: distance(x1, y1, x2, y2) or distance(x1, y1, x2, y2, unit)
    //    - Returns distance in kilometers by default (backward compatibility)

    if (params.length == 2 || (params.length == 3 && !(params[2] instanceof Number))) {
      // Cypher/Neo4j style: distance(point1, point2) or distance(point1, point2, unit)
      cypherStyle = true;
      extractCoordinatesFromPoint(params[0], values, 0);
      extractCoordinatesFromPoint(params[1], values, 2);

      if (params.length == 3)
        unit = params[2].toString();
      else
        unit = "m"; // Neo4j default is meters
    } else {
      // SQL style: distance(x1, y1, x2, y2) or distance(x1, y1, x2, y2, unit)
      for (int i = 0; i < params.length && i < 4; ++i) {
        if (params[i] == null)
          return null;

        values[i] = (Double) Type.convert(context.getDatabase(), params[i], Double.class);
      }

      if (params.length > 4)
        unit = params[4].toString();
    }

    final double deltaLat = Math.toRadians(values[2] - values[0]);
    final double deltaLon = Math.toRadians(values[3] - values[1]);

    final double a =
        Math.pow(Math.sin(deltaLat / 2), 2) + Math.cos(Math.toRadians(values[0])) * Math.cos(Math.toRadians(values[2])) * Math.pow(Math.sin(deltaLon / 2), 2);
    distance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS;

    // Apply unit conversion
    if (unit.equalsIgnoreCase("km"))
      // ALREADY IN KM
      ;
    else if (unit.equalsIgnoreCase("m"))
      // METERS
      distance *= 1000;
    else if (unit.equalsIgnoreCase("mi"))
      // MILES
      distance *= 0.621371192;
    else if (unit.equalsIgnoreCase("nmi"))
      // NAUTICAL MILES
      distance *= 0.539956803;
    else
      throw new IllegalArgumentException("Unsupported unit '" + unit + "'. Use m, km, mi and nmi. Default is " + (cypherStyle ? "m (meters)" : "km (kilometers)"));

    return distance;
  }

  /**
   * Extract X and Y coordinates from a point parameter.
   * Handles Spatial4j Point objects and WKT strings like "POINT (x y)".
   */
  private void extractCoordinatesFromPoint(final Object param, final double[] values, final int offset) {
    if (param == null)
      throw new IllegalArgumentException("Point parameter cannot be null");

    // Check if it's a Spatial4j Point object
    try {
      final Class<?> pointClass = Class.forName("org.locationtech.spatial4j.shape.Point");
      if (pointClass.isInstance(param)) {
        final java.lang.reflect.Method getXMethod = pointClass.getMethod("getX");
        final java.lang.reflect.Method getYMethod = pointClass.getMethod("getY");
        values[offset] = (Double) getXMethod.invoke(param);
        values[offset + 1] = (Double) getYMethod.invoke(param);
        return;
      }
    } catch (Exception e) {
      // Spatial4j not available or not a Point, continue to string parsing
    }

    // Try to parse as WKT string: "POINT (x y)" or "Pt(x=...,y=...)"
    final String str = param.toString();

    // Handle WKT format: "POINT (x y)"
    if (str.startsWith("POINT (") && str.endsWith(")")) {
      final String coords = str.substring(7, str.length() - 1).trim();
      final String[] parts = coords.split("\\s+");
      if (parts.length >= 2) {
        values[offset] = Double.parseDouble(parts[0]);
        values[offset + 1] = Double.parseDouble(parts[1]);
        return;
      }
    }

    // Handle internal format: "Pt(x=...,y=...)"
    if (str.startsWith("Pt(") && str.endsWith(")")) {
      final String coords = str.substring(3, str.length() - 1);
      final String[] parts = coords.split(",");
      if (parts.length >= 2) {
        for (String part : parts) {
          part = part.trim();
          if (part.startsWith("x="))
            values[offset] = Double.parseDouble(part.substring(2));
          else if (part.startsWith("y="))
            values[offset + 1] = Double.parseDouble(part.substring(2));
        }
        return;
      }
    }

    throw new IllegalArgumentException("Cannot extract coordinates from point parameter: " + str);
  }

  public String getSyntax() {
    return "distance(<point1>,<point2>[,<unit>]) or distance(<x1>,<y1>,<x2>,<y2>[,<unit>])";
  }
}
