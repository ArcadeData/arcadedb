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

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

/**
 * SQL function geo.distance: computes the Haversine distance between two points.
 * Points may be WKT strings or Spatial4j Shape/Point objects.
 *
 * <p>Usage: {@code geo.distance(<point1>, <point2>[, <unit>])}</p>
 * <p>Unit: "m" (default), "km", "mi", "nmi"</p>
 * <p>Returns: Double distance value</p>
 */
public class SQLFunctionGeoDistance extends SQLFunctionAbstract {
  public static final String NAME = "geo.distance";

  private static final double EARTH_RADIUS_KM = 6371.0;

  public SQLFunctionGeoDistance() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 2 || iParams[0] == null || iParams[1] == null)
      return null;

    final double[] p1 = extractPointCoords(iParams[0]);
    final double[] p2 = extractPointCoords(iParams[1]);

    final String unit = (iParams.length >= 3 && iParams[2] != null) ? iParams[2].toString() : "m";

    final double deltaLat = Math.toRadians(p2[1] - p1[1]);
    final double deltaLon = Math.toRadians(p2[0] - p1[0]);

    final double a = Math.pow(Math.sin(deltaLat / 2), 2)
        + Math.cos(Math.toRadians(p1[1])) * Math.cos(Math.toRadians(p2[1]))
        * Math.pow(Math.sin(deltaLon / 2), 2);

    double distance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS_KM;

    return switch (unit.toLowerCase()) {
      case "km" -> distance;
      case "m" -> distance * 1000;
      case "mi" -> distance * 0.621371192;
      case "nmi" -> distance * 0.539956803;
      default ->
          throw new IllegalArgumentException("Unsupported unit '" + unit + "'. Use m (default), km, mi, nmi.");
    };
  }

  /**
   * Extract [x, y] coordinates from a point given as WKT string, Shape, or Point.
   */
  private double[] extractPointCoords(final Object param) {
    if (param instanceof Point p)
      return new double[] { p.getX(), p.getY() };

    // Parse as geometry and get center point
    final Shape shape = GeoUtils.parseGeometry(param);
    if (shape instanceof Point p)
      return new double[] { p.getX(), p.getY() };

    // Fall back to bounding box center
    final var bbox = shape.getBoundingBox();
    return new double[] {
        (bbox.getMinX() + bbox.getMaxX()) / 2.0,
        (bbox.getMinY() + bbox.getMaxY()) / 2.0
    };
  }

  @Override
  public String getSyntax() {
    return "geo.distance(<point1>, <point2>[, <unit>])";
  }
}
