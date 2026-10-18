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

import java.util.Locale;

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

  public SQLFunctionGeoDistance() {
    super(NAME);
  }

  /**
   * Two points and an optional unit. The optional third argument is the whole point of declaring this: Cypher's
   * {@code distance()} reaches here through {@code SQLFunctionBridge} and had been registered with the parser as
   * taking exactly two, which rejected every call that named a unit once the declaration became a gate (#5484).
   */
  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 2 || iParams[0] == null || iParams[1] == null)
      return null;

    final Shape shape1 = toShape(iParams[0]);
    final Shape shape2 = toShape(iParams[1]);

    final String unit = iParams.length >= 3 && iParams[2] != null ? iParams[2].toString() : "m";

    final double distance;
    if (shape1 instanceof Point p1 && shape2 instanceof Point p2)
      distance = GeoUtils.haversineRadians(p1.getX(), p1.getY(), p2.getX(), p2.getY()) * GeoUtils.EARTH_RADIUS_KM;
    else
      // between the closest points of the shapes, 0 when they intersect, rather than between the centres of their
      // bounding boxes (issue #10210)
      distance = Math.toRadians(GeoUtils.distanceDegrees(shape1, shape2)) * GeoUtils.EARTH_RADIUS_KM;

    return switch (unit.toLowerCase(Locale.ROOT)) {
      case "km" -> distance;
      case "m" -> distance * 1000;
      case "mi" -> distance * 0.621371192;
      case "nmi" -> distance * 0.539956803;
      default ->
          throw new IllegalArgumentException("Unsupported unit '" + unit + "'. Use m (default), km, mi, nmi.");
    };
  }

  /**
   * The shape of an argument: a Shape, a WKT string or a Cypher point map.
   */
  private static Shape toShape(final Object param) {
    final Shape shape = GeoUtils.parseGeometry(param);
    if (shape == null)
      throw new IllegalArgumentException("Cannot parse geometry from: " + param);
    return shape;
  }

  @Override
  public String getAlias() {
    return "distance";
  }

  @Override
  public String getSyntax() {
    return "geo.distance(<point1>, <point2>[, <unit>])";
  }
}
