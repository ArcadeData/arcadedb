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

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * Lightweight Point implementation for fast serialization.
 * This class is optimized for bulk insert operations where we only need
 * to store x,y coordinates without the full overhead of spatial4j Point.
 *
 * For read operations or complex geospatial queries, this will be converted
 * to a full spatial4j Point object.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class LightweightPoint implements Point {
  private final double x;
  private final double y;

  public LightweightPoint(final double x, final double y) {
    this.x = x;
    this.y = y;
  }

  @Override
  public double getX() {
    return x;
  }

  @Override
  public double getY() {
    return y;
  }

  @Override
  public void reset(final double x, final double y) {
    throw new UnsupportedOperationException("LightweightPoint is immutable");
  }

  @Override
  public SpatialContext getContext() {
    return GeoUtils.getSpatialContext();
  }

  @Override
  public Rectangle getBoundingBox() {
    // Lazy conversion to full Point only when needed
    return toSpatial4jPoint().getBoundingBox();
  }

  @Override
  public boolean hasArea() {
    return false;
  }

  @Override
  public double getArea(final SpatialContext ctx) {
    return 0;
  }

  @Override
  public Point getCenter() {
    return this;
  }

  @Override
  public Shape getBuffered(final double distance, final SpatialContext ctx) {
    // Lazy conversion to full Point only when needed
    return toSpatial4jPoint().getBuffered(distance, ctx);
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public SpatialRelation relate(final Shape other) {
    // Lazy conversion to full Point only when needed
    return toSpatial4jPoint().relate(other);
  }

  /**
   * Convert to full spatial4j Point when complex operations are needed.
   * This is only called for read operations or complex geospatial queries.
   */
  private Point toSpatial4jPoint() {
    return GeoUtils.getSpatialContext().getShapeFactory().pointXY(x, y);
  }

  @Override
  public String toString() {
    return "Pt(x=" + x + ",y=" + y + ")";
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof Point other))
      return false;
    return Double.compare(other.getX(), x) == 0 && Double.compare(other.getY(), y) == 0;
  }

  @Override
  public int hashCode() {
    long temp = Double.doubleToLongBits(x);
    int result = (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(y);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }
}
