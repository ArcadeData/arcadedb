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

import java.util.List;

/**
 * SQL function geo.lineString: constructs a WKT LINESTRING string from a list of coordinate pairs.
 *
 * <p>Usage: {@code geo.lineString([[x1,y1],[x2,y2],...])}</p>
 * <p>Returns: WKT string {@code "LINESTRING (x1 y1, x2 y2, ...)"}</p>
 */
public class SQLFunctionGeoLineString extends SQLFunctionAbstract {
  public static final String NAME = "geo.lineString";

  public SQLFunctionGeoLineString() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 1 || iParams[0] == null)
      return null;

    @SuppressWarnings("unchecked")
    final List<Object> points = (List<Object>) iParams[0];
    if (points.isEmpty())
      return null;

    final StringBuilder sb = new StringBuilder("LINESTRING (");
    for (int i = 0; i < points.size(); i++) {
      if (i > 0)
        sb.append(", ");
      appendCoord(sb, points.get(i));
    }
    sb.append(")");
    return sb.toString();
  }

  private void appendCoord(final StringBuilder sb, final Object point) {
    if (point instanceof Point p) {
      sb.append(GeoUtils.formatCoord(p.getX())).append(" ").append(GeoUtils.formatCoord(p.getY()));
    } else if (point instanceof List<?> list) {
      sb.append(GeoUtils.formatCoord(GeoUtils.getDoubleValue(list.get(0))))
          .append(" ")
          .append(GeoUtils.formatCoord(GeoUtils.getDoubleValue(list.get(1))));
    } else {
      throw new IllegalArgumentException("Invalid point element: " + point);
    }
  }

  @Override
  public String getAlias() {
    return "linestring";
  }

  @Override
  public String getSyntax() {
    return "geo.lineString([[x1,y1],[x2,y2],...])";
  }
}
