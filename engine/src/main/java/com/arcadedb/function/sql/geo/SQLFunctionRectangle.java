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
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Point;

/**
 * Deprecated alias for {@code rectangle()}: returns a rectangle shape from two corner coordinates.
 *
 * <p><b>Deprecated</b>: Use {@code geo.geomFromText("POLYGON ((x1 y1, x2 y1, x2 y2, x1 y2, x1 y1))")} instead.</p>
 *
 * @deprecated since 25.x — use {@code geo.geomFromText} with an explicit POLYGON WKT
 */
@Deprecated
public class SQLFunctionRectangle extends SQLFunctionAbstract {
  public static final String NAME = "rectangle";

  public SQLFunctionRectangle() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 4;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    checkArity(params);

    final SpatialContext spatialContext = GeoUtils.getSpatialContext();
    final double topY = GeoUtils.getDoubleValue(params[1]);
    final double bottomY = GeoUtils.getDoubleValue(params[3]);
    try {
      // spatial4j wants the lower-left and the upper-right corner (minY <= maxY), while the arguments are named top and bottom: a
      // top-left and a bottom-right corner, the natural order, has the larger Y first and was refused. Y is ordered here; X is not,
      // as a minX past maxX is a rectangle across the antimeridian (issue #12744)
      final Point lowerLeft = spatialContext.getShapeFactory().pointXY(GeoUtils.getDoubleValue(params[0]), Math.min(topY, bottomY));
      final Point upperRight = spatialContext.getShapeFactory().pointXY(GeoUtils.getDoubleValue(params[2]), Math.max(topY, bottomY));
      return spatialContext.getShapeFactory().rect(lowerLeft, upperRight);
    } catch (final InvalidShapeException e) {
      throw new IllegalArgumentException(NAME + "() " + e.getMessage(), e);
    }
  }

  @Override
  public String getSyntax() {
    return "rectangle(<top-x>,<top-y>,<bottom-x>,<bottom-y>) [deprecated: use geo.geomFromText with POLYGON WKT]";
  }
}
