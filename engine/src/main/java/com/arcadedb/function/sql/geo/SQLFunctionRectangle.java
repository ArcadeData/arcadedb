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

/**
 * Deprecated alias for {@code rectangle()}: returns a rectangle shape from two corner coordinates,
 * {@code rectangle(<west-x>, <y1>, <east-x>, <y2>)}. The two latitudes are read in either order, so the
 * {@code (left-x, top-y, right-x, bottom-y)} the documentation gives and the {@code (minX, minY, maxX, maxY)} the
 * implementation read both make the same box; {@code (left, top, right, bottom)} used to fail with "maxY must be >= minY"
 * (issue #12277). The longitudes keep their order: a west x greater than the east one crosses the antimeridian.
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
    final double y1 = GeoUtils.getDoubleValue(params[1]);
    final double y2 = GeoUtils.getDoubleValue(params[3]);
    try {
      return spatialContext.getShapeFactory()
          .rect(GeoUtils.getDoubleValue(params[0]), GeoUtils.getDoubleValue(params[2]), Math.min(y1, y2), Math.max(y1, y2));
    } catch (final InvalidShapeException e) {
      // an invalid rectangle (e.g. a latitude outside [-90, 90]) is a client error, not a raw spatial4j exception (issue #12744)
      throw new IllegalArgumentException(NAME + "() " + e.getMessage(), e);
    }
  }

  @Override
  public String getSyntax() {
    return "rectangle(<west-x>,<y1>,<east-x>,<y2>) [deprecated: use geo.geomFromText with POLYGON WKT]";
  }
}
