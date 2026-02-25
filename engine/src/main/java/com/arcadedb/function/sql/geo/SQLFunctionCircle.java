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

/**
 * Deprecated alias for {@code circle()}: returns a circle shape centered at (x, y) with the given radius.
 *
 * <p><b>Deprecated</b>: Use {@code geo.buffer(geo.point(x, y), radius)} instead.</p>
 *
 * @deprecated since 25.x — use {@code geo.buffer(geo.point(x, y), radius)}
 */
@Deprecated
public class SQLFunctionCircle extends SQLFunctionAbstract {
  public static final String NAME = "circle";

  public SQLFunctionCircle() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length != 3)
      throw new IllegalArgumentException("circle() requires 3 parameters: circle(<center-x>, <center-y>, <distance>)");

    final SpatialContext spatialContext = GeoUtils.getSpatialContext();
    return spatialContext.getShapeFactory()
        .circle(GeoUtils.getDoubleValue(params[0]), GeoUtils.getDoubleValue(params[1]), GeoUtils.getDoubleValue(params[2]));
  }

  @Override
  public String getSyntax() {
    return "circle(<center-x>,<center-y>,<distance>) [deprecated: use geo.buffer(geo.point(x,y), radius)]";
  }
}
