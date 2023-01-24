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
import org.locationtech.spatial4j.context.SpatialContext;

/**
 * Returns a circle shape with the 3 coordinates received as parameters.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionCircle extends SQLFunctionAbstract {
  public static final String NAME = "circle";

  public SQLFunctionCircle() {
    super(NAME);
  }

  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      final CommandContext iContext) {
    if (iParams.length != 3)
      throw new IllegalArgumentException("circle() requires 3 parameters");

    final SpatialContext spatialContext = GeoUtils.getSpatialContext();
    return spatialContext.getShapeFactory()
        .circle(GeoUtils.getDoubleValue(iParams[0]), GeoUtils.getDoubleValue(iParams[1]), GeoUtils.getDoubleValue(iParams[2]));
  }

  public String getSyntax() {
    return "circle(<center-x>,<center-y>,<distance>)";
  }
}
