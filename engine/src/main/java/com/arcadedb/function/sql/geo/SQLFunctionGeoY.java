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
 * SQL function geo.y: returns the Y (latitude) coordinate of a point geometry.
 *
 * <p>Usage: {@code geo.y(<point>)}</p>
 * <p>Returns: Double Y coordinate, or null if input is not a point</p>
 */
public class SQLFunctionGeoY extends SQLFunctionAbstract {
  public static final String NAME = "geo.y";

  public SQLFunctionGeoY() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 1 || iParams[0] == null)
      return null;

    final Object input = iParams[0];

    if (input instanceof Point p)
      return p.getY();

    // Try parsing as geometry
    try {
      final Shape shape = GeoUtils.parseGeometry(input);
      if (shape instanceof Point p)
        return p.getY();
    } catch (Exception ignored) {
      // Not a valid geometry or not a point
    }

    return null;
  }

  @Override
  public String getSyntax() {
    return "geo.y(<point>)";
  }
}
