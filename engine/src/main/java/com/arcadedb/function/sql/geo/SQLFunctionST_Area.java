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
import org.locationtech.spatial4j.shape.Shape;

/**
 * SQL function ST_Area: returns the area of a geometry in square degrees.
 *
 * <p>Usage: {@code ST_Area(<geometry>)}</p>
 * <p>Returns: Double area value in square degrees</p>
 */
public class SQLFunctionST_Area extends SQLFunctionAbstract {
  public static final String NAME = "ST_Area";

  public SQLFunctionST_Area() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 1 || iParams[0] == null)
      return null;

    final Shape shape = GeoUtils.parseGeometry(iParams[0]);
    if (shape == null)
      return null;

    final SpatialContext ctx = GeoUtils.getSpatialContext();
    return shape.getArea(ctx);
  }

  @Override
  public String getSyntax() {
    return "ST_Area(<geometry>)";
  }
}
