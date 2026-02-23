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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import org.locationtech.spatial4j.shape.Shape;

/**
 * SQL function ST_DWithin: returns true if geometry g is within the given distance of shape.
 * Distance is specified in degrees (consistent with Spatial4j's coordinate system).
 *
 * <p>Usage: {@code ST_DWithin(g, shape, distanceDegrees)}</p>
 * <p>Returns: Boolean</p>
 */
public class SQLFunctionST_DWithin extends SQLFunctionGeoPredicate {
  public static final String NAME = "ST_DWithin";

  public SQLFunctionST_DWithin() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  /**
   * ST_DWithin uses a radius distance check against the centers of the two geometries.
   * The geospatial index returns records based on geohash intersection, which does not
   * directly correspond to the distance radius. To guarantee correctness, indexed
   * execution is disabled and the predicate is evaluated inline on all records.
   */
  @Override
  public boolean allowsIndexedExecution(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    return false;
  }

  @Override
  protected Boolean evaluate(final Shape geom1, final Shape geom2, final Object[] params) {
    if (params.length < 3 || params[2] == null)
      return null;
    final double distance = GeoUtils.getDoubleValue(params[2]);
    final double actualDistance = GeoUtils.getSpatialContext()
        .calcDistance(geom1.getCenter(), geom2.getCenter());
    return actualDistance <= distance;
  }

  @Override
  public String getSyntax() {
    return "ST_DWithin(<geometry>, <shape>, <distanceDegrees>)";
  }
}
