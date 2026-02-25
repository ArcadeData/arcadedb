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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;

/**
 * SQL function geo.overlaps: returns true if the two geometries overlap (share some but not all points,
 * and have the same dimension).
 * Uses JTS for this DE-9IM predicate.
 *
 * <p>Usage: {@code geo.overlaps(g1, g2)}</p>
 * <p>Returns: Boolean</p>
 */
public class SQLFunctionGeoOverlaps extends SQLFunctionGeoPredicate {
  public static final String NAME = "geo.overlaps";

  public SQLFunctionGeoOverlaps() {
    super(NAME);
  }

  /**
   * geo.overlaps cannot use indexed execution: overlapping is a DE-9IM predicate requiring
   * geometries of the same dimension to share some but not all interior points. Bounding-box
   * intersection (which the GeoHash index evaluates) is not a valid candidate superset for
   * DE-9IM overlapping, so the index would produce incorrect results.
   */
  @Override
  public boolean allowsIndexedExecution(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    return false;
  }

  @Override
  protected Boolean evaluate(final Shape geom1, final Shape geom2, final Object[] params) {
    final Geometry jts1 = GeoUtils.parseJtsGeometry(geom1);
    final Geometry jts2 = GeoUtils.parseJtsGeometry(geom2);
    if (jts1 == null || jts2 == null)
      return null;
    return jts1.overlaps(jts2);
  }

  @Override
  public String getSyntax() {
    return "geo.overlaps(<geometry1>, <geometry2>)";
  }
}
