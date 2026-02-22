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
 * SQL function ST_Equals: returns true if the two geometries are geometrically equal.
 * Uses JTS geometric equality (structural equivalence after normalisation).
 *
 * <p>Usage: {@code ST_Equals(g1, g2)}</p>
 * <p>Returns: Boolean</p>
 */
public class SQLFunctionST_Equals extends SQLFunctionST_Predicate {
  public static final String NAME = "ST_Equals";

  public SQLFunctionST_Equals() {
    super(NAME);
  }

  /**
   * ST_Equals cannot use indexed execution: geometric equality requires an exact coordinate match.
   * The GeoHash index returns all records whose bounding box intersects the search shape, which is
   * a much coarser superset than exact equality — the index cannot guarantee correctness here.
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
    return jts1.norm().equals(jts2.norm());
  }

  @Override
  public String getSyntax() {
    return "ST_Equals(<geometry1>, <geometry2>)";
  }
}
