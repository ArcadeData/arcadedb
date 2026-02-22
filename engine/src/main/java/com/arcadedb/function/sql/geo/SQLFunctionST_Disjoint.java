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
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * SQL function ST_Disjoint: returns true if the two geometries share no points.
 *
 * <p>Usage: {@code ST_Disjoint(g1, g2)}</p>
 * <p>Returns: Boolean</p>
 */
public class SQLFunctionST_Disjoint extends SQLFunctionST_Predicate {
  public static final String NAME = "ST_Disjoint";

  public SQLFunctionST_Disjoint() {
    super(NAME);
  }

  /**
   * ST_Disjoint cannot use indexed execution: the index returns records that intersect
   * the search shape, but disjoint records are precisely those NOT in the intersection result.
   * Using the index would miss all disjoint records, so we always fall back to full scan.
   */
  @Override
  public boolean allowsIndexedExecution(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    return false;
  }

  @Override
  protected Boolean evaluate(final Shape geom1, final Shape geom2, final Object[] params) {
    return geom1.relate(geom2) == SpatialRelation.DISJOINT;
  }

  @Override
  public String getSyntax() {
    return "ST_Disjoint(<geometry1>, <geometry2>)";
  }
}
