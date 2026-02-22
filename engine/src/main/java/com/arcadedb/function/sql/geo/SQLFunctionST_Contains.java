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
 * SQL function ST_Contains: returns true if geometry g fully contains shape.
 *
 * <p>Usage: {@code ST_Contains(g, shape)}</p>
 * <p>Returns: Boolean</p>
 */
public class SQLFunctionST_Contains extends SQLFunctionST_Predicate {
  public static final String NAME = "ST_Contains";

  public SQLFunctionST_Contains() {
    super(NAME);
  }

  /**
   * ST_Contains cannot use indexed execution: the stored geometry is the container and the query
   * argument is the containee. The GeoHash index is built on the stored shape, but containment
   * queries run in the opposite direction — the index cannot serve as a valid candidate superset
   * for containment queries from that reversed direction.
   */
  @Override
  public boolean allowsIndexedExecution(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    return false;
  }

  @Override
  protected Boolean evaluate(final Shape geom1, final Shape geom2, final Object[] params) {
    return geom1.relate(geom2) == SpatialRelation.CONTAINS;
  }

  @Override
  public String getSyntax() {
    return "ST_Contains(<geometry>, <shape>)";
  }
}
