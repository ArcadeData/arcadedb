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

import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * SQL function ST_Intersects: returns true if the two geometries share any point.
 *
 * <p>Usage: {@code ST_Intersects(g1, g2)}</p>
 * <p>Returns: Boolean</p>
 */
public class SQLFunctionST_Intersects extends SQLFunctionGeoPredicate {
  public static final String NAME = "ST_Intersects";

  public SQLFunctionST_Intersects() {
    super(NAME);
  }

  @Override
  protected Boolean evaluate(final Shape geom1, final Shape geom2, final Object[] params) {
    return geom1.relate(geom2) != SpatialRelation.DISJOINT;
  }

  @Override
  public String getSyntax() {
    return "ST_Intersects(<geometry1>, <geometry2>)";
  }
}
