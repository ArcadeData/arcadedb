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

import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;

/**
 * SQL function ST_Touches: returns true if the geometries have at least one point in common
 * but their interiors do not intersect.
 * Uses JTS for this DE-9IM predicate.
 *
 * <p>Usage: {@code ST_Touches(g1, g2)}</p>
 * <p>Returns: Boolean</p>
 */
public class SQLFunctionST_Touches extends SQLFunctionST_Predicate {
  public static final String NAME = "ST_Touches";

  public SQLFunctionST_Touches() {
    super(NAME);
  }

  @Override
  protected Boolean evaluate(final Shape geom1, final Shape geom2, final Object[] params) {
    final Geometry jts1 = GeoUtils.parseJtsGeometry(geom1);
    final Geometry jts2 = GeoUtils.parseJtsGeometry(geom2);
    if (jts1 == null || jts2 == null)
      return null;
    return jts1.touches(jts2);
  }

  @Override
  public String getSyntax() {
    return "ST_Touches(<geometry1>, <geometry2>)";
  }
}
