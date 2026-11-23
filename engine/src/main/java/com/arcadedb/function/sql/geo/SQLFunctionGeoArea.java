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
import org.locationtech.spatial4j.shape.Shape;

/**
 * SQL function geo.area: returns the area of a geometry in square degrees, measured on the longitude/latitude plane the
 * predicates read a shape on (OGC WKT, PostGIS {@code geometry}, Shapely): a 10 x 10 degree square is 100 wherever it lies.
 * <p>
 * It used to return Spatial4j's geodetic estimate, the spherical area of the bounding box scaled by the planar fraction of
 * the box the shape fills, which is neither unit: two triangles mirrored north/south got the same answer although one is six
 * times larger on the sphere, and a MULTIPOLYGON was not the sum of its parts (issue #12272).
 *
 * <p>Usage: {@code geo.area(<geometry>)}</p>
 * <p>Returns: Double area value in square degrees</p>
 */
public class SQLFunctionGeoArea extends SQLFunctionAbstract {
  public static final String NAME = "geo.area";

  public SQLFunctionGeoArea() {
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

    // a null context is Spatial4j's planar area: JTS's for a geometry, width x height for a rectangle (the width taken
    // across the antimeridian when it crosses it), pi r^2 for a circle
    return shape.getArea(null);
  }

  @Override
  public String getSyntax() {
    return "geo.area(<geometry>)";
  }
}
