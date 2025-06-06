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
package com.arcadedb.query.sql.function.geo;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.ShapeFactory;

import java.util.*;

/**
 * Returns a polygon shape with the coordinates received as parameters.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionPolygon extends SQLFunctionAbstract {
  public static final String NAME = "polygon";

  public SQLFunctionPolygon() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length != 1)
      throw new IllegalArgumentException("polygon() requires array of points as parameters");

    final SpatialContext spatialContext = GeoUtils.getSpatialContext();

    final List<Object> points = (List<Object>) params[0];

    ShapeFactory.PolygonBuilder polygon = spatialContext.getShapeFactory().polygon();

    for (int i = 0; i < points.size(); i++) {
      final Object point = points.get(i);

      if (point instanceof Point point1)
        polygon.pointXY(point1.getX(), point1.getY());
      else if (point instanceof List list)
        polygon.pointXY(GeoUtils.getDoubleValue(list.getFirst()), GeoUtils.getDoubleValue(list.get(1)));
    }
    return polygon.build();
  }

  public String getSyntax() {
    return "polygon([ <point>* ])";
  }
}
