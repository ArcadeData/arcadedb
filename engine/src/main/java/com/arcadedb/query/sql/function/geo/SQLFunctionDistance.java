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
import com.arcadedb.schema.Type;

/**
 * Haversine formula to compute the distance between 2 gro points.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionDistance extends SQLFunctionAbstract {
  public static final String NAME = "distance";

  private final static double EARTH_RADIUS = 6371;

  public SQLFunctionDistance() {
    super(NAME);
  }

  public Object execute( final Object iThis, final Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, CommandContext iContext) {
    double distance;

    final double[] values = new double[4];

    for (int i = 0; i < iParams.length && i < 4; ++i) {
      if (iParams[i] == null)
        return null;

      values[i] = (Double) Type.convert(iContext.getDatabase(), iParams[i], Double.class);
    }

    final double deltaLat = Math.toRadians(values[2] - values[0]);
    final double deltaLon = Math.toRadians(values[3] - values[1]);

    final double a =
        Math.pow(Math.sin(deltaLat / 2), 2) + Math.cos(Math.toRadians(values[0])) * Math.cos(Math.toRadians(values[2])) * Math
            .pow(Math.sin(deltaLon / 2), 2);
    distance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS;

    if (iParams.length > 4) {
      final String unit = iParams[4].toString();
      if (unit.equalsIgnoreCase("km"))
        // ALREADY IN KM
        ;
      else if (unit.equalsIgnoreCase("mi"))
        // MILES
        distance *= 0.621371192;
      else if (unit.equalsIgnoreCase("nmi"))
        // NAUTICAL MILES
        distance *= 0.539956803;
      else
        throw new IllegalArgumentException("Unsupported unit '" + unit + "'. Use km, mi and nmi. Default is km.");
    }

    return distance;
  }

  public String getSyntax() {
    return "distance(<field-x>,<field-y>,<x-value>,<y-value>[,<unit>])";
  }
}
