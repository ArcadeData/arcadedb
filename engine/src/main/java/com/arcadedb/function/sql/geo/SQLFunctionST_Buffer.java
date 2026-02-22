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
import org.locationtech.jts.geom.Geometry;

/**
 * SQL function ST_Buffer: returns a WKT string of the buffered geometry.
 * Uses JTS Geometry.buffer(distance) for the computation.
 *
 * <p>Usage: {@code ST_Buffer(<geometry>, <distance>)}</p>
 * <p>Returns: WKT string of the buffered shape</p>
 */
public class SQLFunctionST_Buffer extends SQLFunctionAbstract {
  public static final String NAME = "ST_Buffer";

  public SQLFunctionST_Buffer() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 2 || iParams[0] == null || iParams[1] == null)
      return null;

    final Geometry geometry = GeoUtils.parseJtsGeometry(iParams[0]);
    if (geometry == null)
      return null;

    final double distance = GeoUtils.getDoubleValue(iParams[1]);
    final Geometry buffered = geometry.buffer(distance);
    return GeoUtils.jtsToWKT(buffered);
  }

  @Override
  public String getSyntax() {
    return "ST_Buffer(<geometry>, <distance>)";
  }
}
