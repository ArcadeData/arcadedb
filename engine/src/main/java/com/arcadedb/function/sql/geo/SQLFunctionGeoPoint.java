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

/**
 * SQL function geo.point: constructs a WKT POINT string from X (longitude) and Y (latitude).
 *
 * <p>Usage: {@code geo.point(<x>, <y>)}</p>
 * <p>Returns: WKT string {@code "POINT (x y)"}</p>
 */
public class SQLFunctionGeoPoint extends SQLFunctionAbstract {
  public static final String NAME = "geo.point";

  public SQLFunctionGeoPoint() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 2 || iParams[0] == null || iParams[1] == null)
      return null;
    final double x = GeoUtils.getDoubleValue(iParams[0]);
    final double y = GeoUtils.getDoubleValue(iParams[1]);
    return "POINT (" + GeoUtils.formatCoord(x) + " " + GeoUtils.formatCoord(y) + ")";
  }

  @Override
  public String getSyntax() {
    return "geo.point(<x>, <y>)";
  }
}
