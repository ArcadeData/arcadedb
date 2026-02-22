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
 * SQL function ST_AsText: returns the WKT representation of a geometry.
 * If the input is already a WKT string, it is returned as-is.
 * If the input is a Shape object, it is converted to WKT.
 *
 * <p>Usage: {@code ST_AsText(<geometry>)}</p>
 * <p>Returns: WKT string</p>
 */
public class SQLFunctionST_AsText extends SQLFunctionAbstract {
  public static final String NAME = "ST_AsText";

  public SQLFunctionST_AsText() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 1 || iParams[0] == null)
      return null;

    final Object input = iParams[0];
    if (input instanceof String str)
      return str;

    if (input instanceof Shape shape)
      return GeoUtils.toWKT(shape);

    // Try to parse then convert
    final Shape shape = GeoUtils.parseGeometry(input);
    return GeoUtils.toWKT(shape);
  }

  @Override
  public String getSyntax() {
    return "ST_AsText(<geometry>)";
  }
}
