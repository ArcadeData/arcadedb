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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.locationtech.jts.operation.buffer.BufferParameters;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SQL function geo.buffer: returns a WKT string of the buffered geometry.
 * Uses JTS Geometry.buffer(distance) for the computation.
 *
 * <p>Usage: {@code geo.buffer(<geometry>, <distance>)}</p>
 * <p>Returns: WKT string of the buffered shape</p>
 */
public class SQLFunctionGeoBuffer extends SQLFunctionAbstract {
  public static final String NAME = "geo.buffer";

  private static final Set<String> OPTIONS = Set.of("quadrantSegments", "endCapStyle", "joinStyle");

  public SQLFunctionGeoBuffer() {
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

    final Geometry buffered;
    if (iParams.length >= 3 && iParams[2] instanceof Map<?, ?> rawMap) {
      final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTIONS);
      final BufferParameters bufferParams = new BufferParameters();
      if (opts.containsKey("quadrantSegments"))
        bufferParams.setQuadrantSegments(opts.getInt("quadrantSegments", BufferParameters.DEFAULT_QUADRANT_SEGMENTS));
      if (opts.containsKey("endCapStyle"))
        bufferParams.setEndCapStyle(parseEndCapStyle(opts.getString("endCapStyle", "ROUND")));
      if (opts.containsKey("joinStyle"))
        bufferParams.setJoinStyle(parseJoinStyle(opts.getString("joinStyle", "ROUND")));
      buffered = BufferOp.bufferOp(geometry, distance, bufferParams);
    } else {
      buffered = geometry.buffer(distance);
    }
    return GeoUtils.jtsToWKT(buffered);
  }

  private static int parseEndCapStyle(final String s) {
    return switch (s.toUpperCase(Locale.ENGLISH)) {
      case "ROUND" -> BufferParameters.CAP_ROUND;
      case "FLAT" -> BufferParameters.CAP_FLAT;
      case "SQUARE" -> BufferParameters.CAP_SQUARE;
      default -> throw new CommandSQLParsingException(
          "Option 'endCapStyle' for function 'geo.buffer' must be ROUND, FLAT, or SQUARE, got: " + s);
    };
  }

  private static int parseJoinStyle(final String s) {
    return switch (s.toUpperCase(Locale.ENGLISH)) {
      case "ROUND" -> BufferParameters.JOIN_ROUND;
      case "MITRE", "MITER" -> BufferParameters.JOIN_MITRE;
      case "BEVEL" -> BufferParameters.JOIN_BEVEL;
      default -> throw new CommandSQLParsingException(
          "Option 'joinStyle' for function 'geo.buffer' must be ROUND, MITRE, or BEVEL, got: " + s);
    };
  }

  @Override
  public String getSyntax() {
    return "geo.buffer(<geometry>, <distance>"
        + " [, { quadrantSegments: <int>, endCapStyle: 'ROUND|FLAT|SQUARE', joinStyle: 'ROUND|MITRE|BEVEL' }])";
  }
}
