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
package com.arcadedb.function.geo;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.sql.geo.GeoUtils;
import com.arcadedb.function.sql.geo.LightweightPoint;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Cypher {@code point(lat, lon)} function.
 *
 * <p>Constructs a spatial point from latitude and longitude. Following Cypher/Neo4j convention,
 * the first argument is latitude and the second is longitude. The point is stored internally
 * using the spatial4j convention (x=longitude, y=latitude) so that spatial distance functions
 * such as {@code ST_Distance} operate correctly.</p>
 *
 * <p>Usage: {@code point(<latitude>, <longitude>)}</p>
 */
public class CypherPointFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "point";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args == null || args.length < 2)
      throw new CommandExecutionException("point() requires latitude and longitude as parameters");
    if (args[0] == null || args[1] == null)
      return null;
    final double lat = GeoUtils.getDoubleValue(args[0]);
    final double lon = GeoUtils.getDoubleValue(args[1]);
    // Store as LightweightPoint(x=longitude, y=latitude) per spatial4j convention
    return new LightweightPoint(lon, lat);
  }
}
