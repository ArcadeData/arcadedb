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

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;

/**
 * Geospatial utility class.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class GeoUtils {
  static final JtsSpatialContextFactory FACTORY         = new JtsSpatialContextFactory();
  static final JtsSpatialContext        SPATIAL_CONTEXT = new JtsSpatialContext(FACTORY);

  public static SpatialContextFactory getFactory() {
    return FACTORY;
  }

  public static SpatialContext getSpatialContext() {
    return SPATIAL_CONTEXT;
  }

  public static double getDoubleValue(final Object param) {
    return ((Number) param).doubleValue();
  }
}
