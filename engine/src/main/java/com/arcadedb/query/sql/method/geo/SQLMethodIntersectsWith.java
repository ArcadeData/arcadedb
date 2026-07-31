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
package com.arcadedb.query.sql.method.geo;

import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * Returns true if a shape intersects with another shape.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodIntersectsWith extends AbstractSQLGeoRelationMethod {

  public static final String NAME = "intersectswith";

  public SQLMethodIntersectsWith() {
    super(NAME, "intersectsWith");
  }

  @Override
  protected boolean matches(final SpatialRelation relation) {
    return relation != SpatialRelation.DISJOINT;
  }

  @Override
  public String getSyntax() {
    return "intersectsWith( <shape> )";
  }
}
