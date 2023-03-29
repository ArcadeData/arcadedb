/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.query.sql.method.geo;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.misc.AbstractSQLMethod;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * Returns true if a shape intersects with another shape.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodIntersectsWith extends AbstractSQLMethod {

  public static final String NAME = "intersectswith";

  public SQLMethodIntersectsWith() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "intersectsWith( <shape> )";
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final CommandContext context, final Object ioResult, final Object[] iParams) {
    if (iThis == null)
      return null;
    else if (!(iThis instanceof Shape))
      return null;

    if (iParams.length != 1 || iParams[0] == null)
      throw new IllegalArgumentException("intersectsWith() requires a shape as parameter");

    final Shape shape = (Shape) iParams[0];

    return ((Shape) iThis).relate(shape) != SpatialRelation.DISJOINT;
  }
}
