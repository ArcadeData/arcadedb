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

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.geo.GeoUtils;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * Base of the SQL methods that answer a spatial relation between the value they are applied to and a shape passed as
 * their parameter, as in {@code coords.isWithin(<shape>)}.
 * <p>
 * Both operands are accepted as a spatial4j {@link Shape}, as WKT text or as a Cypher point map. The deserializer no
 * longer turns a WKT string into a {@code Shape} behind everyone's back (issue #5600), so the conversion happens here,
 * where a geometry is actually asked for - the same contract every {@code geo.*} function follows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class AbstractSQLGeoRelationMethod extends AbstractSQLMethod {
  private final String label;

  /**
   * @param name  the registered method name, lower-case as the parser resolves it
   * @param label the spelling used in error messages, e.g. {@code isWithin}
   */
  protected AbstractSQLGeoRelationMethod(final String name, final String label) {
    super(name, 0, 1);
    this.label = label;
  }

  /**
   * Whether the relation OF THE VALUE to the parameter shape satisfies this method.
   */
  protected abstract boolean matches(SpatialRelation relation);

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (value == null)
      return null;

    if (params.length != 1 || params[0] == null)
      throw new IllegalArgumentException(label + "() requires a shape as parameter");

    // The parameter is written in the query, so a malformed one - `isWithin('POLYGONN (...)')` - is a mistake worth
    // reporting, exactly as a missing one already is. The value comes from the record instead: a row that does not
    // hold a geometry simply does not match, because failing the whole query over one bad row would be worse than
    // filtering it out.
    final Shape shape = GeoUtils.parseGeometry(params[0]);
    if (shape == null)
      return null;

    final Shape target;
    try {
      target = GeoUtils.parseGeometry(value);
    } catch (final IllegalArgumentException e) {
      return null;
    }
    if (target == null)
      return null;

    return matches(target.relate(shape));
  }
}
