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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.query.opencypher.temporal.TemporalUtil;

import java.util.List;
import java.util.Map;

/**
 * Comparisons and validation shared by every openCypher write clause (CREATE, MERGE, SET) between a value stored on
 * a record and a value a query supplies.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherValues {
  private CypherValues() {
  }

  /**
   * Numeric-tolerant equality: a stored {@code Integer} 1 equals a supplied {@code Long} 1, which is what makes a
   * MERGE pattern match a record written with a different integral width. A {@code Float} and a {@code Double} may
   * still report unequal after widening (0.1f != 0.1d); that is conservative on purpose, since every caller pays for
   * a false negative with a redundant write or an extra probe, never with a wrong answer.
   */
  public static boolean equalValues(final Object a, final Object b) {
    if (a == null)
      return b == null;
    if (a.equals(b))
      return true;
    if (a instanceof Number numberA && b instanceof Number numberB)
      return numberA.longValue() == numberB.longValue()
          && Double.compare(numberA.doubleValue(), numberB.doubleValue()) == 0;
    return false;
  }

  /**
   * Coerces a property value to its stored Java type and rejects the ones a property cannot hold, matching Neo4j's
   * "Property values can only be of primitive types or arrays thereof". Every openCypher write clause (CREATE, MERGE,
   * SET) funnels its property values through here so a map - or a list containing one - is refused the same way
   * regardless of which clause, or which right-hand-side shape (dot property, {@code +=}/{@code =} map, bare
   * parameter), produced it (issue #7629).
   * <p>
   * A Point value is exempt: Neo4j treats Point as a primitive property type, ArcadeDB just has no dedicated
   * Geometry runtime type yet (issue #4870) and represents one as a map of coordinate keys under the hood, so
   * refusing every map would also reject {@code point()}'s own output. {@link #isPointShaped} recognises one
   * structurally - by the {@code x}/{@code y}/{@code crs} keys every branch of {@code CypherPointFunction} writes,
   * the same keys {@code CypherPointDistanceFunction} and {@code PointWithinBBoxFunction} already key off to read
   * one back - rather than by class identity, because a Point read back off storage is deserialized as a plain
   * {@link Map} (issue #7629): identity would exempt a point() call's immediate result but not a value copied from
   * an already-stored Point property (e.g. {@code MATCH (a) CREATE (b {loc: a.loc})}).
   */
  public static Object coerceAndValidatePropertyValue(final Object value) {
    if (value == null)
      return null; // a null value is a removal (SET) or simply not stored (CREATE/MERGE), not a stored value
    final Object coerced = TemporalUtil.toCoreJavaType(value);
    validatePropertyValue(coerced);
    return coerced;
  }

  private static void validatePropertyValue(final Object value) {
    if (value instanceof List) {
      for (final Object element : (List<?>) value) {
        if (element instanceof Map map && isPointShaped(map))
          continue;
        if (element instanceof Map)
          throw new IllegalArgumentException("TypeError: InvalidPropertyType - Property values can not contain map values");
        if (element instanceof List)
          validatePropertyValue(element);
      }
    } else if (value instanceof Map map && !isPointShaped(map))
      throw new IllegalArgumentException("TypeError: InvalidPropertyType - Property values can not be maps");
  }

  private static boolean isPointShaped(final Map<?, ?> map) {
    return map.containsKey("crs") && map.containsKey("x") && map.containsKey("y");
  }
}
