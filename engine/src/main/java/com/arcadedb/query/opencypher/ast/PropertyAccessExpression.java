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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.database.Document;
import com.arcadedb.query.opencypher.temporal.*;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Expression representing property access on a variable.
 * Example: n.name, person.age
 */
public class PropertyAccessExpression implements Expression {
  private final String variableName;
  private final String propertyName;

  public PropertyAccessExpression(final String variableName, final String propertyName) {
    this.variableName = variableName;
    this.propertyName = propertyName;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Object variable = result.getProperty(variableName);
    if (variable instanceof Document) {
      final Object rawValue = ((Document) variable).get(propertyName);
      return convertFromStorage(rawValue);
    } else if (variable instanceof Map) {
      // Handle Map types (e.g., from UNWIND with parameter maps)
      return ((Map<?, ?>) variable).get(propertyName);
    } else if (variable instanceof Result) {
      // Handle Result types (nested results)
      return ((Result) variable).getProperty(propertyName);
    } else if (variable instanceof CypherTemporalValue) {
      // Handle temporal value property access (e.g., date.year, time.hour)
      return ((CypherTemporalValue) variable).getTemporalProperty(propertyName);
    } else if (variable instanceof LocalDate) {
      // java.time.LocalDate stored in ArcadeDB → wrap in CypherDate for property access
      return new CypherDate((LocalDate) variable).getTemporalProperty(propertyName);
    } else if (variable instanceof LocalDateTime) {
      // java.time.LocalDateTime stored in ArcadeDB → wrap in CypherLocalDateTime for property access
      return new CypherLocalDateTime((LocalDateTime) variable).getTemporalProperty(propertyName);
    }
    return null;
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return variableName + "." + propertyName;
  }

  public String getVariableName() {
    return variableName;
  }

  public String getPropertyName() {
    return propertyName;
  }

  /**
   * Convert ArcadeDB-stored values back to Cypher temporal types.
   * Duration, LocalTime, and Time are stored as Strings because ArcadeDB
   * doesn't have native binary types for them.
   */
  private static Object convertFromStorage(final Object value) {
    // Handle collections (lists/arrays of temporal values)
    if (value instanceof java.util.Collection<?> collection) {
      final java.util.List<Object> converted = new java.util.ArrayList<>(collection.size());
      for (final Object item : collection) {
        converted.add(convertFromStorage(item));
      }
      return converted;
    }
    if (value instanceof Object[] array) {
      final Object[] converted = new Object[array.length];
      for (int i = 0; i < array.length; i++) {
        converted[i] = convertFromStorage(array[i]);
      }
      return converted;
    }

    // Handle single values
    if (value instanceof LocalDate ld)
      return new CypherDate(ld);
    if (value instanceof LocalDateTime ldt)
      return new CypherLocalDateTime(ldt);
    if (value instanceof String str) {
      // Duration strings start with P (ISO-8601)
      if (str.length() > 1 && str.charAt(0) == 'P') {
        try {
          return CypherDuration.parse(str);
        } catch (final Exception ignored) {
          // Not a valid duration string
        }
      }
      // DateTime strings: contain 'T' with date part before it and timezone/offset
      // e.g., 1912-01-01T00:00Z, 1984-10-11T12:31:14+01:00[Europe/Stockholm]
      final int tIdx = str.indexOf('T');
      if (tIdx >= 4 && tIdx < str.length() - 1 && Character.isDigit(str.charAt(0))) {
        try {
          return CypherDateTime.parse(str);
        } catch (final Exception ignored) {
          // Not a valid datetime string
        }
      }
      // Time strings: HH:MM:SS[.nanos][+/-offset] or HH:MM:SS[.nanos]Z
      if (str.length() >= 8 && str.charAt(2) == ':' && str.charAt(5) == ':') {
        // Check if it has a timezone offset
        final boolean hasOffset = str.contains("+") || str.contains("-") || str.endsWith("Z");
        if (hasOffset) {
          try {
            return CypherTime.parse(str);
          } catch (final Exception ignored) {
            // Not a valid time string
          }
        } else {
          try {
            return CypherLocalTime.parse(str);
          } catch (final Exception ignored) {
            // Not a valid local time string
          }
        }
      }
    }
    return value;
  }
}
