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
package com.arcadedb.query.opencypher.temporal;

/**
 * Common interface for all OpenCypher temporal value types.
 * Temporal values support property access (e.g., date.year, time.hour)
 * and can be compared and used in arithmetic with durations.
 */
public interface CypherTemporalValue extends Comparable<CypherTemporalValue> {

  /**
   * Access a named property of this temporal value.
   * For example, date.year, time.hour, duration.months, etc.
   */
  Object getTemporalProperty(String name);

  /**
   * The {@code java.time} value a Cypher date, time or datetime wraps, or {@code value} itself for anything else. The one conversion
   * the wire protocols (Bolt, gRPC, Postgres) apply before they encode a value, so a property read through Cypher is sent as the
   * same type as through SQL (issue #11955). A {@link CypherDuration} is returned as is: no single {@code java.time} type holds
   * its months, days and seconds, and each protocol has its own encoding for it.
   */
  static Object toJavaTime(final Object value) {
    // a single check for the common case: the wire protocols call this for every cell they encode
    if (!(value instanceof CypherTemporalValue))
      return value;

    return switch (value) {
      case CypherDate date -> date.getValue();
      case CypherLocalDateTime localDateTime -> localDateTime.getValue();
      case CypherDateTime dateTime -> dateTime.getValue();
      case CypherLocalTime localTime -> localTime.getValue();
      case CypherTime time -> time.getValue();
      default -> value;
    };
  }
}
