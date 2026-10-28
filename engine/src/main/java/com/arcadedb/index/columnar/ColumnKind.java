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
package com.arcadedb.index.columnar;

import com.arcadedb.index.IndexException;
import com.arcadedb.schema.Type;

/** How a COLUMNAR index stores one indexed property, decided from the property type and the BY ITEM modifier. */
public enum ColumnKind {
  /** One dictionary ordinal per record (STRING, BOOLEAN, LINK). */
  ORDINAL,
  /** A sorted, deduplicated set of dictionary ordinals per record (a LIST or ARRAY_OF_* indexed BY ITEM). */
  ORDINAL_LIST,
  /** One 64-bit integer per record: integral numbers, dates and date-times as epoch counts. */
  LONG,
  /** One double per record (FLOAT, DOUBLE). */
  DOUBLE;

  /**
   * @throws IndexException when the property type cannot be projected (DECIMAL, MAP, EMBEDDED, BINARY, a LIST without BY ITEM, BY ITEM on
   *                        a type that is not a list or array, and any other type)
   */
  public static ColumnKind of(final Type propertyType, final boolean byItem) {
    if (byItem) {
      return switch (propertyType) {
        case LIST, ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS, ARRAY_OF_LONGS, ARRAY_OF_FLOATS, ARRAY_OF_DOUBLES -> ORDINAL_LIST;
        default -> throw new IndexException("A COLUMNAR index BY ITEM needs a LIST or an array property, not " + propertyType);
      };
    }
    return switch (propertyType) {
      case STRING, BOOLEAN, LINK -> ORDINAL;
      case BYTE, SHORT, INTEGER, LONG, DATE, DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> LONG;
      case FLOAT, DOUBLE -> DOUBLE;
      default -> throw new IndexException("A COLUMNAR index does not support properties of type " + propertyType
          + (propertyType == Type.LIST ? " (index it BY ITEM)" : ""));
    };
  }
}
