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

import com.arcadedb.database.RID;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.schema.Type;

/**
 * The index key a node or relationship seek looks up for a text that spells a RID ({@code '#0:1'}). openCypher's {@code =} reads such a
 * text against a number as the id {@code id()} encodes it to ({@code 1 = '#0:1'}, the id()/RID-text interop of issue #4183), so in a
 * column holding numbers the text stands for that one number, and the seek has to look the number up: looked up as a text, it found no
 * entry where the scan of the same statement found the vertex (issue #11933). In a column holding texts it stays a text, which is what
 * {@code =} compares it with there.
 */
public final class RidTextKeys {
  private RidTextKeys() {
  }

  /**
   * @param keyType the type of the index column the value is sought in; null when unknown, which keeps the value
   *
   * @return the id the value encodes when it is the text of a RID and the column holds numbers; the value otherwise
   */
  public static Object keyFor(final Object value, final Type keyType) {
    if (value instanceof String text && keyType != null && isNumeric(keyType) && RID.is(text))
      return IdFunction.encodeRidAsLong(new RID(text));
    return value;
  }

  /** {@link #keyFor} for column {@code column} of an index with {@code keyTypes}, which may be shorter or null. */
  public static Object keyFor(final Object value, final Type[] keyTypes, final int column) {
    return keyFor(value, keyTypes != null && column < keyTypes.length ? keyTypes[column] : null);
  }

  private static boolean isNumeric(final Type type) {
    return switch (type) {
      case BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL -> true;
      default -> false;
    };
  }
}
