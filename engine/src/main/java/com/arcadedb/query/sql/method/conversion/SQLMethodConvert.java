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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Locale;
import java.util.logging.Level;

/**
 * Converts a value to another type in Java or ArcadeDB's supported types.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodConvert extends AbstractSQLMethod {

  public static final String NAME = "convert";

  // THE FIRST DECIMALS OUT OF THE LONG RANGE: THE FRACTION IS DISCARDED, SO 9223372036854775807.5 IS Long.MAX_VALUE AND 2^63 IS THE
  // FIRST VALUE BEYOND IT (THE RULE OF THE RANGE CHECK OF A LONG PROPERTY)
  private static final BigDecimal BEYOND_LONG_MAX = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
  private static final BigDecimal BEYOND_LONG_MIN = BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);

  public SQLMethodConvert() {
    super(NAME, 1, 1);
  }

  @Override
  public String getSyntax() {
    return "convert(<type>)";
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    if (value == null || params[0] == null) {
      return null;
    }

    final String destType = params[0].toString();

    // convertOrNull(), not convert(): this is a conversion asked for inside a query, so a value that cannot be
    // expressed in the target type answers null for that row - as this method has always done and its syntax
    // implies - instead of aborting the whole query at the first row that does not fit. Only a WRITE refuses; see
    // the strict/lenient split in Type.convertOrNull's Javadoc (issue #8090). An unknown TYPE NAME is a different
    // thing and still fails loudly below: that is a query to fix, not a row that does not match.
    if (destType.contains(".")) {
      try {
        return convertOrNull(context, value, Class.forName(destType));
      } catch (final ClassNotFoundException e) {
        LogManager.instance().log(this, Level.SEVERE, "Type for destination type was not found", e);
      }
    } else {
      // Type is an enum: valueOf() throws IllegalArgumentException for an unknown name and NEVER returns null, so the
      // guard that used to stand here was dead code and an unknown type name escaped as a raw JDK exception. Answer
      // with a typed parsing error that names the valid types instead (issue #6389).
      final Type arcadeType;
      try {
        arcadeType = Type.valueOf(destType.toUpperCase(Locale.ENGLISH));
      } catch (final IllegalArgumentException e) {
        throw new CommandSQLParsingException(
            "Unknown type '" + destType + "' in convert(): expected one of " + Arrays.toString(Type.values())
                + " or a fully qualified Java class name", e);
      }
      return convertOrNull(context, value, arcadeType.getDefaultJavaType());
    }

    return null;
  }

  /**
   * {@link Type#convertOrNull} with the range of a LONG target held: the plain conversion has no range check for LONG, which it
   * keeps for the comparison callers that rely on it, so a BigInteger or BigDecimal beyond 64 bits would wrap to its low 64 bits
   * and a Double or Float would saturate, where INTEGER, SHORT and BYTE answer null (issue #13029).
   */
  private static Object convertOrNull(final CommandContext context, final Object value, final Class<?> targetClass) {
    if ((targetClass == Long.class || targetClass == Long.TYPE) && isBeyondLong(value))
      return null;
    return Type.convertOrNull(context.getDatabase(), value, targetClass);
  }

  private static boolean isBeyondLong(final Object value) {
    return switch (value) {
      case BigDecimal decimal -> decimal.compareTo(BEYOND_LONG_MAX) >= 0 || decimal.compareTo(BEYOND_LONG_MIN) <= 0;
      case BigInteger integer -> integer.bitLength() > 63;
      // 2^63 IS THE FIRST DOUBLE ABOVE THE RANGE, AND -2^63 IS EXACTLY Long.MIN_VALUE
      case Double number -> number >= 0x1p63 || number < -0x1p63;
      case Float number -> number >= 0x1p63f || number < -0x1p63f;
      default -> false;
    };
  }
}
