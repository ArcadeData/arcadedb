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

    if (destType.contains(".")) {
      try {
        return Type.convert(context.getDatabase(), value, Class.forName(destType));
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
      return Type.convert(context.getDatabase(), value, arcadeType.getDefaultJavaType());
    }

    return null;
  }
}
