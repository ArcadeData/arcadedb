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
package com.arcadedb.query.sql.function.conversion;

import com.arcadedb.database.Identifiable;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.misc.AbstractSQLMethod;
import com.arcadedb.schema.Type;

import java.util.Locale;
import java.util.logging.Level;

/**
 * Converts a value to another type in Java or ArcadeDB's supported types.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
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
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final CommandContext iContext, final Object ioResult, final Object[] iParams) {
    if (iThis == null || iParams[0] == null) {
      return null;
    }

    final String destType = iParams[0].toString();

    if (destType.contains(".")) {
      try {
        return Type.convert(iContext.getDatabase(), iThis, Class.forName(destType));
      } catch (ClassNotFoundException e) {
        LogManager.instance().log(this, Level.SEVERE, "Type for destination type was not found", e);
      }
    } else {
      final Type orientType = Type.valueOf(destType.toUpperCase(Locale.ENGLISH));
      if (orientType != null) {
        return Type.convert(iContext.getDatabase(), iThis, orientType.getDefaultJavaType());
      }
    }

    return null;
  }
}
