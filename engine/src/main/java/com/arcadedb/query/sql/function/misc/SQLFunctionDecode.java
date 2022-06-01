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
package com.arcadedb.query.sql.function.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.util.Base64;

/**
 * Encode a string in various format (only base64 for now)
 *
 * @author Johann Sorel (Geomatys)
 */
public class SQLFunctionDecode extends SQLFunctionAbstract {

  public static final String NAME = "decode";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionDecode() {
    super(NAME);
  }

  @Override
  public Object execute( final Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    final String candidate = iParams[0].toString();
    final String format = iParams[1].toString();

    if (SQLFunctionEncode.FORMAT_BASE64.equalsIgnoreCase(format)) {
      return Base64.getDecoder().decode(candidate);
    } else {
      throw new CommandSQLParsingException("Unknown format :" + format);
    }
  }

  @Override
  public String getSyntax() {
    return "decode(<binaryfield>, <format>)";
  }
}
