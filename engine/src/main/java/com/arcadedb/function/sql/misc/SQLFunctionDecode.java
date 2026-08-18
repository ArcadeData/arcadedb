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
package com.arcadedb.function.sql.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.SQLFunctionAbstract;

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
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute( final Object self, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext context) {

    // A null value decodes to null, mirroring encode() which returns null for anything it cannot turn into bytes.
    if (params[0] == null || params[1] == null)
      return null;

    final String candidate = params[0].toString();
    final String format = params[1].toString();

    try {
      if (SQLFunctionEncode.FORMAT_BASE64.equalsIgnoreCase(format)) {
        return Base64.getDecoder().decode(candidate);
      } else if (SQLFunctionEncode.FORMAT_BASE64URL.equalsIgnoreCase(format)) {
        final int padding = candidate.length() % 4 == 2 ? 2 : (candidate.length() % 4 == 3 ? 1 : 0);
        return Base64.getDecoder().decode(candidate.replace('-', '+').replace('_', '/') + "=".repeat(padding));
      } else {
        throw new CommandSQLParsingException("Unknown format :" + format);
      }
    } catch (final IllegalArgumentException e) {
      // The JDK decoder rejects malformed input with a bare IllegalArgumentException whose message names an offset
      // and nothing else; say which function and which format refused it (issue #6389).
      throw new IllegalArgumentException(NAME + "() cannot decode the value as " + format + ": " + e.getMessage(), e);
    }
  }

  @Override
  public String getSyntax() {
    return "decode(<value>, <format>)";
  }

  /**
   * A pure base64/base64url decode of its string argument - unlike {@link SQLFunctionEncode}, it never dereferences
   * a RID (issue #6190).
   */
  @Override
  public boolean isDeterministic() {
    return true;
  }
}
