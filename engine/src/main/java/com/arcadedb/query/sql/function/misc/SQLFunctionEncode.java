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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.util.*;

/**
 * Encode a string in various format (only base64 for now).
 *
 * @author Johann Sorel (Geomatys)
 */
public class SQLFunctionEncode extends SQLFunctionAbstract {

  public static final String NAME             = "encode";
  public static final String FORMAT_BASE64    = "base64";
  public static final String FORMAT_BASE64URL = "base64url";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionEncode() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {

    final Object candidate = params[0];
    final String format = params[1].toString();

    byte[] data = null;
    if (candidate instanceof byte[] bytes) {
      data = bytes;
    } else if (candidate instanceof String string) {
      data = string.getBytes();
    } else if (candidate instanceof RID iD) {
      final Record rec = iD.getRecord();
      if (rec instanceof Binary binary) {
        data = binary.toByteArray();
      }
    }

    if (data == null)
      return null;

    if (FORMAT_BASE64.equalsIgnoreCase(format)) {
      return Base64.getEncoder().encodeToString(data);
    } else if (FORMAT_BASE64URL.equalsIgnoreCase(format)) {
      return Base64.getEncoder().withoutPadding().encodeToString(data).replace('+', '-').replace('/', '_');
    } else {
      throw new CommandSQLParsingException("Unknown format :" + format);
    }
  }

  @Override
  public String getSyntax() {
    return "encode(<value|rid>, <format>)";
  }
}
