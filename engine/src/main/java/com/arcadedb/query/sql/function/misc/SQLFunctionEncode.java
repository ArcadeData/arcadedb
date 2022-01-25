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

import java.util.Base64;

/**
 * Encode a string in various format (only base64 for now).
 *
 * @author Johann Sorel (Geomatys)
 */
public class SQLFunctionEncode extends SQLFunctionAbstract {

  public static final String NAME          = "encode";
  public static final String FORMAT_BASE64 = "base64";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionEncode() {
    super(NAME);
  }

  public Object execute( Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, CommandContext iContext) {

    final Object candidate = iParams[0];
    final String format = iParams[1].toString();

    byte[] data = null;
    if (candidate instanceof byte[]) {
      data = (byte[]) candidate;
    } else if (candidate instanceof RID) {
      final Record rec = ((RID) candidate).getRecord();
      if (rec instanceof Binary) {
        data = ((Binary) rec).toByteArray();
      }
    }

    if (data == null) {
      return null;
    }

    if (FORMAT_BASE64.equalsIgnoreCase(format)) {
      return Base64.getEncoder().encodeToString(data);
    } else {
      throw new CommandSQLParsingException("Unknown format :" + format);
    }
  }

  @Override
  public String getSyntax() {
    return "encode(<binaryfield>, <format>)";
  }
}
