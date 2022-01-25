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
package com.arcadedb.query.sql.function.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

/**
 * Formats content.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionFormat extends SQLFunctionAbstract {
  public static final String NAME = "format";

  public SQLFunctionFormat() {
    super(NAME);
  }

  public Object execute( final Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      final Object[] params, CommandContext iContext) {
    final Object[] args = new Object[params.length - 1];

      System.arraycopy(params, 1, args, 0, args.length);

    return String.format((String) params[0], args);
  }

  public String getSyntax() {
    return "format(<format>, <arg1> [,<argN>]*)";
  }
}
