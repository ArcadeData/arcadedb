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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

/**
 * Compares two strings
 */
public class SQLFunctionStrcmpci extends SQLFunctionAbstract {

  public static final String NAME = "strcmpci";

  public SQLFunctionStrcmpci() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      final CommandContext iContext) {
    String s1 = null;
    if (iParams[0] != null && iParams[0] instanceof String)
      s1 = (String) iParams[0];

    String s2 = null;
    if (iParams[1] != null && iParams[1] instanceof String)
      s2 = (String) iParams[1];

    if (s1 == null && s2 == null)
      return 0;
    else if (s1 == null)
      return -1;
    else if (s2 == null)
      return 1;

    int res = s1.compareToIgnoreCase(s2);
    if (res != 0) {
      // normalize res to -1, 0, 1
      res = res / Math.abs(res);
    }
    return res;
  }

  @Override
  public String getSyntax() {
    return "strcmpci(<arg1>, <arg2>)";
  }
}
