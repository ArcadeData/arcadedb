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
package com.arcadedb.query.sql.function.math;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.security.SecureRandom;

/**
 * Generates a random number integer between 0 and the number passed as parameter.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionRandomInt extends SQLFunctionAbstract {
  public static final String NAME = "randomInt";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionRandomInt() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 1)
      throw new CommandSQLParsingException("Expected maximum value in function");

    int bound = params[0] instanceof Number n ? n.intValue() : Integer.parseInt(params[0].toString());

    return new SecureRandom().nextInt(bound);
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "randomInt(<maximum>)";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
