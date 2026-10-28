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
package com.arcadedb.function.sql.math;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.SQLFunctionAbstract;

import java.security.SecureRandom;

/**
 * Generates a random number integer between 0 and the number passed as parameter.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionRandomInt extends SQLFunctionAbstract {
  public static final  String       NAME   = "randomInt";
  private static final SecureRandom RANDOM = new SecureRandom();

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

    if (params[0] == null)
      return null;

    // intValue() READ 2.5 AS 2, AND 0.9 AND 3000000000 AS NON-POSITIVE INTS REPORTED AS "bound must be positive"; A TEXT
    // THAT DID NOT PARSE ESCAPED AS A RAW NumberFormatException (issue #10699)
    final int bound = requireWholeIntArgument(params[0], "maximum");
    if (bound <= 0)
      throw new IllegalArgumentException(NAME + "() <maximum> must be positive, but received " + bound);

    return RANDOM.nextInt(bound);
  }

  public String getSyntax() {
    return "randomInt(<maximum>)";
  }
}
