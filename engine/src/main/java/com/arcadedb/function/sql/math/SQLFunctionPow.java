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
import com.arcadedb.query.sql.executor.CommandContext;

import java.math.*;

/**
 * Math pow() function.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionPow extends SQLFunctionMathAbstract {
  public static final String NAME = "pow";
  private             Object result;

  public SQLFunctionPow() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable record, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length != 2)
      throw new IllegalArgumentException("Syntax error: " + getSyntax());
    if (params[1] == null)
      throw new IllegalArgumentException("Syntax error: " + getSyntax());

    final Object inputValue = params[0];
    final int powerValue = ((Number) params[1]).intValue();

    switch (inputValue) {
    case null -> result = null;
    case Number number when number.doubleValue() < 0.0 -> result = null;
    case BigDecimal decimal -> result = decimal.pow(powerValue);
    case BigInteger integer -> result = integer.pow(powerValue);
    case Integer integer -> result = Double.valueOf(Math.pow(integer, powerValue)).intValue();
    case Long long1 -> result = Double.valueOf(Math.pow(long1, powerValue)).longValue();
    case Short short1 -> result = Double.valueOf(Math.pow(short1, powerValue)).shortValue();
    case Double double1 -> result = Math.pow(double1, powerValue);
    case Float float1 -> result = Double.valueOf(Math.pow(float1, powerValue)).floatValue();
    default -> throw new IllegalArgumentException("Argument to power must be a number");
    }

    return getResult();
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "pow(<number>, <power>)";
  }

  @Override
  public Object getResult() {
    return result;
  }
}
