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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public class SQLFunctionSquareRoot extends SQLFunctionMathAbstract {
  public static final String NAME = "sqrt";
  private             Object result;

  public SQLFunctionSquareRoot() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  public Object execute(final Object self, final Identifiable record, final Object currentResult, final Object[] params,
      final CommandContext context) {
    final Object inputValue = params[0];

    // The root of a number is a function of its value, not of the Java width it arrived in (issue #11047): sqrt(2) of an
    // INTEGER is 1.414..., not 1. A DECIMAL keeps its exact arithmetic, rounded to DECIMAL128 like a decimal division
    if (inputValue == null)
      result = null;
    else if (inputValue instanceof Number number && number.doubleValue() < 0.0)
      result = null;
    else if (inputValue instanceof BigDecimal decimal)
      result = decimal.sqrt(MathContext.DECIMAL128);
    else if (inputValue instanceof BigInteger integer)
      result = new BigDecimal(integer).sqrt(MathContext.DECIMAL128);
    else if (inputValue instanceof Number number)
      result = Math.sqrt(number.doubleValue());
    else
      throw new IllegalArgumentException("Argument to square root must be a number");

    return getResult();
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "sqrt(<number>)";
  }

  @Override
  public Object getResult() {
    return result;
  }

  /**
   * Pure math over its single argument (issue #6190).
   */
  @Override
  public boolean isDeterministic() {
    return true;
  }
}
