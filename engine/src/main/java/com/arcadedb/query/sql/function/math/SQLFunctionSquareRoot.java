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
import com.arcadedb.query.sql.executor.CommandContext;

import java.math.*;
import java.time.*;

public class SQLFunctionSquareRoot extends SQLFunctionMathAbstract {
  public static final String NAME = "sqrt";
  private             Object result;

  public SQLFunctionSquareRoot() {
    super(NAME);
  }

  public Object execute(final Object iThis, final Identifiable record, final Object currentResult, final Object[] params, final CommandContext context) {
    final Object inputValue = params[0];

    if (inputValue == null) {
      result = null;
    } else if (inputValue instanceof Number number && number.doubleValue() < 0.0) {
      result = null;
    } else if (inputValue instanceof BigDecimal decimal) {
      result = decimal.sqrt(new MathContext(10));
    } else if (inputValue instanceof BigInteger integer) {
      result = integer.sqrt();
    } else if (inputValue instanceof Integer integer) {
      result = Double.valueOf(Math.sqrt(integer)).intValue();
    } else if (inputValue instanceof Long long1) {
      result = Double.valueOf(Math.sqrt(long1)).longValue();
    } else if (inputValue instanceof Short short1) {
      result = Double.valueOf(Math.sqrt(short1)).shortValue();
    } else if (inputValue instanceof Double double1) {
      result = Double.valueOf(Math.sqrt(double1)).doubleValue();
    } else if (inputValue instanceof Float float1) {
      result = Double.valueOf(Math.sqrt(float1)).floatValue();
    } else if (inputValue instanceof Duration duration) {
      final int seconds = duration.toSecondsPart();
      final long nanos = duration.toNanosPart();
      if (seconds < 0 && nanos < 0)
        result = null;
      else {
        result = Duration.ofSeconds((int) Math.sqrt(seconds), (long) Math.sqrt(nanos));
      }
    } else {
      throw new IllegalArgumentException("Argument to square root must be a number.");
    }

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
}
