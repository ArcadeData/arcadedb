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

  public Object execute(final Object iThis, final Identifiable iRecord, final Object iCurrentResult, final Object[] iParams, final CommandContext iContext) {
    final Object inputValue = iParams[0];

    if (inputValue == null) {
      result = null;
    } else if (inputValue instanceof Number && ((Number) inputValue).doubleValue() < 0.0) {
      result = null;
    } else if (inputValue instanceof BigDecimal) {
      result = ((BigDecimal) inputValue).sqrt(new MathContext(10));
    } else if (inputValue instanceof BigInteger) {
      result = ((BigInteger) inputValue).sqrt();
    } else if (inputValue instanceof Integer) {
      result = Double.valueOf(Math.sqrt((Integer) inputValue)).intValue();
    } else if (inputValue instanceof Long) {
      result = Double.valueOf(Math.sqrt((Long) inputValue)).longValue();
    } else if (inputValue instanceof Short) {
      result = Double.valueOf(Math.sqrt((Short) inputValue)).shortValue();
    } else if (inputValue instanceof Double) {
      result = Double.valueOf(Math.sqrt((Double) inputValue)).doubleValue();
    } else if (inputValue instanceof Float) {
      result = Double.valueOf(Math.sqrt((Float) inputValue)).floatValue();
    } else if (inputValue instanceof Duration) {
      final int seconds = ((Duration) inputValue).toSecondsPart();
      final long nanos = ((Duration) inputValue).toNanosPart();
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
