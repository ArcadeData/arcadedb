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
package com.arcadedb.function.math;

import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * abs() function - returns the absolute value of a number preserving the input type,
 * as per the Cypher contract: abs(INTEGER) returns INTEGER, abs(FLOAT) returns FLOAT.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AbsFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "abs";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    // Rejects anything outside INTEGER | FLOAT as a client-facing type error rather than a 500 (issue #5484).
    final Number value = CypherFunctionHelper.requireNumberArgument(args[0], "abs");
    if (value == null)
      return null;
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      try {
        // absExact() fails on Long.MIN_VALUE, whose magnitude is not representable in a signed 64-bit
        // integer and which Math.abs() would silently return unchanged - a negative "absolute value".
        return Math.absExact(value.longValue());
      } catch (final ArithmeticException e) {
        // Same classification as the +, - and * operators (issue #5602): no representable answer is the caller's
        // pair of values, not a server fault, so it reports as a client error rather than a 500.
        throw new ArithmeticErrorException("long overflow", e);
      }
    }
    return Math.abs(value.doubleValue());
  }
}
